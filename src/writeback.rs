// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Synchronization policy owned by the ordered filesystem request loop.
use hyper_filesystem::protocol::{Error, Operation};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WritebackPolicy {
    /// Each mutation completes filesystem synchronization before replying.
    Synchronous,
    /// Recoverable media with periodic synchronization. The backend owns
    /// cadence until VFS explicitly takes dirty-data and timer ownership.
    Periodic(Duration),
    /// No persistent medium exists; no durability is advertised.
    Volatile,
    ReadOnly,
}

pub(crate) fn mutation(operation: Operation) -> bool {
    matches!(
        operation,
        Operation::Write
            | Operation::Create
            | Operation::Remove
            | Operation::Rename
            | Operation::Resize
            | Operation::SetMetadata
    )
}

pub(crate) struct State {
    policy: WritebackPolicy,
    interval: Option<u64>,
    deadline: u64,
    failure: Option<Error>,
}

impl State {
    pub(crate) fn new(policy: WritebackPolicy, now: u64) -> Result<Self, Error> {
        let interval = match policy {
            WritebackPolicy::Periodic(duration) => {
                let value = u64::try_from(duration.as_nanos()).map_err(|_| Error::InvalidInput)?;
                if value == 0 || value == u64::MAX {
                    return Err(Error::InvalidInput);
                }
                Some(value)
            }
            _ => None,
        };
        Ok(Self {
            policy,
            interval,
            failure: None,
            deadline: interval.map_or(u64::MAX, |n| now.saturating_add(n).min(u64::MAX - 1)),
        })
    }

    pub(crate) fn take_writeback(&mut self) -> Result<u64, Error> {
        self.admit(Operation::TakeWriteback)?;
        let interval = match self.policy {
            WritebackPolicy::Periodic(duration) => {
                u64::try_from(duration.as_nanos()).map_err(|_| Error::InvalidInput)?
            }
            _ => 0,
        };
        if interval != 0 && !(1_000_000_000..=3_600_000_000_000).contains(&interval) {
            return Err(Error::InvalidInput);
        }
        self.interval = None;
        self.deadline = u64::MAX;
        Ok(interval)
    }

    pub(crate) fn deadline(&self) -> u64 {
        self.deadline
    }

    pub(crate) fn admit(&self, operation: Operation) -> Result<(), Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if self.policy == WritebackPolicy::ReadOnly && mutation(operation) {
            return Err(Error::ReadOnly);
        }
        Ok(())
    }

    pub(crate) fn complete(
        &mut self,
        operation: Operation,
        result: Result<u64, Error>,
        sync: impl FnOnce() -> Result<(), Error>,
    ) -> Result<u64, Error> {
        if self.failure.is_some() {
            return result;
        }
        if mutation(operation) && self.policy == WritebackPolicy::Synchronous {
            // Even ENOSPC can leave metadata changes. Preserve accepted write
            // progress when the final fence fails; every later RPC sees the error.
            if let Err(error) = sync() {
                self.failure = Some(error);
                return match result {
                    Ok(bytes) if operation == Operation::Write && bytes != 0 => Ok(bytes),
                    Err(original) => Err(original),
                    _ => Err(error),
                };
            }
        }
        if let Err(error) = result
            && (operation == Operation::Sync
                || (operation == Operation::SyncFile && error.terminal()))
        {
            self.failure = Some(error);
        }
        result
    }

    /// Called after a potentially slow sync so an expired timeout cannot
    /// prevent the owner from servicing a ready foreground request.
    pub(crate) fn coalesce(&mut self, now: u64) {
        if self.failure.is_none()
            && let Some(interval) = self.interval
            && now >= self.deadline
        {
            let ticks = (now - self.deadline) / interval + 1;
            self.deadline = self
                .deadline
                .saturating_add(ticks.saturating_mul(interval))
                .min(u64::MAX - 1);
        }
    }

    pub(crate) fn periodic(
        &mut self,
        now: u64,
        sync: impl FnOnce() -> Result<(), Error>,
    ) -> Result<(), Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if now < self.deadline {
            return Ok(());
        }
        let Some(interval) = self.interval else {
            return Ok(());
        };
        if let Err(error) = sync() {
            self.failure = Some(error);
            return Err(error);
        }
        // Advance the fixed cadence past missed ticks without building a queue
        // of catch-up checkpoints. Ordinary requests never move this deadline.
        let ticks = (now - self.deadline) / interval + 1;
        self.deadline = self
            .deadline
            .saturating_add(ticks.saturating_mul(interval))
            .min(u64::MAX - 1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn negotiated_vfs_owner_disables_only_the_periodic_timer() -> Result<(), Error> {
        let mut state = State::new(WritebackPolicy::Periodic(Duration::from_secs(30)), 0)?;
        assert_eq!(state.take_writeback()?, 30_000_000_000);
        assert_eq!(state.deadline(), u64::MAX);
        state.periodic(60_000_000_000, || Err(Error::Io))?;
        state.complete(Operation::Write, Ok(4), || Err(Error::Io))?;
        assert_eq!(
            state.complete(Operation::Sync, Err(Error::Io), || Ok(())),
            Err(Error::Io)
        );
        assert_eq!(state.admit(Operation::Write), Err(Error::Io));
        let mut synchronous = State::new(WritebackPolicy::Synchronous, 0)?;
        assert_eq!(synchronous.take_writeback()?, 0);
        assert_eq!(
            synchronous.complete(Operation::Create, Ok(0), || Err(Error::Io)),
            Err(Error::Io)
        );
        Ok(())
    }
    #[test]
    fn idle_and_busy_traffic_use_same_fixed_deadline() -> Result<(), Error> {
        let mut state = State::new(WritebackPolicy::Periodic(Duration::from_nanos(30)), 0)?;
        let mut passes = 0;
        for now in 0..95 {
            state.periodic(now, || {
                passes += 1;
                Ok(())
            })?;
            state.admit(Operation::Write)?;
            state.complete(Operation::Write, Ok(4), || Err(Error::Io))?;
        }
        assert_eq!(passes, 3);
        assert_eq!(state.deadline(), 120);
        state.periodic(300, || {
            passes += 1;
            Ok(())
        })?;
        assert_eq!(passes, 4);
        assert_eq!(state.deadline(), 330);
        state.coalesce(400);
        assert_eq!(state.deadline(), 420);
        Ok(())
    }
    #[test]
    fn failure_does_not_advance_deadline_or_admit_more_mutations() -> Result<(), Error> {
        let mut state = State::new(WritebackPolicy::Periodic(Duration::from_nanos(30)), 0)?;
        assert_eq!(state.periodic(30, || Err(Error::Io)), Err(Error::Io));
        assert_eq!(state.deadline(), 30);
        assert_eq!(state.admit(Operation::Sync), Err(Error::Io));
        assert_eq!(state.periodic(60, || Ok(())), Err(Error::Io));
        Ok(())
    }
    #[test]
    fn synchronous_partial_progress_and_error_are_both_preserved() -> Result<(), Error> {
        let mut state = State::new(WritebackPolicy::Synchronous, 0)?;
        assert_eq!(
            state.complete(Operation::Write, Ok(23), || Err(Error::Io)),
            Ok(23)
        );
        assert_eq!(state.admit(Operation::Write), Err(Error::Io));
        assert_eq!(state.admit(Operation::Sync), Err(Error::Io));
        for op in [
            Operation::Create,
            Operation::Remove,
            Operation::Rename,
            Operation::Resize,
            Operation::SetMetadata,
            Operation::Write,
        ] {
            let mut state = State::new(WritebackPolicy::Synchronous, 0)?;
            let mut flushed = false;
            assert_eq!(
                state.complete(op, Err(Error::NoSpace), || {
                    flushed = true;
                    Ok(())
                }),
                Err(Error::NoSpace)
            );
            assert!(flushed);
            assert_eq!(state.complete(op, Ok(0), || Err(Error::Io)), Err(Error::Io));
        }
        Ok(())
    }
    #[test]
    fn readonly_volatile_and_invalid_periods() -> Result<(), Error> {
        let state = State::new(WritebackPolicy::ReadOnly, 0)?;
        assert_eq!(state.admit(Operation::Write), Err(Error::ReadOnly));
        assert_eq!(state.admit(Operation::Read), Ok(()));
        let mut state = State::new(WritebackPolicy::Volatile, 0)?;
        assert_eq!(
            state.complete(Operation::Write, Ok(1), || Err(Error::Io)),
            Ok(1)
        );
        assert_eq!(state.deadline(), u64::MAX);
        assert!(State::new(WritebackPolicy::Periodic(Duration::ZERO), 0).is_err());
        Ok(())
    }
}
