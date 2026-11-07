// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Fixed-capacity receipt ownership, independent of the request actor's progress.
use hyper_filesystem::protocol::{self, Error, WritebackCompletion};

/// A backend-owned data writeback receipt. Physical completion and page retirement
/// must progress independently of this wait and of the mutable `Volume` actor.
/// Dropping an uncollected receipt must safely retire its resources or transfer
/// them to a mount-owned completion owner joined before backend teardown.
pub trait WriteCompletion: Send {
    fn wait(self: Box<Self>) -> Result<(), Error>;
}

/// The backend reserves this receipt and its completion resources before any
/// mutation. Input slices must be fully consumed before returning. On partial
/// acceptance, completion covers exactly this prefix, including any late failure.
pub struct SubmittedWrite {
    pub accepted: usize,
    pub completion: Box<dyn WriteCompletion>,
}

struct PendingWrite {
    batch: u64,
    submitted: SubmittedWrite,
}

pub(super) struct Pending {
    entries: [Option<PendingWrite>; protocol::MAX_WRITEBACK_BATCHES],
    head: usize,
    length: usize,
    bytes: usize,
    last_batch: u64,
}

impl Pending {
    pub(super) fn new() -> Self {
        Self {
            entries: core::array::from_fn(|_| None),
            head: 0,
            length: 0,
            bytes: 0,
            last_batch: 0,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub(super) fn submit(
        &mut self,
        batch: u64,
        requested: usize,
        submit: impl FnOnce() -> Result<SubmittedWrite, Error>,
    ) -> Result<u64, Error> {
        if batch == 0 || batch <= self.last_batch {
            return Err(Error::Corrupt);
        }
        if requested == 0 || requested > protocol::DATA_BYTES {
            return Err(Error::InvalidInput);
        }
        // Capacity is reserved before calling code which may mutate the medium.
        // Entries are inline; storing an accepted receipt cannot allocate or fail.
        if self.length == self.entries.len()
            || requested > protocol::MAX_WRITEBACK_BATCHES * protocol::DATA_BYTES - self.bytes
        {
            return Err(Error::ResourceLimit);
        }
        let submitted = submit()?;
        if submitted.accepted == 0 || submitted.accepted > requested {
            return Err(Error::Corrupt);
        }
        let accepted = submitted.accepted;
        let slot = (self.head + self.length) % self.entries.len();
        self.entries[slot] = Some(PendingWrite { batch, submitted });
        self.length += 1;
        self.bytes += accepted;
        self.last_batch = batch;
        Ok(accepted as u64)
    }

    pub(super) fn reap(&mut self, batch: u64) -> Result<WritebackCompletion, Error> {
        let next = self.entries[self.head].as_ref().ok_or(Error::Corrupt)?;
        if batch == 0 || next.batch != batch {
            return Err(Error::Corrupt);
        }
        let next = self.entries[self.head].take().ok_or(Error::Corrupt)?;
        self.head = (self.head + 1) % self.entries.len();
        self.length -= 1;
        self.bytes -= next.submitted.accepted;
        Ok(WritebackCompletion {
            batch,
            accepted: next.submitted.accepted as u64,
            result: next.submitted.completion.wait(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Completion(Arc<AtomicUsize>, Result<(), Error>);
    impl WriteCompletion for Completion {
        fn wait(self: Box<Self>) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::Relaxed);
            self.1
        }
    }
    fn submitted(
        accepted: usize,
        count: &Arc<AtomicUsize>,
        result: Result<(), Error>,
    ) -> SubmittedWrite {
        SubmittedWrite {
            accepted,
            completion: Box::new(Completion(count.clone(), result)),
        }
    }

    #[test]
    fn reserves_bounded_capacity_before_mutating_and_reuses_only_reaped_slots() -> Result<(), Error>
    {
        let mut pending = Pending::new();
        let count = Arc::new(AtomicUsize::new(0));
        for batch in 1..=2 {
            assert_eq!(
                pending.submit(batch, protocol::DATA_BYTES, || Ok(submitted(
                    protocol::DATA_BYTES,
                    &count,
                    Ok(())
                )))?,
                protocol::DATA_BYTES as u64
            );
        }
        let mut called = false;
        assert_eq!(
            pending.submit(3, 1, || {
                called = true;
                Ok(submitted(1, &count, Ok(())))
            }),
            Err(Error::ResourceLimit)
        );
        assert!(!called);
        assert_eq!(count.load(Ordering::Relaxed), 0);
        assert_eq!(pending.reap(1)?.result, Ok(()));
        assert_eq!(
            pending.submit(3, 4, || Ok(submitted(4, &count, Ok(()))))?,
            4
        );
        assert_eq!(pending.reap(2)?.batch, 2);
        assert_eq!(pending.reap(3)?.accepted, 4);
        assert_eq!(count.load(Ordering::Relaxed), 3);
        assert!(pending.is_empty());
        Ok(())
    }

    #[test]
    fn exact_oldest_identity_rejects_unknown_duplicate_and_stale_receipts() -> Result<(), Error> {
        let mut pending = Pending::new();
        let count = Arc::new(AtomicUsize::new(0));
        pending.submit(7, 4, || Ok(submitted(4, &count, Ok(()))))?;
        pending.submit(9, 4, || Ok(submitted(4, &count, Ok(()))))?;
        for batch in [0, 1, 8, 9, u64::MAX] {
            assert_eq!(pending.reap(batch), Err(Error::Corrupt));
        }
        assert_eq!(count.load(Ordering::Relaxed), 0);
        pending.reap(7)?;
        assert_eq!(pending.reap(7), Err(Error::Corrupt));
        pending.reap(9)?;
        assert_eq!(
            pending.submit(7, 4, || Ok(submitted(4, &count, Ok(())))),
            Err(Error::Corrupt)
        );
        Ok(())
    }

    #[test]
    fn partial_acceptance_preserves_known_prefix_and_late_failure() -> Result<(), Error> {
        let mut pending = Pending::new();
        let count = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            pending.submit(1, 100, || Ok(submitted(23, &count, Err(Error::Io))))?,
            23
        );
        assert_eq!(
            pending.reap(1)?,
            WritebackCompletion {
                batch: 1,
                accepted: 23,
                result: Err(Error::Io)
            }
        );
        assert_eq!(pending.bytes, 0);
        Ok(())
    }

    #[test]
    fn rejected_submission_retains_capacity_and_invalid_progress_is_terminal() -> Result<(), Error>
    {
        let mut pending = Pending::new();
        let count = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            pending.submit(1, 4, || Err(Error::NoSpace)),
            Err(Error::NoSpace)
        );
        assert_eq!(
            pending.submit(2, 4, || Ok(submitted(5, &count, Ok(())))),
            Err(Error::Corrupt)
        );
        assert_eq!(
            pending.submit(3, 4, || Ok(submitted(0, &count, Ok(())))),
            Err(Error::Corrupt)
        );
        assert!(pending.is_empty());
        pending.submit(4, 4, || Ok(submitted(4, &count, Ok(()))))?;
        pending.reap(4)?;
        Ok(())
    }
}
