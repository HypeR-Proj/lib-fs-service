// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Default)]
struct Backend {
    mutations: usize,
    fences: usize,
    fail_fence: bool,
    periodic: bool,
    vfs_owner: bool,
    read_extents: bool,
    policy: Option<WritebackPolicy>,
    policy_queries: std::cell::Cell<usize>,
    enables: usize,
    async_writeback: bool,
    accepted: Option<usize>,
    completion_error: Option<Error>,
    completed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl Volume for Backend {
    fn capabilities(&self) -> protocol::Capabilities {
        protocol::Capabilities::from_bits(
            if self.read_extents {
                protocol::Capabilities::READ_EXTENTS.bits()
            } else {
                0
            } | if self.async_writeback {
                protocol::Capabilities::ASYNC_WRITEBACK.bits()
            } else {
                0
            },
        )
    }
    fn writeback_policy(&self) -> WritebackPolicy {
        self.policy_queries.set(self.policy_queries.get() + 1);
        if let Some(policy) = self.policy {
            return policy;
        }
        if self.periodic {
            WritebackPolicy::Periodic(std::time::Duration::from_secs(30))
        } else {
            WritebackPolicy::Synchronous
        }
    }
    fn enable_vfs_writeback(&mut self) -> Result<(), Error> {
        self.enables += 1;
        if self.fail_fence {
            return Err(Error::Io);
        }
        self.vfs_owner = true;
        Ok(())
    }

    fn stat(&mut self, _: &str) -> Result<Entry, Error> {
        Ok(Entry::empty())
    }
    fn read_directory(
        &mut self,
        _: &str,
        _: protocol::DirectoryCursor,
        _: &mut [u8],
    ) -> Result<usize, Error> {
        Ok(0)
    }
    fn read(&mut self, _: &str, _: u64, _: &mut [u8]) -> Result<usize, Error> {
        Ok(0)
    }
    fn write(&mut self, _: &str, _: u64, input: &[u8]) -> Result<usize, Error> {
        self.mutations += 1;
        Ok(input.len())
    }
    fn submit_writeback(
        &mut self,
        _: &str,
        _: u64,
        input: &[u8],
        _: u64,
    ) -> Result<SubmittedWrite, Error> {
        let completion = Box::new(TestCompletion(
            self.completed.clone(),
            self.completion_error,
        ));
        self.mutations += 1;
        Ok(SubmittedWrite {
            accepted: self.accepted.unwrap_or(input.len()),
            completion,
        })
    }
    fn create(&mut self, _: &str, _: bool) -> Result<(), Error> {
        self.mutations += 1;
        Ok(())
    }
    fn remove(&mut self, _: &str) -> Result<(), Error> {
        self.mutations += 1;
        Ok(())
    }
    fn rename(&mut self, _: &str, _: &str) -> Result<(), Error> {
        self.mutations += 1;
        Ok(())
    }
    fn resize(&mut self, _: &str, _: u64) -> Result<(), Error> {
        self.mutations += 1;
        Ok(())
    }
    fn set_times(
        &mut self,
        _: &str,
        _: Option<hyper_filesystem::Timestamp>,
        _: Option<hyper_filesystem::Timestamp>,
    ) -> Result<(), Error> {
        self.mutations += 1;
        Ok(())
    }
    fn sync(&mut self) -> Result<(), Error> {
        self.fences += 1;
        if self.fail_fence {
            Err(Error::Io)
        } else {
            Ok(())
        }
    }
}

fn request(op: Operation) -> Request<'static> {
    Request {
        window_token: 0,
        cursor: protocol::DirectoryCursor::default(),
        sequence: 1,
        operation: op,
        offset: 0,
        length: if op == Operation::Write { 4 } else { 0 },
        path: if matches!(op, Operation::TakeWriteback | Operation::Capabilities) {
            ""
        } else {
            "file"
        },
        target: "",
        accessed: None,
        modified: None,
    }
}

fn invoke(
    backend: &mut Backend,
    state: &mut writeback::State,
    op: Operation,
) -> Result<Result<u64, Error>, String> {
    let request = request(op);
    let mut reply = [0; protocol::RESPONSE_BYTES];
    dispatch(backend, state, request, &mut [1, 2, 3, 4], None, &mut reply)?;
    Ok(protocol::ResponseRef::decode(&reply, 1, op)
        .ok_or("bad reply")?
        .result())
}

#[test]
fn capability_query_is_readonly_and_range_rejection_preserves_support() -> Result<(), String> {
    let mut backend = Backend {
        periodic: true,
        ..Backend::default()
    };
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    let deadline = state.deadline();
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Capabilities)?,
        Ok(0)
    );
    assert!(!backend.vfs_owner);
    assert_eq!(state.deadline(), deadline);
    backend.read_extents = true;
    let advertised = protocol::Capabilities::READ_EXTENTS.bits();
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Capabilities)?,
        Ok(advertised)
    );
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::ReadExtents)?,
        Err(Error::Unsupported)
    );
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Capabilities)?,
        Ok(advertised)
    );
    assert_eq!(backend.mutations, 0);
    assert_eq!(backend.fences, 0);
    assert_eq!(state.deadline(), deadline);
    Ok(())
}

#[test]
fn malformed_capability_query_has_no_side_effects() -> Result<(), String> {
    let mut backend = Backend::default();
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    for field in 0..6 {
        let request = Request {
            sequence: 1,
            operation: Operation::Capabilities,
            window_token: 0,
            cursor: protocol::DirectoryCursor::START,
            path: if field == 0 { "file" } else { "" },
            target: if field == 1 { "target" } else { "" },
            offset: u64::from(field == 2),
            length: u64::from(field == 3),
            accessed: if field == 4 {
                hyper_filesystem::Timestamp::new(1, 0)
            } else {
                None
            },
            modified: if field == 5 {
                hyper_filesystem::Timestamp::new(1, 0)
            } else {
                None
            },
        };
        let mut reply = [0; protocol::RESPONSE_BYTES];
        dispatch(&mut backend, &mut state, request, &mut [], None, &mut reply)?;
        let response = protocol::ResponseRef::decode(&reply, 1, Operation::Capabilities)
            .ok_or("capability error reply")?;
        assert_eq!(response.result(), Err(Error::InvalidInput));
        assert!(response.features().supports_capability_query());
    }
    assert_eq!(backend.mutations, 0);
    assert_eq!(backend.fences, 0);
    assert!(!backend.vfs_owner);
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Capabilities)?,
        Ok(0)
    );
    Ok(())
}

#[test]
fn every_persistent_mutation_fences_before_its_reply() -> Result<(), String> {
    let mut backend = Backend::default();
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    for op in [
        Operation::Write,
        Operation::Create,
        Operation::Remove,
        Operation::Rename,
        Operation::Resize,
        Operation::SetMetadata,
    ] {
        assert!(invoke(&mut backend, &mut state, op)?.is_ok());
        assert_eq!(backend.fences, backend.mutations);
    }
    assert_eq!(backend.fences, 6);
    Ok(())
}

#[test]
fn wire_reply_preserves_bytes_then_reports_the_latched_fence_error() -> Result<(), String> {
    let mut backend = Backend {
        fail_fence: true,
        ..Backend::default()
    };
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    assert_eq!(invoke(&mut backend, &mut state, Operation::Write)?, Ok(4));
    backend.fail_fence = false;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Sync)?,
        Err(Error::Io)
    );
    assert_eq!(backend.mutations, 1);
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Write)?,
        Err(Error::Io)
    );
    assert_eq!(backend.fences, 1);
    Ok(())
}

#[test]
fn readonly_admission_never_calls_a_mutator() -> Result<(), String> {
    let mut backend = Backend::default();
    let mut state = writeback::State::new(WritebackPolicy::ReadOnly, 0).map_err(show)?;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Write)?,
        Err(Error::ReadOnly)
    );
    assert_eq!((backend.mutations, backend.fences), (0, 0));
    Ok(())
}

#[test]
fn ownership_transfer_requires_backend_acceptance_and_preserves_sync_fallback() -> Result<(), String>
{
    let mut backend = Backend {
        periodic: true,
        fail_fence: true,
        ..Backend::default()
    };
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::TakeWriteback)?,
        Err(Error::Io)
    );
    assert!(!backend.vfs_owner);
    assert_eq!(state.deadline(), 30_000_000_000);
    backend.fail_fence = false;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::TakeWriteback)?,
        Ok(30_000_000_000)
    );
    assert!(backend.vfs_owner);
    assert_eq!(state.deadline(), u64::MAX);
    let mut backend = Backend::default();
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::TakeWriteback)?,
        Ok(0)
    );
    assert!(!backend.vfs_owner);
    assert_eq!(invoke(&mut backend, &mut state, Operation::Write)?, Ok(4));
    assert_eq!(backend.fences, 1);
    Ok(())
}

#[test]
fn file_sync_falls_back_to_one_volume_fence_and_latches_io_failure() -> Result<(), String> {
    let mut backend = Backend::default();
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::SyncFile)?,
        Ok(0)
    );
    assert_eq!(backend.fences, 1);
    backend.fail_fence = true;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::SyncFile)?,
        Err(Error::Io)
    );
    backend.fail_fence = false;
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Write)?,
        Err(Error::Io)
    );
    assert_eq!(backend.fences, 2);
    Ok(())
}

#[test]
fn malformed_file_sync_does_not_fence_or_poison_the_mount() -> Result<(), String> {
    let mut backend = Backend::default();
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    for field in 0..4 {
        let request = Request {
            sequence: 1,
            operation: Operation::SyncFile,
            window_token: 0,
            cursor: protocol::DirectoryCursor::default(),
            path: "file",
            target: if field == 0 { "other" } else { "" },
            offset: u64::from(field == 1),
            length: if field == 2 { 2 } else { 0 },
            accessed: None,
            modified: if field == 3 {
                hyper_filesystem::Timestamp::new(1, 0)
            } else {
                None
            },
        };
        let mut reply = [0; protocol::RESPONSE_BYTES];
        dispatch(&mut backend, &mut state, request, &mut [], None, &mut reply)?;
        assert_eq!(
            protocol::ResponseRef::decode(&reply, 1, Operation::SyncFile)
                .ok_or("bad sync reply")?
                .result(),
            Err(Error::InvalidInput)
        );
        assert_eq!(backend.fences, 0);
    }
    assert_eq!(invoke(&mut backend, &mut state, Operation::Write)?, Ok(4));
    Ok(())
}

#[test]
fn preparation_validates_range_and_captures_policy_once() -> Result<(), String> {
    use std::time::Duration;
    for duration in [
        Duration::ZERO,
        Duration::from_millis(500),
        Duration::from_secs(3601),
        Duration::MAX,
    ] {
        let mut backend = Backend {
            policy: Some(WritebackPolicy::Periodic(duration)),
            ..Backend::default()
        };
        let error = PreparedRuntime::new(&mut backend, true, 0)
            .err()
            .ok_or("invalid policy accepted")?;
        assert!(error.contains("periodic interval"));
        assert_eq!(backend.enables, 0);
    }
    for seconds in [1, 3600] {
        let mut backend = Backend {
            policy: Some(WritebackPolicy::Periodic(Duration::from_secs(seconds))),
            ..Backend::default()
        };
        let mut runtime = PreparedRuntime::new(&mut backend, false, 0)?;
        assert_eq!(runtime.bytes.len(), protocol::REQUEST_BYTES);
        // A backend changing its getter later cannot change the admitted policy.
        backend.policy = Some(WritebackPolicy::Synchronous);
        assert_eq!(
            invoke(
                &mut backend,
                &mut runtime.writeback,
                Operation::TakeWriteback
            )?,
            Ok(seconds * 1_000_000_000)
        );
        assert_eq!(backend.enables, 1);
        assert!(runtime.writeback.delegated());
        assert_eq!(
            invoke(
                &mut backend,
                &mut runtime.writeback,
                Operation::TakeWriteback
            )?,
            Ok(seconds * 1_000_000_000)
        );
        assert_eq!(backend.enables, 1);
        assert_eq!(backend.policy_queries.get(), 1);
    }
    Ok(())
}

#[test]
fn resumed_runtime_is_ready_only_after_restored_owner_accepts() -> Result<(), String> {
    let mut backend = Backend {
        periodic: true,
        fail_fence: true,
        ..Backend::default()
    };
    assert!(PreparedRuntime::new(&mut backend, true, 0).is_err());
    assert!(!backend.vfs_owner);
    backend.fail_fence = false;
    let runtime = PreparedRuntime::new(&mut backend, true, 0)?;
    assert!(backend.vfs_owner);
    assert!(runtime.writeback.delegated());
    assert_eq!(runtime.writeback.deadline(), u64::MAX);
    let mut synchronous = Backend::default();
    assert!(PreparedRuntime::new(&mut synchronous, true, 0).is_err());
    assert_eq!(synchronous.enables, 0);
    Ok(())
}

#[test]
fn malformed_negotiation_has_no_backend_side_effects() -> Result<(), String> {
    let mut backend = Backend {
        periodic: true,
        ..Backend::default()
    };
    let mut runtime = PreparedRuntime::new(&mut backend, false, 0)?;
    let mut request = request(Operation::TakeWriteback);
    request.path = "unexpected";
    let mut reply = [0; protocol::RESPONSE_BYTES];
    assert_eq!(
        dispatch(
            &mut backend,
            &mut runtime.writeback,
            request,
            &mut [],
            None,
            &mut reply
        )?,
        Some(Error::InvalidInput)
    );
    assert_eq!(backend.enables, 0);
    assert_eq!(runtime.writeback.deadline(), 30_000_000_000);
    Ok(())
}

struct TestCompletion(
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    Option<Error>,
);
impl WriteCompletion for TestCompletion {
    fn wait(self: Box<Self>) -> Result<(), Error> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.1.map_or(Ok(()), Err)
    }
}

type AsyncReply = (
    Result<u64, Error>,
    [u8; protocol::WRITEBACK_COMPLETION_BYTES],
    Option<Error>,
);

fn async_invoke(
    backend: &mut Backend,
    state: &mut writeback::State,
    request: Request<'_>,
) -> Result<AsyncReply, String> {
    let mut reply = [0; protocol::RESPONSE_BYTES];
    let mut data = [0; protocol::WRITEBACK_COMPLETION_BYTES];
    let failure = dispatch(backend, state, request, &mut data, None, &mut reply)?;
    let decoded = protocol::ResponseRef::decode(&reply, request.sequence, request.operation)
        .ok_or("bad async reply")?;
    Ok((decoded.result(), data, failure))
}

#[test]
fn async_requires_separate_capability_and_delegation_and_keeps_readonly_guard() -> Result<(), String>
{
    let submit = Request {
        sequence: 4,
        length: 4,
        ..request(Operation::SubmitWriteback)
    };
    let mut backend = Backend {
        periodic: true,
        ..Backend::default()
    };
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    assert_eq!(
        async_invoke(&mut backend, &mut state, submit)?.0,
        Err(Error::Unsupported)
    );
    backend.async_writeback = true;
    assert_eq!(
        async_invoke(&mut backend, &mut state, submit)?.0,
        Err(Error::Unsupported)
    );
    assert_eq!(backend.mutations, 0);
    invoke(&mut backend, &mut state, Operation::TakeWriteback)?.map_err(show)?;
    assert_eq!(async_invoke(&mut backend, &mut state, submit)?.0, Ok(4));
    assert_eq!(backend.mutations, 1);
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Sync)?,
        Err(Error::InvalidInput)
    );
    assert_eq!(backend.fences, 0);
    let mut readonly = writeback::State::new(WritebackPolicy::ReadOnly, 0).map_err(show)?;
    assert_eq!(
        async_invoke(&mut backend, &mut readonly, submit)?.0,
        Err(Error::ReadOnly)
    );
    assert_eq!(backend.mutations, 1);
    Ok(())
}

#[test]
fn async_completion_returns_prefix_and_error_without_promising_rollback() -> Result<(), String> {
    let mut backend = Backend {
        periodic: true,
        async_writeback: true,
        accepted: Some(3),
        completion_error: Some(Error::Io),
        ..Backend::default()
    };
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    invoke(&mut backend, &mut state, Operation::TakeWriteback)?.map_err(show)?;
    let submit = Request {
        sequence: 4,
        length: 4,
        ..request(Operation::SubmitWriteback)
    };
    assert_eq!(async_invoke(&mut backend, &mut state, submit)?.0, Ok(3));
    let reap = Request {
        sequence: 5,
        offset: 4,
        path: "",
        ..request(Operation::ReapWriteback)
    };
    let (result, data, failure) = async_invoke(&mut backend, &mut state, reap)?;
    assert_eq!(result, Ok(protocol::WRITEBACK_COMPLETION_BYTES as u64));
    assert_eq!(failure, Some(Error::Io));
    assert_eq!(
        protocol::WritebackCompletion::decode(&data),
        Some(protocol::WritebackCompletion {
            batch: 4,
            accepted: 3,
            result: Err(Error::Io),
        })
    );
    assert_eq!(
        invoke(&mut backend, &mut state, Operation::Sync)?,
        Err(Error::Io)
    );
    assert_eq!(
        backend.completed.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    Ok(())
}

#[test]
fn malformed_async_requests_do_not_consume_or_mutate_backend() -> Result<(), String> {
    let mut backend = Backend {
        periodic: true,
        async_writeback: true,
        ..Backend::default()
    };
    let mut state = writeback::State::new(backend.writeback_policy(), 0).map_err(show)?;
    invoke(&mut backend, &mut state, Operation::TakeWriteback)?.map_err(show)?;
    let submit = Request {
        sequence: 4,
        length: 4,
        target: "unexpected",
        ..request(Operation::SubmitWriteback)
    };
    assert_eq!(
        async_invoke(&mut backend, &mut state, submit)?.0,
        Err(Error::InvalidInput)
    );
    assert_eq!(backend.mutations, 0);
    let submit = Request {
        target: "",
        ..submit
    };
    assert_eq!(async_invoke(&mut backend, &mut state, submit)?.0, Ok(4));
    let reap = Request {
        sequence: 5,
        offset: 4,
        ..request(Operation::ReapWriteback)
    };
    assert_eq!(
        async_invoke(&mut backend, &mut state, reap)?.0,
        Err(Error::InvalidInput)
    );
    assert_eq!(
        backend.completed.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!state.failed());
    let reap = Request { path: "", ..reap };
    assert_eq!(
        async_invoke(&mut backend, &mut state, reap)?.0,
        Ok(protocol::WRITEBACK_COMPLETION_BYTES as u64)
    );
    assert_eq!(
        async_invoke(
            &mut backend,
            &mut state,
            Request {
                sequence: 6,
                ..reap
            }
        )?
        .0,
        Err(Error::Corrupt)
    );
    Ok(())
}
