// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Default)]
struct Backend {
    mutations: usize,
    fences: usize,
    fail_fence: bool,
}
impl Volume for Backend {
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

fn invoke(
    backend: &mut Backend,
    state: &mut writeback::State,
    op: Operation,
) -> Result<Result<u64, Error>, String> {
    let request = Request {
        window_token: 0,
        cursor: protocol::DirectoryCursor::default(),
        sequence: 1,
        operation: op,
        offset: 0,
        length: if op == Operation::Write { 4 } else { 0 },
        path: "file",
        target: "",
        accessed: None,
        modified: None,
    };
    let mut reply = [0; protocol::RESPONSE_BYTES];
    dispatch(backend, state, request, &mut [1, 2, 3, 4], None, &mut reply)?;
    Ok(protocol::ResponseRef::decode(&reply, 1, op)
        .ok_or("bad reply")?
        .result())
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
