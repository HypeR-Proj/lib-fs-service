// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Common filesystem worker lifecycle and ordered range/metadata dispatch.
//! Drivers implement media semantics; capability ownership, request validation,
//! shared-memory ownership, mount publication and parent failure live here.

use hyper_filesystem::protocol::{self, Entry, Error, Operation, Request, Response};
use hyper_os::{
    block::BlockQueue,
    handle::{BlockQueueObject, ByteChannelObject, HandleRef},
    startup::Startup,
    wait::{self, ObjectSignals, WaitItem},
};

mod writeback;
pub use writeback::WritebackPolicy;

/// Backend operations run in request order. Io/Closed/Corrupt are terminal
/// protocol errors; ordinary path, capacity and unsupported-operation errors
/// affect the request. Sync failures are sticky in the service's writeback owner.
/// Successful writes report accepted prefixes, not durability. Shared window
/// borrows must retire all uses before returning, including error paths.
///
/// After `serve`/`resume` returns, the adapter still owns media teardown: close
/// its filesystem, drain/retire block work, then acknowledge shutdown or transfer
/// the handoff transport. A lost parent is not permission to reuse the medium.
pub trait Volume {
    /// Stable optional operations for this provider instance. A supported
    /// operation may still reject a particular path or range.
    fn capabilities(&self) -> protocol::Capabilities {
        protocol::Capabilities::NONE
    }

    /// Captured once before READY/RESUMED; it is immutable for this serving
    /// instance. Restored VFS ownership requires a periodic policy.
    fn writeback_policy(&self) -> WritebackPolicy {
        WritebackPolicy::Synchronous
    }

    /// Opt in only when writes retire bounded data-page staging before reply.
    /// Metadata/checkpoint ownership remains in the backend; VFS owns cadence.
    /// Failure must preserve the prior owner or return a terminal error. Do not
    /// publish a partial ownership change before returning an error.
    fn enable_vfs_writeback(&mut self) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    fn stat(&mut self, path: &str) -> Result<Entry, Error>;

    /// Enumerate ordinary children; dot entries are namespace traversal, not children.
    fn read_directory(
        &mut self,
        path: &str,
        cursor: protocol::DirectoryCursor,
        output: &mut [u8],
    ) -> Result<usize, Error>;

    fn read(&mut self, path: &str, offset: u64, output: &mut [u8]) -> Result<usize, Error>;

    fn read_extents(
        &mut self,
        _path: &str,
        _offset: u64,
        _length: usize,
        _output: &mut [u8],
    ) -> Result<usize, Error> {
        Err(Error::Unsupported)
    }

    fn readlink(&mut self, _path: &str, _output: &mut [u8]) -> Result<usize, Error> {
        Err(Error::Unsupported)
    }

    /// Return the accepted input prefix, which may be shorter than input.
    /// Preserve known progress when a later operation fails; a storage failure
    /// must remain observable through subsequent operations or sync. Success
    /// does not promise durability. An error is not a rollback guarantee.
    fn write(&mut self, path: &str, offset: u64, input: &[u8]) -> Result<usize, Error>;

    /// Same progress contract as write; retire every accepted block request
    /// before returning, including after an error, so the input can be reused.
    fn write_window(
        &mut self,
        path: &str,
        offset: u64,
        input: &[u8],
        _token: u64,
    ) -> Result<usize, Error> {
        self.write(path, offset, input)
    }

    fn create(&mut self, path: &str, directory: bool) -> Result<(), Error>;

    fn remove(&mut self, path: &str) -> Result<(), Error>;

    fn rename(&mut self, path: &str, target: &str) -> Result<(), Error>;

    fn resize(&mut self, path: &str, length: u64) -> Result<(), Error>;

    fn sync(&mut self) -> Result<(), Error>;

    /// Backends without a file recovery log may provide the stronger volume fence.
    fn sync_file(&mut self, path: &str, _mode: protocol::SyncMode) -> Result<(), Error> {
        self.stat(path)?;
        self.sync()
    }

    /// Backends may skip a periodic checkpoint after inspecting all dirty state.
    /// Explicit volume sync and teardown continue to use `sync`.
    fn periodic_sync(&mut self) -> Result<(), Error> {
        self.sync()
    }

    fn set_times(
        &mut self,
        path: &str,
        accessed: Option<hyper_filesystem::Timestamp>,
        modified: Option<hyper_filesystem::Timestamp>,
    ) -> Result<(), Error>;
}

/// A drained worker either acknowledges unmount, or transfers the still-mounted
/// transport. The manager retains exclusive NativeBlock ownership throughout.
pub enum WorkerReturn {
    Stopped(hyper_os::capability_channel::CapabilityChannel),
    Handoff(Transport),
}

/// One stopped consumer of the same ordered endpoint. No request or shared
/// buffer borrow crosses this boundary; pending requests remain in the channel.
pub struct Transport {
    control: hyper_os::capability_channel::CapabilityChannel,
    owner: hyper_os::handle::OwnedHandle<ByteChannelObject>,
    server: hyper_os::handle::OwnedHandle<ByteChannelObject>,
    buffer: hyper_os::filesystem::SharedBuffer,
    payload: PayloadTransport,
    sequence: u64,
    vfs_writeback: bool,
}

/// Blockless providers never receive a storage capability or window token.
/// A block-backed worker keeps the exact queue identity through handoff.
enum PayloadTransport {
    Buffered,
    Block {
        window: hyper_os::filesystem::ReadonlyBuffer,
        queue: BlockQueue,
    },
}

impl Transport {
    pub fn queue(&self) -> Option<HandleRef<'_, BlockQueueObject>> {
        match &self.payload {
            PayloadTransport::Buffered => None,
            PayloadTransport::Block { queue, .. } => Some(queue.as_handle_ref()),
        }
    }
}

/// Offer the transport to the manager, then serve only ordered kernel requests.
/// Success requires an explicit stop command; parent or transport death is an
/// error and never permits reusing a possibly unfinished block transaction.
pub fn serve(
    startup: &mut Startup<'_>,
    volume: &mut impl Volume,
    queue: BlockQueue,
) -> Result<WorkerReturn, String> {
    let window = hyper_os::filesystem::SharedBuffer::from_readonly_memory(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
        queue.create_window().map_err(show)?,
        protocol::DATA_BYTES,
    )
    .map_err(show)?;
    serve_payload(startup, volume, PayloadTransport::Block { window, queue })
}

/// Serve an in-memory or generated filesystem without granting block authority.
/// Payloads use the ordered shared buffer and no window token is accepted.
pub fn serve_buffered(
    startup: &mut Startup<'_>,
    volume: &mut impl Volume,
) -> Result<WorkerReturn, String> {
    serve_payload(startup, volume, PayloadTransport::Buffered)
}

fn serve_payload(
    startup: &mut Startup<'_>,
    volume: &mut impl Volume,
    payload: PayloadTransport,
) -> Result<WorkerReturn, String> {
    let runtime = PreparedRuntime::new(volume, false, now()?)?;
    use hyper_os::{
        capability_channel::{CapabilityChannel, CapabilityDisposition},
        handle::RightsOffer,
    };
    use hyper_service::filesystem;
    let control =
        CapabilityChannel::from_handle(startup.take(filesystem::WORKER_CONTROL).map_err(show)?);
    let owner = startup.take(filesystem::OWNER).map_err(show)?;
    let (client, server) = hyper_os::channel::create_pair().map_err(show)?;
    let buffer = hyper_os::filesystem::SharedBuffer::create(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
        protocol::DATA_BYTES,
    )
    .map_err(show)?;
    if volume.stat("").map_err(show)?.kind != protocol::DIRECTORY {
        return Err("filesystem root is not a directory".into());
    }
    hyper_os::memory::lock_current_memory(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
    )
    .map_err(show)?;
    let mut client = Some(client);
    {
        let mut dispositions = vec![
            CapabilityDisposition::move_handle(
                &mut client,
                RightsOffer::Exact(filesystem::TRANSPORT_RIGHTS),
            )
            .map_err(show)?,
            CapabilityDisposition::duplicate(
                buffer.memory().as_handle_ref(),
                RightsOffer::Exact(filesystem::BUFFER_RIGHTS),
            )
            .map_err(show)?,
        ];
        if let PayloadTransport::Block { queue, .. } = &payload {
            dispositions.push(
                CapabilityDisposition::duplicate(
                    queue.as_handle_ref(),
                    RightsOffer::Exact(filesystem::QUEUE_RIGHTS),
                )
                .map_err(show)?,
            );
        }
        hyper_service::io::send_capabilities(
            &control,
            filesystem::WORKER_READY,
            &mut dispositions,
            hyper_os::DEADLINE_INFINITE,
        )
        .map_err(show)?;
    }
    serve_transport(
        Transport {
            control,
            owner,
            server,
            buffer,
            payload,
            sequence: 0,
            vfs_writeback: false,
        },
        volume,
        runtime,
    )
}

struct PreparedRuntime {
    bytes: Vec<u8>,
    writeback: writeback::State,
}

impl PreparedRuntime {
    fn new(volume: &mut impl Volume, delegated: bool, now: u64) -> Result<Self, String> {
        let policy = volume.writeback_policy();
        policy.validate()?;
        let mut writeback = writeback::State::new(policy, now).map_err(show)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(protocol::REQUEST_BYTES)
            .map_err(show)?;
        bytes.resize(protocol::REQUEST_BYTES, 0);
        if delegated {
            if !matches!(policy, WritebackPolicy::Periodic(_)) {
                return Err("adopted backend lost VFS writeback support".into());
            }
            writeback
                .take_writeback(|| volume.enable_vfs_writeback())
                .map_err(show)?;
        }
        Ok(Self { bytes, writeback })
    }
}

fn serve_transport(
    state: Transport,
    volume: &mut impl Volume,
    runtime: PreparedRuntime,
) -> Result<WorkerReturn, String> {
    use hyper_service::filesystem;
    let Transport {
        control,
        owner,
        server,
        mut buffer,
        payload,
        mut sequence,
        vfs_writeback: _,
    } = state;
    let PreparedRuntime {
        mut bytes,
        mut writeback,
    } = runtime;
    let mut reply = [0; protocol::RESPONSE_BYTES];
    let trace = hyper_os::channel::RpcTracer::new(server.as_handle_ref());
    loop {
        if writeback.deadline() != hyper_os::DEADLINE_INFINITE {
            let current = now()?;
            if current >= writeback.deadline() {
                writeback
                    .periodic(current, || volume.periodic_sync())
                    .map_err(|error| format!("periodic filesystem sync failed: {error:?}"))?;
                writeback.coalesce(now()?);
            }
        }
        let observed = match wait::wait_many(
            &[
                WaitItem::new(
                    owner.as_handle_ref(),
                    ObjectSignals::<ByteChannelObject>::READABLE
                        .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                ),
                WaitItem::new(
                    server.as_handle_ref(),
                    ObjectSignals::<ByteChannelObject>::READABLE
                        .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                ),
            ],
            writeback.deadline(),
        ) {
            Ok(observed) => observed,
            Err(hyper_os::Error::Status(hyper_os::Status::TIMED_OUT)) => continue,
            Err(error) => return Err(show(error)),
        };
        if ObjectSignals::<ByteChannelObject>::PEER_CLOSED.is_present_in(observed.observed) {
            return Err("filesystem owner or transport disconnected".into());
        }
        if observed.index == 0 {
            let mut command = [0; 16];
            let count = owner
                .as_byte_channel()
                .receive(&mut command)
                .map_err(show)?;
            if &command[..count] == filesystem::WORKER_HANDOFF {
                // Requests are sequential. This branch is entered only after
                // the preceding reply was published and its buffer borrow ended.
                writeback.admit(Operation::Sync).map_err(show)?;
                volume.sync().map_err(show)?;
                return Ok(WorkerReturn::Handoff(Transport {
                    control,
                    owner,
                    server,
                    buffer,
                    payload,
                    sequence,
                    vfs_writeback: writeback.delegated(),
                }));
            }
            if &command[..count] != filesystem::WORKER_STOP {
                return Err("invalid worker stop command".into());
            }
            // The kernel fence already succeeded. Close mapping and transport
            // before returning the block; only the manager can reuse it next.
            drop(buffer);
            drop(server);
            return Ok(WorkerReturn::Stopped(control));
        }
        let length = match server.as_byte_channel().try_receive(&mut bytes) {
            Ok(length) => length,
            Err(hyper_os::Error::Status(hyper_os::Status::WOULD_BLOCK)) => continue,
            Err(error) => return Err(format!("receive filesystem request: {error:?}")),
        };
        let request = Request::decode(&bytes[..length]).ok_or("malformed filesystem request")?;
        if request.sequence <= sequence {
            return Err("filesystem sequence mismatch".into());
        }
        sequence = request.sequence;
        let context = trace.server_begin(sequence, request.operation as u64, None);
        // SAFETY: The received request transfers exclusive ownership of this
        // shared byte range. Kernel waits for the reply before accessing it.
        let result = dispatch(
            volume,
            &mut writeback,
            request,
            unsafe { buffer.bytes_mut() },
            match &payload {
                PayloadTransport::Buffered => None,
                PayloadTransport::Block { window, .. } => {
                    // SAFETY: The ordered request owns this window until its
                    // reply. No slice escapes dispatch or survives the reply.
                    Some(unsafe { window.bytes() })
                }
            },
            &mut reply,
        )?;
        let sent = server.as_byte_channel().send(&reply);
        trace.server_end(
            context,
            if sent.is_err() {
                u64::MAX
            } else {
                result.map_or(0, |error| error as u64)
            },
        );
        sent.map_err(show)?;
        if result.is_some_and(Error::terminal) {
            return Err("filesystem backend failed".into());
        }
    }
}

/// Acknowledge teardown after the adapter has drained its own submissions.
/// The manager retains NativeBlock and independently retires the delegated queue.
pub fn finish(completion: WorkerReturn) -> Result<(), String> {
    match completion {
        WorkerReturn::Stopped(control) => hyper_service::io::send_capabilities(
            &control,
            hyper_service::filesystem::WORKER_RETURN,
            &mut [],
            hyper_os::DEADLINE_INFINITE,
        )
        .map_err(show),
        WorkerReturn::Handoff(state) => {
            use hyper_os::{capability_channel::CapabilityDisposition, handle::RightsOffer};
            use hyper_service::filesystem;
            let PayloadTransport::Block { window, queue } = state.payload else {
                return Err("handoff requires a block-backed transport".into());
            };
            let mut server = Some(state.server);
            let mut queue = Some(queue.into_handle());
            let mut offers = [
                CapabilityDisposition::move_handle(
                    &mut server,
                    RightsOffer::Exact(filesystem::TRANSPORT_RIGHTS),
                )
                .map_err(show)?,
                CapabilityDisposition::duplicate(
                    state.buffer.memory().as_handle_ref(),
                    RightsOffer::Exact(filesystem::SERVER_BUFFER_RIGHTS),
                )
                .map_err(show)?,
                CapabilityDisposition::move_handle(
                    &mut queue,
                    RightsOffer::Exact(filesystem::QUEUE_RIGHTS),
                )
                .map_err(show)?,
            ];
            let mut record = [0; 24];
            record[..8].copy_from_slice(filesystem::WORKER_STATE);
            record[8..16].copy_from_slice(&state.sequence.to_le_bytes());
            record[16..24].copy_from_slice(&u64::from(state.vfs_writeback).to_le_bytes());
            hyper_service::io::send_capabilities(
                &state.control,
                &record,
                &mut offers,
                hyper_os::DEADLINE_INFINITE,
            )
            .map_err(show)?;
            drop(state.buffer);
            drop(window);
            drop(state.owner);
            drop(state.control);
            loop {
                std::thread::park();
            }
        }
    }
}

/// Receive the original transport after the manager has loaded this process
/// resident. Queue ownership moves only after the previous worker drains it.
pub fn adopt(startup: &mut Startup<'_>) -> Result<Transport, String> {
    use hyper_os::{
        capability_channel::{CapabilityChannel, CapabilityReceiveSlot},
        handle::VmoObject,
    };
    use hyper_service::filesystem;
    let control =
        CapabilityChannel::from_handle(startup.take(filesystem::WORKER_CONTROL).map_err(show)?);
    let owner = startup.take(filesystem::OWNER).map_err(show)?;
    // Read faults remain serviceable until the old worker hands over. Lock
    // the new worker's current mappings before allowing that rendezvous.
    hyper_os::memory::lock_current_memory(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
    )
    .map_err(show)?;
    hyper_service::io::send_capabilities(
        &control,
        filesystem::WORKER_WAITING,
        &mut [],
        hyper_os::DEADLINE_INFINITE,
    )
    .map_err(show)?;
    let mut bytes = [core::mem::MaybeUninit::uninit(); 24];
    let mut slots = [
        CapabilityReceiveSlot::new::<ByteChannelObject>(filesystem::TRANSPORT_RIGHTS),
        CapabilityReceiveSlot::new::<VmoObject>(filesystem::SERVER_BUFFER_RIGHTS),
        CapabilityReceiveSlot::new::<BlockQueueObject>(filesystem::QUEUE_RIGHTS),
    ];
    let message = control
        .receive(hyper_os::DEADLINE_INFINITE, &mut bytes, &mut slots)
        .map_err(show)?;
    if message.bytes().len() != 24
        || &message.bytes()[..8] != filesystem::WORKER_STATE
        || message.capability_count() != 3
    {
        return Err("invalid filesystem adoption record".into());
    }
    let sequence = u64::from_le_bytes(message.bytes()[8..16].try_into().map_err(show)?);
    let vfs_writeback = match u64::from_le_bytes(message.bytes()[16..24].try_into().map_err(show)?)
    {
        0 => false,
        1 => true,
        _ => return Err("invalid filesystem writeback owner".into()),
    };
    let server = slots[0]
        .take::<ByteChannelObject>()
        .map_err(show)?
        .ok_or("missing adopted endpoint")?;
    let memory = slots[1]
        .take::<VmoObject>()
        .map_err(show)?
        .ok_or("missing adopted shared memory")?;
    let queue = BlockQueue::from_handle(
        slots[2]
            .take::<BlockQueueObject>()
            .map_err(show)?
            .ok_or("missing adopted block queue")?,
    );
    let window = hyper_os::filesystem::SharedBuffer::from_readonly_memory(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
        queue.create_window().map_err(show)?,
        protocol::DATA_BYTES,
    )
    .map_err(show)?;
    let buffer = hyper_os::filesystem::SharedBuffer::from_memory(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
        memory,
        protocol::DATA_BYTES,
    )
    .map_err(show)?;
    Ok(Transport {
        control,
        owner,
        server,
        buffer,
        payload: PayloadTransport::Block { window, queue },
        sequence,
        vfs_writeback,
    })
}

pub fn resume(state: Transport, volume: &mut impl Volume) -> Result<WorkerReturn, String> {
    let runtime = PreparedRuntime::new(volume, state.vfs_writeback, now()?)?;
    hyper_service::io::send_capabilities(
        &state.control,
        hyper_service::filesystem::WORKER_RESUMED,
        &mut [],
        hyper_os::DEADLINE_INFINITE,
    )
    .map_err(show)?;
    serve_transport(state, volume, runtime)
}

fn now() -> Result<u64, String> {
    hyper_os::time::monotonic_now()
        .map(|instant| instant.as_nanoseconds())
        .map_err(show)
}

fn dispatch(
    volume: &mut impl Volume,
    writeback: &mut writeback::State,
    request: Request<'_>,
    data: &mut [u8],
    window: Option<&[u8]>,
    reply: &mut [u8; protocol::RESPONSE_BYTES],
) -> Result<Option<Error>, String> {
    let mut entry = Entry::empty();
    let result = writeback
        .admit(request.operation)
        .and_then(|()| match request.operation {
            Operation::Capabilities => {
                if !request.path.is_empty()
                    || !request.target.is_empty()
                    || request.offset != 0
                    || request.length != 0
                    || request.accessed.is_some()
                    || request.modified.is_some()
                {
                    return Err(Error::InvalidInput);
                }
                Ok(volume.capabilities().bits())
            }
            Operation::Stat => {
                entry = volume.stat(request.path)?;
                Ok(0)
            }
            Operation::ReadDirectory => {
                let length = usize::try_from(request.length).map_err(|_| Error::InvalidInput)?;
                if request.offset != 0
                    || !request.target.is_empty()
                    || request.accessed.is_some()
                    || request.modified.is_some()
                    || !(protocol::DIRECTORY_MIN_BYTES..=protocol::DIRECTORY_BATCH_BYTES)
                        .contains(&length)
                {
                    return Err(Error::InvalidInput);
                }
                volume
                    .read_directory(
                        request.path,
                        request.cursor,
                        data.get_mut(..length).ok_or(Error::InvalidInput)?,
                    )
                    .map(|n| n as u64)
            }
            Operation::Read => {
                let length = usize::try_from(request.length).map_err(|_| Error::InvalidInput)?;
                volume
                    .read(
                        request.path,
                        request.offset,
                        data.get_mut(..length).ok_or(Error::InvalidInput)?,
                    )
                    .map(|n| n as u64)
            }
            Operation::ReadExtents => {
                let length = usize::try_from(request.length).map_err(|_| Error::InvalidInput)?;
                if length > protocol::DATA_BYTES || !request.target.is_empty() {
                    return Err(Error::InvalidInput);
                }
                volume
                    .read_extents(request.path, request.offset, length, data)
                    .map(|n| n as u64)
            }
            Operation::ReadLink => {
                let length = usize::try_from(request.length).map_err(|_| Error::InvalidInput)?;
                volume
                    .readlink(
                        request.path,
                        data.get_mut(..length).ok_or(Error::InvalidInput)?,
                    )
                    .map(|n| n as u64)
            }
            Operation::Write => {
                let length = usize::try_from(request.length).map_err(|_| Error::InvalidInput)?;
                match (window, request.window_token) {
                    (None, 0) => volume.write(
                        request.path,
                        request.offset,
                        data.get(..length).ok_or(Error::InvalidInput)?,
                    ),
                    (Some(window), token) if token != 0 => volume.write_window(
                        request.path,
                        request.offset,
                        window.get(..length).ok_or(Error::InvalidInput)?,
                        token,
                    ),
                    _ => Err(Error::InvalidInput),
                }
                .map(|n| n as u64)
            }
            Operation::Create if request.length <= 1 => {
                volume.create(request.path, request.length != 0).map(|_| 0)
            }
            Operation::Create => Err(Error::InvalidInput),
            Operation::Remove => volume.remove(request.path).map(|_| 0),
            Operation::Rename => volume.rename(request.path, request.target).map(|_| 0),
            Operation::Resize => volume.resize(request.path, request.length).map(|_| 0),
            Operation::TakeWriteback => {
                if !request.path.is_empty()
                    || !request.target.is_empty()
                    || request.offset != 0
                    || request.length != 0
                    || request.accessed.is_some()
                    || request.modified.is_some()
                {
                    return Err(Error::InvalidInput);
                }
                writeback.take_writeback(|| volume.enable_vfs_writeback())
            }
            Operation::Sync => volume.sync().map(|_| 0),
            Operation::SyncFile => {
                if !request.target.is_empty()
                    || request.offset != 0
                    || request.accessed.is_some()
                    || request.modified.is_some()
                {
                    return Err(Error::InvalidInput);
                }
                let mode = protocol::SyncMode::decode(request.length).ok_or(Error::InvalidInput)?;
                volume.sync_file(request.path, mode).map(|_| 0)
            }
            Operation::SetMetadata => volume
                .set_times(request.path, request.accessed, request.modified)
                .map(|_| 0),
        });
    let result = writeback.complete(request.operation, result, || volume.sync());

    let failure = result.err();
    Response {
        sequence: request.sequence,
        operation: request.operation,
        result,
        entry,
    }
    .encode_with_features(reply, protocol::ResponseFeatures::CAPABILITY_QUERY)
    .ok_or_else(|| format!("invalid filesystem {:?} response", request.operation))?;
    Ok(failure)
}

fn show(error: impl core::fmt::Debug) -> String {
    format!("{error:?}")
}

#[cfg(test)]
mod tests;
