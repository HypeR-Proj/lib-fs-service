// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Common filesystem worker lifecycle and ordered range/metadata dispatch.
//! Drivers implement media semantics; capability ownership, request validation,
//! shared-memory ownership, mount publication and parent failure live here.

use hyper_filesystem::protocol::{self, Entry, Error, Operation, Request, Response};
use hyper_os::{
    handle::ByteChannelObject,
    startup::Startup,
    wait::{self, ObjectSignals, WaitItem},
};

pub trait Volume {
    fn stat(&mut self, path: &str) -> Result<Entry, Error>;

    /// Enumerate ordinary children; dot entries are namespace traversal, not children.
    fn read_directory(
        &mut self,
        path: &str,
        cursor: protocol::DirectoryCursor,
        output: &mut [u8],
    ) -> Result<usize, Error>;

    fn read(&mut self, path: &str, offset: u64, output: &mut [u8]) -> Result<usize, Error>;

    fn readlink(&mut self, _path: &str, _output: &mut [u8]) -> Result<usize, Error> {
        Err(Error::Unsupported)
    }

    fn write(&mut self, path: &str, offset: u64, input: &[u8]) -> Result<usize, Error>;

    fn create(&mut self, path: &str, directory: bool) -> Result<(), Error>;

    fn remove(&mut self, path: &str) -> Result<(), Error>;

    fn rename(&mut self, path: &str, target: &str) -> Result<(), Error>;

    fn resize(&mut self, path: &str, length: u64) -> Result<(), Error>;

    fn sync(&mut self) -> Result<(), Error>;

    fn set_times(
        &mut self,
        path: &str,
        accessed: Option<hyper_filesystem::Timestamp>,
        modified: Option<hyper_filesystem::Timestamp>,
    ) -> Result<(), Error>;
}

/// A drained worker either returns a block after unmount, or transfers the
/// still-mounted transport. Both outcomes retain exclusive media ownership.
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
    sequence: u64,
}

/// Offer the transport to the manager, then serve only ordered kernel requests.
/// Success requires an explicit stop command; parent or transport death is an
/// error and never permits reusing a possibly unfinished block transaction.
pub fn serve(startup: &mut Startup<'_>, volume: &mut impl Volume) -> Result<WorkerReturn, String> {
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
        let mut dispositions = [
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
            sequence: 0,
        },
        volume,
    )
}

fn serve_transport(state: Transport, volume: &mut impl Volume) -> Result<WorkerReturn, String> {
    use hyper_service::filesystem;
    let Transport {
        control,
        owner,
        server,
        mut buffer,
        mut sequence,
    } = state;
    let mut bytes = vec![0; protocol::REQUEST_BYTES];
    let mut reply = [0; protocol::RESPONSE_BYTES];
    let trace = hyper_os::channel::RpcTracer::new(server.as_handle_ref());
    loop {
        let observed = wait::wait_many(
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
            hyper_os::DEADLINE_INFINITE,
        )
        .map_err(show)?;
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
                volume.sync().map_err(show)?;
                return Ok(WorkerReturn::Handoff(Transport {
                    control,
                    owner,
                    server,
                    buffer,
                    sequence,
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
        let result = dispatch(volume, request, unsafe { buffer.bytes_mut() }, &mut reply)?;
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

/// Complete graceful teardown without closing or reactivating the `NativeBlock`.
pub fn return_block(
    completion: WorkerReturn,
    block: hyper_os::block::NativeBlock,
) -> Result<(), String> {
    use hyper_os::{capability_channel::CapabilityDisposition, handle::RightsOffer};
    let mut block = Some(block.into_handle());
    match completion {
        WorkerReturn::Stopped(control) => {
            let mut offers = [CapabilityDisposition::move_handle(
                &mut block,
                RightsOffer::Exact(hyper_os::block::RIGHTS),
            )
            .map_err(show)?];
            hyper_service::io::send_capabilities(
                &control,
                hyper_service::filesystem::WORKER_RETURN,
                &mut offers,
                hyper_os::DEADLINE_INFINITE,
            )
            .map_err(show)
        }
        WorkerReturn::Handoff(state) => {
            let mut server = Some(state.server);
            let mut offers = [
                CapabilityDisposition::move_handle(
                    &mut block,
                    RightsOffer::Exact(hyper_os::block::RIGHTS),
                )
                .map_err(show)?,
                CapabilityDisposition::move_handle(
                    &mut server,
                    RightsOffer::Exact(hyper_service::filesystem::TRANSPORT_RIGHTS),
                )
                .map_err(show)?,
                CapabilityDisposition::duplicate(
                    state.buffer.memory().as_handle_ref(),
                    RightsOffer::Exact(hyper_service::filesystem::SERVER_BUFFER_RIGHTS),
                )
                .map_err(show)?,
            ];
            let mut record = [0; 16];
            record[..8].copy_from_slice(hyper_service::filesystem::WORKER_STATE);
            record[8..].copy_from_slice(&state.sequence.to_le_bytes());
            hyper_service::io::send_capabilities(
                &state.control,
                &record,
                &mut offers,
                hyper_os::DEADLINE_INFINITE,
            )
            .map_err(show)?;
            drop(state.buffer);
            drop(state.owner);
            drop(state.control);
            // Only system init decides when the old process retires. The
            // successful MOVE has already removed every active transport owner.
            loop {
                std::thread::park();
            }
        }
    }
}

/// Receive the original transport after the manager has loaded this process
/// resident. No filesystem operations run until exclusive block ownership moves.
pub fn adopt(
    startup: &mut Startup<'_>,
) -> Result<(hyper_os::block::NativeBlock, Transport), String> {
    use hyper_os::{
        capability_channel::{CapabilityChannel, CapabilityReceiveSlot},
        handle::{NativeBlockObject, VmoObject},
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
    let mut bytes = [core::mem::MaybeUninit::uninit(); 16];
    let mut slots = [
        CapabilityReceiveSlot::new::<NativeBlockObject>(hyper_os::block::RIGHTS),
        CapabilityReceiveSlot::new::<ByteChannelObject>(filesystem::TRANSPORT_RIGHTS),
        CapabilityReceiveSlot::new::<VmoObject>(filesystem::SERVER_BUFFER_RIGHTS),
    ];
    let message = control
        .receive(hyper_os::DEADLINE_INFINITE, &mut bytes, &mut slots)
        .map_err(show)?;
    if message.bytes().len() != 16
        || &message.bytes()[..8] != filesystem::WORKER_STATE
        || message.capability_count() != 3
    {
        return Err("invalid filesystem adoption record".into());
    }
    let sequence = u64::from_le_bytes(message.bytes()[8..].try_into().map_err(show)?);
    let block = slots[0]
        .take::<NativeBlockObject>()
        .map_err(show)?
        .ok_or("missing adopted block")?;
    let server = slots[1]
        .take::<ByteChannelObject>()
        .map_err(show)?
        .ok_or("missing adopted endpoint")?;
    let memory = slots[2]
        .take::<VmoObject>()
        .map_err(show)?
        .ok_or("missing adopted shared memory")?;
    let buffer = hyper_os::filesystem::SharedBuffer::from_memory(
        startup.borrow(hyper_os::startup::ROOT_VMAR).map_err(show)?,
        memory,
        protocol::DATA_BYTES,
    )
    .map_err(show)?;
    Ok((
        hyper_os::block::NativeBlock::from_handle(block),
        Transport {
            control,
            owner,
            server,
            buffer,
            sequence,
        },
    ))
}

pub fn resume(state: Transport, volume: &mut impl Volume) -> Result<WorkerReturn, String> {
    hyper_service::io::send_capabilities(
        &state.control,
        hyper_service::filesystem::WORKER_RESUMED,
        &mut [],
        hyper_os::DEADLINE_INFINITE,
    )
    .map_err(show)?;
    serve_transport(state, volume)
}

fn dispatch(
    volume: &mut impl Volume,
    request: Request<'_>,
    data: &mut [u8],
    reply: &mut [u8; protocol::RESPONSE_BYTES],
) -> Result<Option<Error>, String> {
    let mut entry = Entry::empty();
    let result = (|| match request.operation {
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
            volume
                .write(
                    request.path,
                    request.offset,
                    data.get(..length).ok_or(Error::InvalidInput)?,
                )
                .map(|n| n as u64)
        }
        Operation::Create if request.length <= 1 => {
            volume.create(request.path, request.length != 0).map(|_| 0)
        }
        Operation::Create => Err(Error::InvalidInput),
        Operation::Remove => volume.remove(request.path).map(|_| 0),
        Operation::Rename => volume.rename(request.path, request.target).map(|_| 0),
        Operation::Resize => volume.resize(request.path, request.length).map(|_| 0),
        Operation::Sync => volume.sync().map(|_| 0),
        Operation::SetMetadata => volume
            .set_times(request.path, request.accessed, request.modified)
            .map(|_| 0),
    })();

    let failure = result.err();
    Response {
        sequence: request.sequence,
        operation: request.operation,
        result,
        entry,
    }
    .encode(reply)
    .ok_or_else(|| format!("invalid filesystem {:?} response", request.operation))?;
    Ok(failure)
}

fn show(error: impl core::fmt::Debug) -> String {
    format!("{error:?}")
}
