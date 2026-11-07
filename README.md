<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# lib-fs-service

Common Native filesystem worker transport and lifecycle.

Format adapters implement the Volume contract. This library validates requests, owns
shared request buffers, orders range/metadata operations and coordinates mount
publication, parent failure and teardown acknowledgement.

See the [implementation entry](src/lib.rs) and [component declaration](component.json)
for the public interfaces, build dependencies and delivered artifacts.

## Boundaries

Accepted write progress is distinct from durable completion. Drivers must preserve
partial progress and report their media errors; the transport does not turn a late flush
failure into a guaranteed zero-byte write.

`Volume::writeback_policy` defaults to `Synchronous`: every mutating request
completes the backend sync before its reply, including cleanup after a partial
failure. Known write progress is still an accepted prefix, not proof of durability;
a late fence error is latched and subsequent requests/sync report failure.
`Periodic(Duration)` is reserved for recoverable backends. Before negotiation,
the ordered request loop performs idle/busy periodic sync; `periodic_sync` may
skip clean work. A backend may implement `enable_vfs_writeback` to accept the
`TakeWriteback` mount request. Ordinary `Write` still retires bounded data staging
before its reply; asynchronous acceptance uses a separately negotiated operation. The reply supplies the interval and disables the service timer: VFS now
owns dirty file pages, pressure submission, and data-before-checkpoint ordering.
`Sync` requests the full volume fence. `SyncFile` names one path and encodes
`SyncMode` in `length` (0 for data and recovery metadata, 1 for all file metadata);
the other payload fields are empty. `Volume::sync_file` defaults to validating
the path and calling volume sync. Recoverable backends may override it to use
file recovery records with checkpoint fallback. Neither operation restarts the
periodic deadline. File-sync device errors are sticky; malformed scopes and
missing paths do not poison a healthy mount.

The 24-byte worker handoff record contains the eight-byte marker, sequence and
VFS-writeback owner flag (u64, 0 or 1), with server endpoint, shared buffer and
block queue capabilities. NativeBlock stays in the manager. The successor restores that owner before
servicing requests; bootstrap media export zero. `ReadOnly` rejects mutations
before dispatch and `Volatile` makes no media promise.

Periodic success includes the backend's device-reported completion, not physical
readback verification. The interval is a scheduling cadence, not a hard maximum
persistence latency. Failure never advances the deadline, and stopping or handing
off the service cannot leave a detached synchronization thread.

## Bounded asynchronous writeback

`Capabilities::ASYNC_WRITEBACK` is independent of periodic synchronization policy.
A provider must advertise this capability and successfully delegate periodic
writeback before accepting `SubmitWriteback`. FAT and other nonparticipants keep
ordinary synchronous `Write` behavior; callers must not infer support from a
filesystem name or a nonzero interval.

`SubmitWriteback` uses the existing write fields and input window. The backend
reserves its receipt, staging and completion resources before mutation, consumes
all borrowed input before replying, and returns the accepted byte prefix. Its
request sequence identifies the batch within this transport generation. The
service retains at most two receipts, each covering at most 512 KiB. A rejected
capacity admission invokes no backend mutation. After partial mutation, return
an accepted prefix with a receipt carrying any late error; ambiguous progress
requires a terminal failure, never an automatic retry.

`ReapWriteback` names the oldest pending submission sequence in `offset`; all
other payload fields are empty. Its successful reply publishes a 24-byte
`WritebackCompletion` in shared data: batch ID, accepted byte count and outcome.
The byte count repeats accepted input, not a measured physical completion prefix.
Success means all accepted data writeback completed; failure may have partial
device effects. Neither outcome is a checkpoint or durability fence. Zero,
unknown, out-of-order and duplicate batch IDs are corrupt protocol transactions.
The endpoint and its monotonic sequence bind identity; a lost acceptance reply
retires the session rather than guessing whether to resubmit.

Physical completion must progress without calling `WriteCompletion::wait` and
without the mutable request actor, VFS state lock or caller's file-content lock.
This lets the actor wait for an older same-page writeback or reap a receipt without
blocking the worker that clears page writeback state. Dropped receipts must retire
safely or transfer to a completion owner joined before backend teardown. The
current ordered transport needs no unsolicited completion frames or extra input
windows: acceptance releases the input window, while VFS retains exact dirty
versions until their corresponding completion is collected.

Before any ordinary metadata, read, sync or structural operation, the client
reaps pending receipts. These conservative fences preserve the sequential Volume
contract; cache-only VFS activity may continue independently. Any completion
failure is sticky and closes serving after the completion reply. Teardown still
requires physical queue retirement before the manager reuses the medium.

## Build and test

Use this checkout inside the [repo-managed workspace](https://github.com/HypeR-Proj/HypeR).
From this repository directory:

```sh
make
make test
```

An absolute `OUT=/path/to/out` selects a shared output directory. The module
entry delegates to build/system, which prepares the installed SDK and declared
dependencies. Native binaries require HypeR; host module checks cover the tests
selected by component.json. A standalone clone does not contain the workspace’s
other source repositories or Native SDK.

## Preparing and publishing a worker

The shared filesystem protocol defines the periodic range as 1 second through
1 hour. Zero is only the non-periodic negotiation sentinel. Configure and validate
`WritebackPolicy` before publication; the service captures it once for each
serving instance. Request handling never re-queries a mutable policy getter.
The receiver still validates untrusted negotiation replies independently.

`WORKER_READY` is sent after root validation, request-buffer allocation and policy
preparation. `WORKER_RESUMED` additionally requires successful restoration of VFS
writeback ownership. Neither means a particular application request was accepted.
A failed preparation publishes neither success notification. During negotiation,
validate first, invoke the backend hook, then commit timer ownership. Hook failure
must retain the prior owner or be terminal; repeated successful negotiation does
not run the enable hook again.

After `serve` or `resume` returns, the format adapter closes its filesystem and
retires outstanding media work before calling `finish` to acknowledge teardown.
The manager independently revokes and drains the queue before reuse. Handoff
retains the mounted endpoint and sequence. Transport closure, process exit and
logical request cancellation do not establish physical I/O retirement. See the
`Volume` trait for terminal errors and borrowed-window lifetime obligations.
