<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# lib-fs-service

Common Native filesystem worker transport and lifecycle.

Format adapters implement the Volume contract. This library validates requests, owns
shared request buffers, orders range/metadata operations and coordinates mount
publication, parent failure and device return.

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
`Periodic(Duration)` is reserved for backends with their own recoverable dirty
cache and background writer. A deadline in the same request owner handles both
idle and continuously busy mounts without concurrent access to the backend.
`periodic_sync` defaults to `sync`; a backend can override it to skip clean work
using its own complete dirty-state accounting. Explicit sync is unaffected.
`ReadOnly` rejects mutations before dispatch; `Volatile` makes no media promise.

Periodic success includes the backend's device-reported completion, not physical
readback verification. The interval is a scheduling cadence, not a hard maximum
persistence latency. Failure never advances the deadline, and stopping or handing
off the service cannot leave a detached synchronization thread.

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
