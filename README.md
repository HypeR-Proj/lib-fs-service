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
