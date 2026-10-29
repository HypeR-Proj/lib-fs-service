<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# Native filesystem services

The kernel owns namespace traversal, capability checks, file identities,
`flock`, content revisions and the clean page cache. Media parsers run in Native
processes. The block-backed implementations are FAT32 and read-only core EROFS;
the protocol and worker lifecycle are independent of either format. Ramfs remains
in the kernel.

## Startup and authority

Init creates an attachment channel between io-runtime and the resident
fs-backend manager. io-runtime enumerates physical disks through the I/O VM's
owner-only block-management channel, validates GPT and resolves the partition
selectors in `etc/hyper/storage.json`. It binds each selected range and access
policy to an independent I/O client, then transfers exclusive `NativeBlock`
capabilities, partition UUIDs and labels, sector counts and read-only policies
to fs-backend. BEGIN transfers the readiness writer; FINISH starts only the bootstrap
`etc/fstab` mounts. No raw writable alias remains in io-runtime after a volume moves
to the filesystem manager.

The Linux I/O VM validates HypeR's selected ranges and creates their `dm-linear`
exports; it does not parse GPT or choose partitions. A Native block capability
addresses its authorized partition starting at sector zero. The filesystem
manager probes a bounded prefix for FAT geometry or EROFS
magic, then loads the corresponding known worker executable. Disk bytes select
a format, never an executable path; FAT's informational type label is not an
authoritative format test.

At startup the manager reads only the RAM filesystem's `etc/fstab` and optional
`etc/hyper/filesystem.json` through its fixed `process.environment-directory`.
They appear under `/boot` after the root switch. System init adopts the running
I/O provider, filesystem manager and format workers into independent system
processes, then requests the automatic mounts in `/etc/fstab` through the
same manager control connector. Mount
requests select their table explicitly in the live namespace, as do mount targets.
Both tables select authorized partitions by `PARTLABEL=` or `PARTUUID=`,
mountpoints, access modes and order within that environment.
The JSON selects `worker_directories` (default `[{"environment":"svc"}]`),
bounded to 16 KiB and eight normalized paths. `environment` entries are relative
to the manager's fixed environment directory: the bootstrap archive before
adoption, the independent system root in the formal manager. Bootstrap environment workers use the
static loader; formal workers use the dynamic loader. `namespace` entries are
absolute paths in the live namespace and use the system loader and libraries. Only a missing executable permits trying the next directory;
access or I/O failures stop the search. Missing workers fail at runtime, without
making the format registry depend on which implementations were packaged.
See [boot and system deployment](https://hyper-proj.github.io/bootstrap/docs/boot-layout.html) for the default boot/system layout,
fstab syntax and the independent system-root handoff.

The manager gives each worker its exclusive volume, bounded resource domain,
parent-lifetime channel and transport rendezvous. It retains the child process
and task group. Workers receive no task factory, device authority or mount-root
capability. A worker validates its root and offers a byte-channel endpoint,
shared metadata VMO and, for a block-backed provider, a block queue. The manager
binds that queue and its read-only source window when publishing the configured
mountpoint. Providers without a block device use the buffered transport instead.
The formal `hyper-fs-service` library owns this handshake and ordered RPC.
Bootstrap workers include their manager-owned transport module instead of linking
a general service library. Format adapters implement media operations. The manager is outside the file
read/write path, and io-runtime continues forwarding console and power events
while waiting for the initial mount result.

Bootstrap and formal format workers have independent source owners and
executables. Bootstrap workers live in the boot archive; replacement providers
live in `/svc`. Worker placement remains independent of format detection
and mount phase: system init requests the default FAT data mount from its own
fstab after adoption.
`fs-fat` implements writable FAT32. `fs-erofs` implements read-only,
uncompressed core EROFS, including compact/extended inodes, plain and inline
data, directories and symlinks. It validates a present superblock CRC32C and
rejects compression, xattrs, external devices and unsupported inode layouts.
Its [reader documentation](https://github.com/HypeR-Proj/fs-erofs/blob/main/README.md) gives bounds and
supported block sizes. Recognition alone is not a promise that a worker can
read every extension of that filesystem format.

## Online provider adoption

The private bootstrap handoff channel is distinct from mount administration.
System init obtains a versioned inventory of attached volumes and mounted
formats, then starts the formal fs-backend with that inventory. The replacement
manager prepares every mounted format worker before asking bootstrap to yield.
Each replacement explicitly locks its current mappings before reporting
readiness, while its predecessor can still satisfy file faults. This requires a
delegated `LOCK_MEMORY` right and charges the worker's pinned-page quota. The
loader supplies no implicit residency policy. Later pageable mappings need a new
explicit lock before being used on a path that serves their own filesystem.

Each original worker completes its current request, flushes media state and
stops consuming the request channel. It moves its exclusive `NativeBlock`, the
same byte-channel server endpoint, shared metadata VMO and logical block queue,
and the last completed request sequence to the replacement. The replacement
exports the same queue-owned read-only source window; it does not substitute
a new queue for the live mount. The manager's client endpoint and shared-buffer
owner also move; no unmount, remount, namespace replacement or block reactivation
occurs. Requests queued during the brief transfer remain queued on that endpoint.
Kernel file identities, open handles, content protection and cached pages remain
attached to the existing mount. FAT directory cookies encode media positions and
path identity, and EROFS cookies encode inode positions and path identity; both
remain valid when a drained parser reopens the same unchanged medium.

Before preparation succeeds, failure retains the old active providers. Once live
ownership starts moving, a failure belongs to system recovery; it cannot be
reported as a successful rollback. After adoption, old workers and the old manager
park without deciding when to retire. The formal manager gives their Process
supervision capabilities to system init, which chooses and executes retirement.
A lost final notification does not tear down an adopted mount: the manager keeps
serving and retains any predecessor supervisors it could not deliver. System init
treats an unconfirmed outcome as recovery state and does not retire bootstrap.
See [the bootstrap handoff contract](https://hyper-proj.github.io/lib-bootstrap-handoff/docs/handoff.html).

`mount` and `umount` send administrative requests to fs-backend using a scoped
control capability. Init delegates that capability to session and the shell;
the formal shell attenuates it to WAIT|WRITE only for `/bin/mount` and
`/bin/umount`. Independent bootstrap recovery commands can operate the
old manager before adoption. Ordinary commands receive neither
mount control nor raw volume handles. A copied tool at a different path does
not gain administration authority.

Graceful unmount refuses active VFS Locations, including open files,
directory/cwd capabilities, active lookups, or a covered location held by a child
mount. Protected file mappings retain their backing node and also keep the
mount busy until the mappings retire. The kernel freezes new admission, synchronizes under the backend mutex, atomically removes
the mount edge, and invalidates health before allowing late speculative reads.
Failure keeps the mount edge and thaws admission. On success, the manager tells
the worker to close its transport/mapping and return its original block owner.
Read-only mounts also reject mutations at the kernel transport gate, independently
of worker behavior. No forced/lazy unmount or raw-block reactivation bypass is
provided. A missing worker leaves the block in the manager so the mount can be
retried after making the worker available. A running worker failure loses that
reusable ownership; reopening requires a new backend connection rather than
assuming interrupted I/O has retired.

The formal manager observes worker Process termination independently of its
serialized mount-control requests. An unexpected exit records the worker name,
mountpoint, process KOID and kernel terminal reason (including exit status or
fault class/code). The record goes to the manager's stderr journal stream, and
`componentctl status /fs-backend` retains a failure summary. `mount` shows each
worker's state and terminal reason. The manager stays running for healthy volumes;
`mount -a` cannot treat a terminated worker as a successful existing mount.
An explicit clean unmount is distinguished from an unexpected exit. Failed
mounts remain fenced rather than exposing the directory underneath or silently
reusing their block connection.

Directory enumeration and child metadata are separate operations. A failed
child mount does not invalidate its parent's directory stream. `ls /` preserves
healthy entries when `/data` metadata fails, displays unknown attributes for that
entry, reports its path on stderr, and returns a nonzero status for the partial
listing. Accessing the failed volume itself still returns an error.

A configured VM can keep `/data` busy even while stopped: vm-manager retains
the registered guest image's file capability. `vmm delete alpine` releases a
stopped Alpine definition and its image handle; it does not delete the image or
configuration files. Stopping the VM alone does not release that registration.

The recovery shell can repair mount state after initial readiness fails. If
`/system` remains available, init can still hand off supervision despite a
data-volume failure. When `/system` is unavailable and init retains stage-1
supervision, repairing mounts does not retry that handoff or start the system
graph; another complete startup attempt requires the next boot.

After adopting core supervision, system init performs a separate namespace
root switch. Existing remote mounts keep their identities, worker transports,
open files and cached content while their mount edges move to the new root.
Ramfs-backed `/system` is bound without copying its files. These generic VFS
operations require `MANAGE_NAMESPACE`, retained by the active init; the shell's
filesystem-manager control channel does not grant it. If the root switch fails
after supervision transfer, system init keeps supervising the core services
on the old root and does not launch the system graph.

## Request and shared-memory contract

`hyper-filesystem` defines explicit little-endian messages for stat, batched
directory enumeration, positioned reads and writes, create, remove, rename,
resize, sync, timestamps, reading symbolic-link targets and physical read plans.
It contains no FAT attributes
or on-disk structures. Paths are bounded UTF-8 relative paths without dot, parent
or empty components.
Directory enumeration returns ordinary children; format adapters exclude
on-disk `.` and `..` entries. A batch contains complete attributes and a private
continuation after each child; FAT resumes its cluster/byte position and EROFS
resumes its inode/block position without scanning the preceding children. The
protocol preserves symlinks as namespace objects and returns their stored target
bytes. The common VFS performs bounded resolution, including absolute and parent
components. Canonical paths remain the content identity contract; distinct
on-disk hard-link paths do not yet share a kernel content identity.

Each mount owns one ordered `ByteChannel` and a resident 512 KiB writable
metadata VMO. A block-backed mount additionally binds one logical block queue
and its separate 512 KiB kernel-owned source window.
The worker has only read-only mapping authority to the source window. The kernel
serializes requests under a sleepable mount mutex. Requests travel in the channel;
directory batches, read plans and buffered fallback reads use the writable VMO.
Block-backed write payload uses the read-only window. A provider without a
block queue uses this same buffered VMO for file payload; its write token is
zero. Token zero never authorizes a block-queue source-window submission.
Directory replies carry at most 4 KiB, with an exact starting checkpoint,
record count, EOF indication, and validated variable-length names and attributes.
A non-EOF reply must make progress; a batch cannot repeat a checkpoint or return
to its starting checkpoint. The kernel copies and validates the complete
batch before retaining it; it never caches a live shared-memory view.

For block-backed writes, the kernel finishes copying application input into
the source window before publishing a
write request with an operation token. The worker submits only ranges belonging
to that active token; kernel leases retain the source until completion. It must
join all accepted writes before replying. Reply invalidates the token, and any
remaining lease retires the mount rather than permitting source reuse. No
writable source handle is exported, and a successful mount's source backing is
never rebound to another mount. The kernel never treats shared bytes as borrowed
Rust objects.
The response must match the operation and increasing request identifier, fit the
exact frame size, and contain valid lengths, names, kinds and timestamps.
A valid media name longer than the Native 255-byte component limit fails that
enumeration with `Unsupported`; it neither truncates the name nor retires the
mount. The filesystem wire format permits up to 1,020 UTF-8 bytes for FAT names.

`ReadExtents` returns a bounded prefix of a file read: at most 64 physical
byte-offset/length pairs describing at most 512 KiB. The reply is at most 1,040
bytes, including its total-length and count header. Zero-length plans mean EOF;
nonempty extents must make progress. Byte offsets support EROFS inline tails.
The kernel copies the entire plan out of shared memory, validates its lengths,
arithmetic and mount-volume bounds, then performs the reads using the bound
queue's volume authority while holding the filesystem transaction lock. The
worker never receives writable cache-page mappings. Mounts without a block
queue skip physical extent requests. Unsupported raw layouts can also fall back
to positioned buffered reads; compressed or transformed formats are not implied
by extent support.

A request timeout, channel closure or malformed reply permanently
retires the mount generation. Its shared range is never reused for another
request. A late server write therefore cannot corrupt a subsequent transaction;
the server's mapping remains an owner of the physical storage until teardown.
Stopping one client process drains its already published transaction within the
same deadline before unwinding its syscall; it does not invalidate the shared
mount or abandon the shared buffer. I/O errors can describe partial media changes
and are not retried as mutations.
The raw block transfer syscall checks per-operation rights, sector alignment,
range bounds and the 512 KiB payload maximum independently of worker input.
`native_block_transfer` operations are 0 (read), 1 (write), 2 (durable flush)
and 3 (disjoint write batch). For a batch, the sector argument is the record
count (1–4); the byte frame contains that many little-endian `(first_sector,
byte_length)` u64 pairs followed by their payloads in order. Total payload is
at most 512 KiB. Every complete-sector range, overlap and exact frame length is
validated before any write. These synchronous operations use the same device
engine as derived asynchronous queues, so their overlaps and durable flushes
cannot bypass ordering against queued work.

The FAT adapter derives a bounded `BlockQueue` from its volume capability. It
submits up to sixteen disjoint ranges per batch and can continue preparing its
current filesystem operation before waiting for each completion. The engine
merges adjacent compatible ranges, consumes independently completed commands
and refills available transport slots. The adapter collects all accepted writes
before replying to a filesystem request and drains its queue before handoff.
See [asynchronous block I/O](https://hyper-proj.github.io/lib-block/docs/block-io.html) for admission, DMA lifetime, ordering
and failure semantics; this does not add asynchronous dirty file-cache writeback.

The worker waits for both requests and parent-channel closure. Parent death
closes the worker service after any already executing bounded block operation
returns; worker exit closes the service as its handles retire. The Process
`TERMINATED` signal reports stopped execution and can precede this handle cleanup;
the `Retired` phase reports completed resource teardown.
The kernel checks local endpoint health even before satisfying a cache hit, so
cached bytes cannot hide a closed service. Restarting a process does not reconnect
an old mount: recovery needs a fresh mount generation. A hot cache hit needs no
server round trip. File-data hits do not take the mount transaction mutex;
name, attribute and directory hits use that mutex to serialize with mutations.

## Identity, cache and mutation ordering

The worker returns the canonical stored leaf name for every lookup. The kernel
registry maps canonical paths to an in-memory, never-reused node incarnation.
Aliases therefore converge on one content record and advisory-lock identity.
The authoritative worker controls media layout and mutation ordering. The
kernel executes its validated read plans but does not parse or independently
modify the filesystem. Out-of-band writers violate this contract. Live node leases prevent unlink. Rename updates
registered descendant paths after a successful backend mutation. Unlink removes
the path binding, so recreating a name cannot see pages belonging to the old file.

On cold bulk reads the kernel reserves private fill pages and copies directly
from the I/O VM exchange pool into them. Pages enter the clean cache only after
successful completion and revision validation; failed or stale fills are
released rather than published. Warm reads copy cache bytes once to the caller.
The exchange pool remains the sole device-visible data storage; neither the
worker nor the I/O VM obtains writable access to cached pages. See
[block I/O data paths](https://hyper-proj.github.io/lib-block/docs/block-io.html#filesystem-integration) for copy boundaries and
fallbacks.

Demand reads fill the complete file pages covering the requested range, bounded
by EOF and the scatter batch limit, and return only the requested bytes. This
also caches the surrounding bytes of small ELF headers and segment boundaries.
Short backend reads never publish an incomplete non-EOF page; admission pressure
falls back to exact-range reads without requiring cache retention.
Large file-read syscalls end their first batch at the next 512 KiB file boundary
and align subsequent batches. Page rounding therefore stays inside the cache
fill limit for unaligned callers too; applications need no private alignment
workaround to avoid falling back to scratch reads at every batch.

Cached pages pin the content record rather than an open capability. Closing the
last handle leaves its identity available while cached pages survive; reopening
from the same or another process can hit them. After all live and cached owners
disappear, a new open receives a fresh incarnation. Quota and pressure reclaim
release pages and expired registry records without making filesystem IPC calls.

Directory enumeration uses a separate `DirectoryStream` capability derived
from Directory READ authority. Each stream owns a fixed namespace location and
small trusted current/previous positions, not the directory's contents. Public
cookies are monotonic tokens bound to that stream; they never contain media
addresses. Only the latest input token may be replayed after failed copyout.
Separate readers are independent; duplicating a stream shares its progress.

The kernel retains directory batches and exact-name attributes in the same
physical-page cache as file contents. Reopening a directory in another process
can reuse those batches. Enumeration seeds the attribute cache so subsequent
`stat` or open of a canonical child need not rescan the backend directory. An
alias or a cache miss still consults the worker; a partial batch never proves
that an absent name does not exist. Hashes select cache entries, but full names
and checkpoints are compared before accepting them.

All these pages use the existing admission watermarks, background reclaim,
allocation-failure reclaim and domain filtering. Under pressure, cache index
storage shrinks and individually allocated identity indices retire with expired
bindings. No per-directory heap index or userspace directory snapshot grows
outside this accounting. An open stream does not prevent its cached pages from
being evicted: it resumes from its private checkpoint, and pressure causes
ordinary bounded backend reads without retaining their results.

Namespace changes invalidate the affected parent streams before the backend
operation. Rename also invalidates streams in a moved subtree because their
path bindings change. The caller restarts such a scan with a new stream; a
mutation in an unrelated directory does not interrupt it. Writes, truncation
and timestamp changes advance a separate volume metadata revision before the
worker call. That conservatively discards cached attributes and directory
batches, while leaving valid traversal positions usable. Both revisions are
checked under the volume mutex; a delayed reply cannot repopulate an obsolete
view. These rules also cover partially successful mutations and cached backend
failure checks. Ramfs retains its existing monotonic child-position semantics.

Executable and shared-library loads use ordinary File capabilities and explicit
`PROTECT` content protection. A protection token pins file identity and length,
not a whole-file snapshot or a resident copy. Selected mapped ranges can be
reloaded through the same page cache after clean mapping pages are reclaimed.
While a protected mapping still depends on the file, writes and truncation
through any alias fail with `Busy`. Mapping retirement releases its protection;
process exit also detaches file sources after threads quiesce and handles close,
even if an escaped VMAR keeps the dead address space allocated. Such retained
pages remain charged to the requester and cannot be refilled. File handle
transfer does not transfer an active protection grant.

Complete clean pages from cacheable files are shared physically across mappings
and independent opens. A mapping retains the canonical cache identity as well as
the page; the cache cannot discard that lookup entry while it is mapped. Private
writes and partially initialized ELF boundary pages use private storage. If cache
admission is paused under pressure, a new mapping may fall back to a private
copy; sharing is an optimization, not a prerequisite for reading the file.
Mapping population first probes resident complete pages without I/O or a scratch
copy, then fills remaining holes in bounded batches. ELF boundary pages stay
private with bytes outside the segment zeroed, even when the source file page is
fully cached.

The userspace loaders first prepare the required ELF header page explicitly,
so parsing cached headers does not start a competing sequential prediction.
They validate all load segments, then merge overlapping or
adjacent file-page ranges independently of virtual mapping permissions. Before
mapping they request a bounded `file_prefetch` fill, with a total preparation
budget of 512 KiB per image in addition to the header page. BSS, file holes and
non-loadable sections outside the rounded ranges are skipped. Larger images
continue through bounded demand reads rather than staging their entire contents
before mapping. Static bootstrap,
formal executable startup and shared-library loading use the same policy in
their independently owned loaders.

`file_prefetch` requires File READ and fills cache pages synchronously without a
userspace payload buffer. It does not enqueue sequential read-ahead or update its
prediction state. It returns the completed requested prefix, which can be zero
under admission pressure or for RAMFS. Backend errors propagate to the caller;
loaders treat preparation as optional and still validate every demand read and
mapping. No prefetch result guarantees residency or bypasses file protection,
execute authority, private writes, or ELF segment zero-fill rules.

`LOCK_MEMORY` is a separate VMAR right. Explicit range locking, or locking all
current mappings, populates the selected bytes before success and prevents clean
mapping reclamation until unlock, unmap or process exit. Current-only locking
skips unmapped guard ranges and does not affect future mappings. Repeated locking
is idempotent within one mapping; another process's lock remains independent.
Each process pays its residency quota even when the physical clean page is
shared. Private writes still use COW and this is not a physical DMA pin.

The filesystem and I/O services lock their executable/dependency mappings while
their predecessors can still satisfy reads, before advertising handoff readiness.
The loader itself does not impose residency policy. File protection stabilizes
content; residency keeps bytes available. Neither mechanism detaches a mapping
from the mount which owns the file.

Clean mapping reclamation still needs temporary transaction metadata and page
tables, charged to the requesting process's domain. At a hard quota without
headroom it can fail, so pressure handling is best effort rather than a promise
that every clean mapped page can always be evicted. The kernel does not borrow
filesystem or global sponsor quota to make the request succeed. Detaching file
sources during process exit does not depend on allocating replacement roots.

Writes, resizes and truncating opens advance the common content revision before
calling the worker. Even a failed mutation may have changed media, so old cache
entries become unreachable. The per-file gate also excludes concurrent fills.
Only clean pages are retained: ordinary writes stay synchronous, and explicit
sync is forwarded to the filesystem and durable block flush.

Sequential demand reads can enqueue bounded common readahead after a miss.
The queue contains weak filesystem, node, content and cache owners. Running
work upgrades only the content record, never an active node lease, so even an
in-flight prediction cannot keep a closed file busy. Its separate kernel worker
checks the identity and revision, tries the content and backend locks, and skips
work on contention or pressure. It resolves the incarnation's current path under
the backend mutex; unlink retires that binding, and later fills of the old cache
identity cannot become visible through a recreated file.
It cannot recursively predict more work. This worker is separate from memory
reclamation, so a backend waiting for memory cannot deadlock the reclaim path.

Implementation entry points: [formal manager](https://github.com/HypeR-Proj/fs-backend/blob/main/src/main.rs),
[bootstrap transfer](https://github.com/HypeR-Proj/bootstrap/blob/main/fs-backend/src/migration.rs),
[formal worker transport](https://github.com/HypeR-Proj/lib-fs-service/blob/main/src/lib.rs),
[FAT worker](https://github.com/HypeR-Proj/fs-fat/blob/main/src/main.rs), [EROFS worker](https://github.com/HypeR-Proj/fs-erofs/blob/main/src/main.rs),
[wire protocol](https://github.com/HypeR-Proj/abi/blob/main/filesystem/src/protocol/mod.rs),
[kernel adapter](https://github.com/HypeR-Proj/kernel/blob/main/src/fs/vfs/remote/mod.rs), and
[transport](https://github.com/HypeR-Proj/kernel/blob/main/src/fs/vfs/remote/transport.rs).
See [FAT semantics](https://hyper-proj.github.io/fs-fat/docs/fat.html) and [VFS cache](https://github.com/HypeR-Proj/kernel/blob/main/docs/design/vfs.md#file-data-cache)
for format details and memory policy.

## Validation

`make test-filesystem-failure` starts real Native filesystem workers with a
controlled test volume. It checks delayed replies without prematurely retiring
the mount, close/reopen cache reuse, worker death after
cache fill, manager-owner channel closure, worker death during a pending read,
stopping one requesting process while another keeps using the mount, and
rejecting overflowing writes derived from worker-controlled file lengths before
publishing any write request. It also checks cold directory RPC counts,
close/reopen enumeration and attribute reuse, independent readers racing
unrelated namespace changes, write-driven attribute invalidation, stale
continuation rejection, safe Rust iterator termination after a stale checkpoint,
and worker death with warm directory pages. These are
deterministic correctness and operation-count checks, not throughput thresholds.
Kernel tests also reclaim encoded directory and lookup pages while retaining a
live directory identity and scalar continuation. The fixture is selected by an
explicit Cargo feature and is never packaged in the normal system or development
profile.

CI runs this fixture with a 54 MHz counter, matching the Pi 5 frequency, so
counter ticks cannot accidentally pass as the absolute nanosecond deadlines
required by object waits. The delayed-reply case waits past five seconds of
monotonic time and withholds a reply to exercise the blocking path.

`make test-board-storage` exercises the actual FAT worker and Linux block
backend: file and directory operations, aliases and shared identities, writes,
truncation, cache invalidation, synchronization and readback after a cold QEMU
restart. These are correctness tests; they do not establish hardware performance
or qualify physical power-loss durability.

`make test-boot-system` also injects an exit with status 37 and a CPU exception
into a mounted test worker, without modifying the underlying FAT media. It
checks journal attribution, persistent component/mount status, clean unmounts,
rejection of a failed mount retry, and continued root listing, program loading
and `/tmp` I/O. A worker fault during shutdown must remain observable after
namespace detach and mount-owner release. This tests supervision and fault
isolation, not FAT correctness.
