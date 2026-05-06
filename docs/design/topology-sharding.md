## Topology-aware sharding (C4)

## Decision

Aulon runs **one worker per L3 cache domain**. Each worker:

- pins its OS thread to one of the L3's logical CPUs,
- binds its own `SO_REUSEPORT` listener on the broker port,
- owns a `Worker` struct identical in shape to the C3 single-worker
  one (subscription trie, connection map, buffer pool, emit scratch),
- **does not** share any of that state with peer workers.

Cross-shard communication is one bounded **lock-free MPSC inbox per
shard**, drained on a **registered `eventfd` read** that the shard's
`io_uring` instance owns. A publish on shard A encodes the
`PublishedFrame` once into an `Arc<PublishedFrame>`, pushes a clone
of the `Arc` onto every peer shard's inbox, and writes 1 to each
peer's eventfd. Peers drain their inbox, run their local trie, and
deliver to their local connections.

No `Arc<Mutex<T>>`, no `Arc<RwLock<T>>`, no global state.

## Why per-L3, not per-core

Per-L3 keeps shard count small (≤8 on real hardware, 1 on most dev
laptops, 2 on a typical workstation). Small shard count is the win:

- Per-PUB cross-shard fan-out is O(shards). Each hop is an inbox
  push + a 1-byte eventfd write. At ≤8 shards this is a handful of
  uncached writes; at 64 shards it would dominate the publish path.
- The trie + connection map of a single L3 fits comfortably in L3
  cache. Cores within the L3 share that cache for free at the
  hardware level — we don't need to fan further.
- Fewer shards means fewer SO_REUSEPORT buckets, simpler bench
  topology, and one trie to inspect per L3.

The thread itself is single-threaded (TPC); we do not spawn one
thread per core within an L3. If a workload pegs the per-L3 worker
on a single core, that's the C5+ signal to add intra-L3 work
distribution — but the shape we ship in C4 is the right default for
a NATS-core broker.

## Why every-shard fan-out (and not subject-hash sharding)

Two shapes were considered:

1. **Subject-hash sharding.** Hash the subject; one shard owns each
   subject. PUB on the wrong shard forwards to the owner. SUB likewise
   forwards to the owner. One-shard-per-subject means publish doesn't
   visit peers; cross-shard work happens on PUB only when the
   publisher is on the wrong shard, and on every SUB always.
2. **Connection sharding + every-shard fan-out.** The shape we ship.
   Connection lives on its accept-time shard for life. Subscriptions
   follow the connection. PUB visits every shard's trie; each shard
   delivers locally.

Subject-hash sharding **does not compose with `>` and `*` wildcards**.
A subscription on `events.>` matches an unbounded family of subjects;
it has no single owner under any hash. Subject sharding would force
every wildcard subscription to be replicated across every shard, at
which point you've reinvented every-shard fan-out for a subset of
the subscription table — strictly worse than fanning out for all
subjects and keeping the trie unsharded per node.

NATS-core's wildcard model is the load-bearing reason connection
sharding is the only scalable shape here.

## Cross-shard primitive

```rust
/// One frame in flight from the publishing shard to a peer.
/// Shared (via `Arc`) so we encode the wire bytes exactly once.
pub struct PublishedFrame {
    pub subject:  Box<[u8]>,
    pub reply_to: Option<Box<[u8]>>,
    pub payload:  Box<[u8]>,
}

/// Bounded lock-free MPSC ring. Capacity is fixed at construction;
/// power-of-two so head/tail wrap with `&` instead of `%`.
pub struct ShardInbox {
    cap:   usize,                          // power of two
    mask:  usize,                          // cap - 1
    cells: Box<[Cell<Slot>]>,              // cap slots
    head:  AtomicUsize,                    // consumer-only writer
    tail:  AtomicUsize,                    // producers CAS-bump
    eventfd: RawFd,                        // owned, registered with the consumer ring
}

struct Slot {
    seq: AtomicUsize,                      // Vyukov-MPMC sequence number
    val: UnsafeCell<MaybeUninit<Arc<PublishedFrame>>>,
}
```

The inbox is a Vyukov bounded MPMC ring restricted to single-consumer
use. Each cell carries a sequence number that producers and the
consumer use to order writes and reads without locks. (The pattern
is well-known; loom test verifies the implementation.)

**Push** (any shard's worker thread):

```text
1. snapshot tail
2. read cells[tail & mask].seq
3. if seq == tail: CAS tail to tail+1; if won, write Arc<PublishedFrame>,
   set seq = tail+1; return Ok
4. if seq <  tail: full; return Err(WouldBlock)  // back-pressure
5. else: lost the CAS; retry from 1
```

**Pop** (only the owning shard's worker):

```text
1. read head
2. read cells[head & mask].seq
3. if seq == head+1: take Arc, set seq = head+cap, advance head; return Some
4. if seq <  head+1: empty; return None
```

After a successful push, **if the inbox went non-empty, write 1 to
the eventfd**. Multiple producers may all see "non-empty after my
push" — the eventfd write is idempotent (the kernel coalesces
counters). One spurious wake is harmless.

The consumer registers a `read(eventfd, &mut [u8; 8])` op via
`tokio_uring`. On completion (any non-zero u64 value), drain the
inbox until empty, deliver each frame through the local trie, then
re-arm the read.

## Allocation discipline

Each PUB allocates **once** for the cross-shard hop:

- One `Arc<PublishedFrame>` (one heap allocation; clones are
  refcount bumps).
- The frame's `Box<[u8]>` for subject / payload / reply_to are
  produced by `Box::from(&[u8])` once at construction.

This is slightly worse than C3's local-only path (which allocated
only for queue-group bucketing). The cost is bounded — one
allocation per PUB irrespective of fan-out — and it is only paid
when the broker has more than one L3 domain. Single-L3 hosts (most
laptops, including the OrbStack VM) hit the same fast path as C3:
the publisher's shard does its own local fan-out and there are no
peers to push to.

The frame is dropped (and refcount goes to zero) after the last
shard delivers it.

## Connection placement

```text
fn worker_main(shard_id, cpu_id):
    pin_thread_to(cpu_id)
    tokio_uring::start(async {
        let listener = TcpListener::with_so_reuseport(LISTEN_ADDR)?
        loop {
            let (stream, peer) = listener.accept().await?
            spawn reader/writer tasks bound to this shard's Worker
        }
    })
```

Kernel `SO_REUSEPORT` distributes accepts across the per-shard
sockets. The distribution is hash-based, deterministic per 4-tuple
on Linux ≥ 4.5; we accept the kernel's verdict and do not try to
re-balance.

A connection's `Rc<ConnectionState>` is `!Send` and lives on its
shard's thread for the connection lifetime. Cross-shard publishes
land in the inbox carrying the **target shard's** view of the
delivery, so the peer's writer task does the `enqueue` and no
`!Send` data crosses a thread boundary.

## Worker side: combined wake source

A C3 worker only had to wake its writer tasks (`Notify`) and its
listener loop. A C4 worker also has the eventfd. The reader-task
loop on each connection owns its own `read_fixed`; the eventfd-read
is **owned by the worker's main task** (the same task that runs
`accept`). On wake, it dispatches into the trie and into each
matching connection's outbound buffer.

This keeps cross-shard delivery off the per-connection critical
path: the reader task continues parsing inbound frames; the worker
task handles cross-shard work in parallel under the same single-
threaded runtime.

## Topology discovery

We read **sysfs** on Linux:
`/sys/devices/system/cpu/cpu<N>/cache/index<M>/level == 3`
gives the L3 cache for `cpuN`, and `shared_cpu_list` lists every
logical CPU sharing it. Group by the canonical (smallest-id) CPU in
each shared set and you have the L3 domains.

```rust
pub struct Topology {
    pub shards: Vec<Shard>,        // one per L3 domain
}
pub struct Shard {
    pub shard_id: u32,
    pub cpus:     Vec<u32>,        // logical CPU ids in this L3
}
```

If sysfs is unavailable (macOS dev) or returns no L3 information,
fall back to **a single shard whose `cpus` is the full system set**.
C4's behavior on macOS dev is identical to C3 single-worker; the
multi-shard codepath is exercised on Linux bench hosts.

The `hwloc` library would give us identical information plus PCIe /
NUMA topology. Sysfs is sufficient for L3-aware sharding and avoids
a system C dependency. If a future checkpoint needs NUMA-aware
buffer-pool placement, swap the backend; the `Topology` struct is
the stable boundary.

## Loom invariants

The cross-shard inbox is the one new sync primitive. Loom tests
exercise:

- Two producers and one consumer; assert every pushed `Arc` is
  popped exactly once and the inbox is empty after drain.
- Producer push that observes "now non-empty" reliably writes the
  eventfd before any subsequent pop returns `None`. (A real
  eventfd is replaced by an atomic counter under loom.)
- Bounded back-pressure: when the inbox is full, push returns
  `Err(WouldBlock)` and does not corrupt the ring.

## What if the inbox is full?

A full inbox means a peer shard's consumer has fallen behind the
sum of all producers. We treat this as the cross-shard analogue of
slow-consumer: the publishing shard returns `-ERR shard back-
pressure` to the publisher, and increments a counter. We do **not**
spin-retry, do **not** sleep, do **not** drop silently. The default
inbox capacity is 4096 frames, which at the headline-bench rate is
several milliseconds of buffering — plenty of head-room for normal
operation.

## Carry-forward to C5

- Adaptive inbox sizing per (src, dst) pair.
- A per-shard subject bloom filter so the publisher can skip peers
  that have no matching subscription. Cuts the every-shard fan-out
  cost when subscriptions are sharded by topic in practice.
- Smarter wake batching (multiple inbox pushes per eventfd write
  via a "kick needed" flag).

## Measurement

`bench/headline.sh` is the C4 deliverable. Topology is the
independent variable; the dependent variables are:

- p50 / p99 / p99.99 publish-to-deliver latency.
- syscalls per published message (via `perf stat -e
  raw_syscalls:sys_enter`). C4 should not regress C3's count by
  more than 1 syscall on the cross-shard path.
- Aulon vs. `nats-server` on the same hardware, identical workload.

Numbers land in `PERFORMANCE.md` and a chart lands in `README.md`.
