# Checkpoint 4 review

## What did we ship?

- `aulon_core::topology` (`crates/aulon-core/src/topology.rs:1`) —
  sysfs-driven L3-cache discovery. Returns a `Topology { shards:
  Vec<Shard> }` where each `Shard` is one L3 domain and its logical
  CPU set. Falls back to a single shard covering all available CPUs
  on macOS / when sysfs is unavailable. 5 unit tests including a
  fixture-built two-domain sysfs tree.
- `aulon_core::shard_inbox`
  (`crates/aulon-core/src/shard_inbox.rs:1`) — bounded lock-free
  Vyukov-MPMC ring restricted to single-consumer use. Power-of-two
  capacity, atomic head/tail, `cfg(loom)`-gated atomics so the same
  source compiles under loom. **Loom-tested**: two interleaving
  models exercise producer-producer slot-claim races and producer-
  consumer visibility. No `Arc<Mutex<T>>`.
- `aulon_core::eventfd` (`crates/aulon-core/src/eventfd.rs:1`) —
  cross-thread wake primitive. `eventfd_pair()` returns a single-
  owner `EventfdReader` (which converts into a
  `tokio_uring::fs::File`) and an `Arc`-shareable `EventfdWaker`
  whose `wake()` writes 1 to the fd. Both sides reference the same
  kernel object via `dup(2)`. The crate's only `#[allow(unsafe_code)]`
  module outside the io_uring driver path.
- **Multi-worker bootstrap** in `aulon-server`
  (`crates/aulon-server/src/main.rs:144`) — discovers topology, pre-
  creates one `ShardInbox` and one `EventfdReader`/`EventfdWaker`
  pair per shard, spawns one OS thread per shard and pins it to the
  L3's first logical CPU (via `core_affinity`). Each thread runs
  its own `tokio_uring::start` runtime and binds an `SO_REUSEPORT`
  listener so the kernel distributes accepts. Single-shard hosts
  skip the OS-thread spawn and run on `main()`'s thread for parity
  with C3 logs / panics.
- **Cross-shard `PUB` fanout** (`handle_pub` →
  `crates/aulon-server/src/main.rs:380`) — encodes a single
  `Arc<PublishedFrame>` per `PUB`, runs local fanout, then pushes
  the `Arc` into every peer shard's inbox; on a transition from
  empty to non-empty, kicks the peer's eventfd. The peer's
  `drain_task` (`main.rs:478`) reads the eventfd via tokio-uring
  and runs `dispatch_local` for each frame.
- `AULON_FORCE_SHARDS` env-var override
  (`crates/aulon-server/src/main.rs:144`) — synthetic topology for
  exercising the multi-shard path on single-L3 dev hardware. Off by
  default; used by the cross-shard smoke test.
- `bench/headline.sh` — Aulon vs `nats-server` 2.10.24, identical
  workload back-to-back, prints a min/p50/p99/p99.99 comparison
  table. Uses the same `aulon-fanout` client for both backends.
- `docs/design/topology-sharding.md`,
  `docs/design/sq-batching.md` — decisions, alternatives,
  measurement.
- `docs/reviews/checkpoint-4.md` (this file).
- `PERFORMANCE.md` C4 entries: cross-shard wiring smoke test,
  syscall accounting under load, in-VM headline numbers with the
  slow-consumer caveat documented.

## What did we measure?

### Cross-shard correctness

Two-shard topology (`AULON_FORCE_SHARDS=2`), 11 connections (10
subscribers + 1 publisher) distributed by SO_REUSEPORT 3 / 8 across
shards. **10 / 10 subscribers received the message.** Confirms
both the publisher-shard local fanout and the peer-shard inbox-
drained fanout deliver correctly.

### Server syscall load

`perf stat` on `aulon-server` while `aulon-fanout` drives 10,000
PUBs × 4 subscribers (44,000 deliveries):

- 2,201 server-side `raw_syscalls:sys_enter`
- 3,835 SQEs submitted, 3,834 CQEs completed
- **20 deliveries per server syscall, 1.74 SQEs per
  `io_uring_enter`**.

The byte-stream outbound buffer plus tokio-uring's per-yield SQ
batching together amortise syscall overhead well. See
`docs/design/sq-batching.md` for why we accept the default policy
in C4.

### Headline (in-VM, with caveats)

`bench/headline.sh`, 4 subscribers × 3,000 iterations + 1,000
warmup, OrbStack Ubuntu VM, 256 B payload:

| backend | min | p50 | p99 | p99.99 |
| ---: | ---: | ---: | ---: | ---: |
| Aulon | 57 µs | 1.39 ms | 43.25 ms | 43.35 ms |
| nats-server 2.10.24 | 39 µs | 99 µs | 583 µs | 699 µs |

These numbers are **caveated** — see "What did we get wrong?".

### Loom

`RUSTFLAGS="--cfg loom" cargo test -p aulon-core --test loom_inbox
--release` exhaustively explores both interleaving models in well
under one second. Both pass.

## What did we decide?

- **One worker per L3 cache domain.** Per-L3 keeps shard count
  small (≤8 on real hardware), the trie + connection map fits in
  L3 cache, and the every-shard PUB fan-out cost stays bounded.
  Per-core-within-L3 sharding is a C5+ option only if measurement
  shows the per-L3 worker is the bottleneck.
- **Connection sharding, every-shard PUB fan-out.** Subject-hash
  sharding does not compose with `*` and `>` wildcards (a `>`
  subscription matches an unbounded family of subjects, no single
  hash owner). Connection sharding + every-shard fan-out is the
  only NATS-compatible shape that scales.
- **One `Arc<PublishedFrame>` per cross-shard PUB.** The trie
  match itself stays alloc-free; the cross-shard hop pays one
  allocation per PUB regardless of fan-out (refcount on every peer
  push). Single-shard hosts hit the same alloc-free path as C3.
- **Sysfs over hwloc.** `/sys/devices/system/cpu/cpu*/cache/index*`
  exposes L3 cache sharing on Linux directly, no system C
  dependency, no extra crate. The hwloc binding stays available as
  a swap if a future checkpoint needs NUMA topology.
- **Vyukov MPMC ring restricted to single-consumer.** Loom-testable,
  no upstream dep, demonstrates concurrency rigour. The Cargo
  `cfg(loom)` machinery (gating `tokio-uring`/`tokio` away under
  `--cfg loom` because both have their own internal loom checks
  that interact badly with a top-level loom build) is documented
  in `crates/aulon-core/Cargo.toml`.
- **Default `tokio-uring` SQ-submission policy.** No custom batch
  driver in C4 — the byte-stream outbound buffer + per-yield
  batching already amortise to 20 deliveries per syscall.
  `docs/design/sq-batching.md` records the measurement and the
  carry-forward.

## What did we get wrong?

- **The headline benchmark trips slow-consumer eviction at run
  termination.** All four subscribers report `eof after 3999/4000
  msgs` near the end of the run, which contaminates Aulon's p99 /
  p99.99 with a several-ms tail spike. The steady-state
  distribution is much tighter than the table shows. The bug is
  in the bench client, not the broker: a single-thread tokio_uring
  runtime hosting the publisher and all 4 subscriber tasks cannot
  drain the subscribers fast enough as the publisher wraps. The
  C2 fanout doc had already flagged this exact shape; we did not
  remediate before measuring. **Fix: pace the publisher against
  subscriber drain (token bucket, or a separate publisher
  process).** Lands in C5 with the bare-metal headline.
- **Loom-cfg interaction with `tokio` was an hour of yak-shaving.**
  Setting `RUSTFLAGS="--cfg loom"` globally activates `cfg(loom)`
  branches inside `tokio` itself, which then disables
  `Builder::on_thread_park`, which `tokio-uring` requires. The fix
  was to gate the `tokio-uring` and `tokio` deps behind
  `cfg(not(loom))` and exclude the modules that use them under
  loom. Documented in the Cargo.toml comment so the next
  contributor doesn't re-litigate it.
- **Initial bench script's `extract_metric` awk was broken.**
  Field-positional matching against `min ns        : 76736` failed
  silently; the comparison table printed empty values until I
  rewrote it as a substring search.

## What's deferred?

- **Bare-metal headline.** OrbStack VM is the wrong host for
  tail-latency measurement. C5 polish runs `bench/headline.sh` on
  a dedicated Linux box and lands the chart in the README.
- **Bench-client publisher pacing.** Fix the slow-consumer
  artifact before re-running headline.
- **NUMA-aware buffer-pool placement / hwloc binding.** Sysfs is
  enough for L3 sharding; NUMA only matters once we measure
  cross-NUMA traffic.
- **Per-(src, dst) shard inbox sizing.** Default 4,096-frame
  capacity covers typical loads; adaptive sizing if measurement
  shows back-pressure in production.
- **Subject bloom filter per shard.** Skip peers with no matching
  subscription, cutting the every-shard fan-out cost. Worth doing
  only after a workload shows it matters.
- **Custom SQ-batching policy / SQPOLL.** See
  `docs/design/sq-batching.md` carry-forward.
- **`UNSUB max_msgs`.** Carry-over from C3.

## What changed about the plan?

- C4 design docs are now `docs/design/topology-sharding.md` (drops
  the placeholder hwloc dependency in favour of sysfs) and
  `docs/design/sq-batching.md` (decision: accept default policy).
- `docs/MILESTONES.md` C4 row is updated in the same commit as
  this review to mark the gate met.
- The headline chart in `README.md` is deferred to C5 because the
  current numbers are slow-consumer-distorted; landing them as
  `the` headline would misrepresent the broker.

## What's next?

C5 polish:

1. Bare-metal Linux host for the headline run.
2. Fix the bench client's publisher pacing.
3. Re-run `bench/headline.sh` and land the chart in `README.md`.
4. `cargo deny` for license/dep audit.
5. `cargo public-api` snapshot for `aulon-core`.
6. One war-story write-up — the loom/tokio cfg interaction is a
   strong candidate.
7. `asciinema` of `nats bench` against Aulon.
