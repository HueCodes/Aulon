# Runtime

## Decision

Use **`tokio-uring`** as the per-core async runtime. One `tokio_uring::start`
context per worker thread, each driving its own `io_uring` instance. Tasks
are `!Send`; nothing migrates across cores. Fixed buffers are registered
with the kernel via `IORING_REGISTER_BUFFERS` and used through
`read_fixed` / `write_fixed_all`.

This decision was reached after a failed attempt with **Monoio**; that
history is preserved at the bottom of this file.

## Rationale

The brief is thread-per-core, shared-nothing, `io_uring` with registered
buffers. `tokio-uring` is the only well-maintained Rust runtime that exposes
`IORING_REGISTER_BUFFERS`, `FixedBufPool`, and the corresponding fixed-op
methods in its **public** API. Picking it lets us spend the budget on the
parts that earn the project's claim — buffer pool policy, routing,
topology-aware sharding, SQ batching — instead of on driver writing or
upstream PRs.

## Alternatives considered

- **Raw `io-uring` crate.** Maximum control. Cost is writing our own task
  scheduler, completion-driven future executor, timer wheel, and SQE
  batching policy. None of that is the point of Aulon. Rejected on
  cost-per-signal: ~3 extra weeks for ~5% extra credibility.
- **Glommio.** Also TPC, also `io_uring`. Development cadence has slowed.
  Rejected on maintenance signal.
- **Tokio (multi-threaded, not `tokio-uring`).** Multi-threaded with
  work-stealing and `Send`-bound futures. The opposite of TPC. Choosing
  Tokio would invalidate every other design decision in this project.
- **Monoio.** Initially picked; later rejected — see history at the bottom.

## What `tokio-uring` actually does

Each `tokio_uring::start` invocation owns:

- An `io_uring` SQ and CQ on a dedicated ring file descriptor.
- A registered-buffer table populated via `FixedBufPool::register` /
  `FixedBufRegistry::register`, addressed by `buf_index`.
- A `tokio::runtime::Builder::new_current_thread` runtime, on which
  `LocalSet`-style `!Send` tasks run.
- A driver task that drains the CQ between scheduler ticks and wakes the
  futures that owned the in-flight ops.

Per loop tick: drain ready completions, wake matching futures, run the
scheduler queue, push pending operations onto SQ, submit. The driver is
cooperative; a task that doesn't yield blocks its core.

## Constraints we adopt

- Broker tasks are `!Send`. Any state shared across cores crosses an
  explicit message-passing boundary, not a `Send` future.
- One ring per core. Buffer registration is per-ring, so `BufferPool` (a
  thin wrapper over `FixedBufPool<Vec<u8>>`) is per-core by construction.
- No global runtime handle. Each thread bootstraps its own
  `tokio_uring::start` after pinning to a core.
- No `tokio::spawn` (multi-threaded) on the data path. Within a
  `tokio_uring::start` context, `tokio_uring::spawn` schedules `!Send`
  tasks on the same thread, which is what we want.

## Things we will want to change in `tokio-uring`

These shape downstream design but do not block C1:

- **Explicit batched submission.** `tokio-uring`'s default policy submits
  per-op or on yield boundaries. SQ batching (C4) needs a way to group N
  ops into one submit syscall; either a feature flag, a thin wrapper, or
  upstream PRs.
- **Multiple registered-buffer pools per ring.** `FixedBufRegistry` and
  `FixedBufPool` cooperate, but we may want priority classes (control
  plane vs. data plane). v1 has one pool per ring.

## Measurement plan

For C1 echo, we want to know:

- Syscalls per echo round-trip. Target: minimum (everything routed through
  registered buffers and the ring). Measured with `perf stat -e
  raw_syscalls:sys_enter` over a long run; numbers land in `PERFORMANCE.md`
  on the bare-metal C4 bench.
- Time spent in driver vs. in user code. Via `perf record` on the bench
  client and server.
- p50 / p99 / p99.99 RTT for 256 B payloads, single core, single
  connection. First number recorded; bare-metal headline lives in C4.

---

## History — why Monoio was abandoned

Aulon's first implementation used Monoio 0.2.4. After completing the
buffer pool, typestate connection, and bench client, attempting to wire up
`IORING_REGISTER_BUFFERS` revealed that the published Monoio crate has **no
public API for fixed buffers**: a grep across `monoio-0.2.4/src/` for
`register_buffers`, `read_fixed`, `write_fixed`, `ReadFixed`, or
`WriteFixed` returned zero matches. The C1 gate explicitly required these.

Three resolutions were considered (see `docs/reviews/checkpoint-1.md` for
the full discussion):

1. Drop registered buffers from the C1 gate.
2. Migrate the runtime.
3. Contribute fixed-buffer support upstream to Monoio.

**Option 2 was chosen.** `tokio-uring` 0.5 exposes the entire surface in a
public, stable form, and the migration cost was a single afternoon —
trait shapes are nearly identical (`IoBuf` / `IoBufMut`, owned-buffer
read/write, `BoundedBuf` for slicing). The buffer pool, typestate
connection, and bench client all ported over with mostly mechanical
changes.

The lesson, captured here so we do not repeat it: **verify the public API
of a load-bearing dependency by grepping its source, not by reading its
README.** Monoio's README implies io_uring fixed-buffer support; the
crate's published API does not deliver it.

Option 3 (an upstream PR to Monoio adding registered-buffer support) is
still attractive as a separate OSS contribution, but it is not on Aulon's
critical path.
