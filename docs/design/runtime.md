# Runtime

## Decision

Use **Monoio** as the per-core async runtime. One Monoio runtime per worker thread, each driving its own io_uring instance. Tasks are `!Send`; nothing migrates across cores.

## Rationale

The brief is thread-per-core, shared-nothing, io_uring. Monoio is the only well-maintained Rust runtime built around exactly that constraint. Picking it skips ~3 weeks of driver-writing and leaves us free to spend the time on the parts that earn the project's claim — buffer pools, routing, topology-aware sharding, SQ batching.

## Alternatives considered

- **Raw `io-uring` crate.** Maximum control. Cost is writing our own task scheduler, completion-driven future executor, timer wheel, and SQE batching policy. None of that is the point of Aulon. Rejected on cost-per-signal: ~3 extra weeks for ~5% extra credibility.
- **Glommio.** Also TPC, also io_uring. Development cadence has slowed; Monoio is more actively maintained and has a simpler driver. Rejected on maintenance signal.
- **Tokio.** Multi-threaded with work-stealing and `Send`-bound futures. The opposite of TPC. Choosing Tokio would invalidate every other design decision in this project.

## What Monoio actually does

Each runtime owns:

- An `io_uring` SQ (submission queue) and CQ (completion queue), backed by a single ring file descriptor.
- A `LocalSet`-style scheduler running `!Send` futures.
- A registered-buffer table (`IORING_REGISTER_BUFFERS`) that backs `read_fixed` / `write_fixed`.
- A timer wheel for `monoio::time::sleep` and friends.

Per loop tick: drain ready completions from CQ, wake matching futures, run the scheduler queue, push pending operations onto SQ, submit. The driver is cooperative — there is no preemption; a task that doesn't yield blocks its core.

## Constraints we adopt

- Broker tasks are `!Send`. Any state shared across cores crosses an explicit message-passing boundary, not a `Send` future.
- One ring per core. Buffer registration is per-ring, so buffer pools are per-core by construction.
- No global runtime handle. Each thread bootstraps its own runtime in `main` after pinning to a core.
- No `tokio::spawn` or `Send`-bound async libraries on the data path. Anything pulled in needs to be Monoio-compatible or `!Send`-friendly.

## Things we will want to change in Monoio

These shape downstream design but do not block C1:

- **Explicit batched submission.** Monoio's default policy submits per-op or on yield boundaries. SQ batching (C4) needs a way to group N ops into one submit syscall; either a feature flag or a thin wrapper over the driver.
- **Multiple registered-buffer pools per ring.** Useful if we ever want priority classes (e.g., control-plane vs. data-plane). v1 has one pool per ring, deferred.

## Measurement plan

For C1 echo, we want to know:

- Syscalls per echo round-trip. Target: 0 syscalls in the steady state (everything routed through registered buffers and the ring). Measure with `perf stat -e raw_syscalls:sys_enter` over a long run.
- Time spent in driver vs. in user code. Measure via `perf record` on the bench client and server.
- p50 / p99 / p99.99 RTT for 256B payloads, single core, single connection. Recorded in `PERFORMANCE.md`.

Numbers from this exercise drive the design notes for C2 and C4.
