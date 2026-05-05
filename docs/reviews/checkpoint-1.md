# Checkpoint 1 review

## What did we ship?

- `aulon_core::buffer_pool` (`crates/aulon-core/src/buffer_pool.rs:1`) — `!Send`
  per-core slab of fixed-size `Box<[u8]>` chunks, `BufferId(u16)` indices,
  explicit `acquire` / `release`. `PooledBuffer` impls
  `monoio::buf::IoBuf` / `IoBufMut` so it slots into monoio's owned-buffer
  read/write API. Three unit tests cover round-trip, exhaustion, and sizing.
- `aulon_core::connection` (`crates/aulon-core/src/connection.rs:1`) — typestate
  `Connection<Active>` / `Connection<Closing>` with a sealed `State` trait.
  `Active` carries its rented `PooledBuffer` between calls; `shutdown`
  consumes self and returns the buffer for the caller to release. `Closing`
  exposes no methods — dropping sends FIN.
- `aulon-server` (`crates/aulon-server/src/main.rs:1`) — single-thread monoio
  io_uring runtime, accept loop, per-connection task with a typestate
  `Connection<Active>` driving an echo loop.
- `aulon-bench` (`crates/aulon-bench/src/main.rs:1`) — synchronous
  single-connection ping-pong client with HDR histogram, env-var configured,
  reporting p50/p90/p99/p99.9/p99.99/min/max.
- `bench/echo.sh` — pinning + reproducer harness.
- Design docs: `docs/design/runtime.md`, `docs/design/buffer-pool.md`,
  `docs/design/connection-lifecycle.md`, `docs/design/dev-environment.md`.
- `PERFORMANCE.md` with the C1 baseline.

## What did we measure?

256 B synchronous echo, single TCP connection, OrbStack Ubuntu VM on Apple
M2, two pinned cores. 100,000 iterations after 1,000 warm-up.

| Metric | Value (ns) |
| ---: | ---: |
| min | 11,160 |
| p50 | 25,055 |
| p99 | 34,879 |
| p99.99 | 63,647 |
| max | 149,503 |

Reproducer: `CARGO_TARGET_DIR=/tmp/aulon-target bash bench/echo.sh`.

## What did we decide?

- Use Monoio for the runtime (`docs/design/runtime.md`); accept the loss of
  control over fixed-buffer registration in exchange for ~3 weeks of saved
  driver work.
- Per-core buffer pool with `Box<[u8]>` slots and explicit acquire/release
  (`docs/design/buffer-pool.md`). No global pool, no cross-core migration,
  no Rc<RefCell<>>-based RAII.
- Connection state encoded as a sealed-trait typestate
  (`docs/design/connection-lifecycle.md`). `Active` / `Closing` are real
  types in C1; `Negotiating` arrives in C2 with the NATS `CONNECT` exchange.
- Linux dev environment is OrbStack Ubuntu VM
  (`docs/design/dev-environment.md`).
- MSRV floor lowered from 1.95 to 1.85 — the floor for `[workspace.lints]`
  and other features actually used. MSRV is a floor, not a target; we bump
  freely.

## What did we get wrong?

**The C1 gate as written committed to `IORING_REGISTER_BUFFERS` and
`read_fixed` / `write_fixed`. Monoio 0.2.4 does not expose any of these in
its public API.** A grep of the published crate's source returns zero hits
for `register_buffers`, `read_fixed`, or `ReadFixed`. We took the dependency
without verifying that the surface we needed was actually there. The right
move was to write a 30-minute spike to confirm `read_fixed` worked end-to-
end *before* committing to monoio in the design doc.

The buffer pool, the typestate, and the rest of C1 stand on their own and
will continue to make sense whatever runtime ends up underneath. The choice
of runtime is the part that needs to be revisited.

## What's deferred?

- **Fixed-buffer registration with io_uring.** Carried into a dedicated
  decision (see "What changed about the plan?" below). Buffer pool stays in
  place, ready to plug into a `read_fixed` / `write_fixed` API once one is
  available.
- **Buffer-pool acquire/release micro-benchmark.** Required by the C1 gate
  but not yet written. Trivial to add; lands when the runtime question is
  resolved so the bench numbers reflect the final shape.
- **Bare-metal headline number.** The VM-on-macOS p99.99 of ~64 µs is
  jitter-bound. The defensible single-core number lands as part of C4 on a
  dedicated Linux box.

## What changed about the plan?

The fixed-buffer commitment is the only locked decision in `docs/PROMPT.md`
that is now in tension with reality. There are three viable resolutions, and
each has costs:

1. **Drop `IORING_REGISTER_BUFFERS` from the C1 gate.** Run on monoio's
   standard read/write, which still goes through the kernel's io_uring
   submission queue but copies into / out of un-registered user buffers.
   Update the design docs to reflect; keep the buffer pool pattern, since
   it remains valuable for memory locality and predictable pressure even
   without registration. Cost: weakens the "fixed-buffer story" that was
   one of the headline differentiators.
2. **Migrate the runtime to `tokio-uring` or `compio`.** Both expose
   registered buffers and `read_fixed` / `write_fixed`. Cost: 1–2 days of
   integration work, lose monoio-specific tuning, possibly slower
   forward progress on C2.
3. **Contribute fixed-buffer support upstream to monoio.** A genuine OSS
   contribution, valuable on its own as a portfolio item. Cost: open-ended
   timeline; depends on maintainer responsiveness.

This decision lives in `docs/design/fixed-buffer-runtime.md` (to be written
when option is chosen). Until that decision lands, C1 is "feature-complete
modulo registered buffers"; the C1 gate is **not** declared closed.

`docs/MILESTONES.md` and `docs/PROMPT.md` will be updated in the same
commit that records the resolution.
