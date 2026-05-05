# Buffer pool

## Decision

Each runtime thread owns a fixed set of equally-sized buffers, registered
with its ring via `IORING_REGISTER_BUFFERS`. The pool is allocated once at
startup, registered before any I/O is issued, and never grows or shrinks.
Allocation is delegated to `tokio_uring::buf::fixed::FixedBufPool<Vec<u8>>`;
Aulon's `aulon_core::BufferPool` is a thin wrapper that pins down sizing,
capacity, and the registration step in one named place.

No global pool, no cross-core migration, no resizing.

## Why a wrapper at all?

`FixedBufPool` is doing the mechanical work — index management, kernel
registration, `FixedBuf` lifetime tracking. Wrapping it adds nothing
mechanical. The wrapper exists for **policy**:

- One sizing decision per pool (`buffer_size`, `capacity`), defaulted from
  `aulon_core::DEFAULT_BUFFER_SIZE` / `DEFAULT_POOL_CAPACITY`.
- One registration call, encapsulated.
- One named place to extend later — exhaustion metrics, priority classes,
  per-tenant pools — without touching call sites.

If a future change makes the wrapping layer too thin to justify, drop it
and use `FixedBufPool` directly. The wrapper has earned its place only as
long as it has policy to express.

## Sizing for v1

- 256 buffers per core.
- 4 KiB per buffer.
- 1 MiB total per core.

Rationale: 256 is well above any realistic in-flight count for one
connection on one core in C1's echo workload. 4 KiB is one page, large
enough to absorb full NATS frames in C2 without fragmenting. Revisit once
C2/C3 pool occupancy data lands.

## Allocation policy

```text
pub struct BufferPool {
    inner: FixedBufPool<Vec<u8>>,
    buffer_size: usize,
    capacity: usize,
}

impl BufferPool {
    pub fn new(capacity: usize, buffer_size: usize) -> Self;
    pub fn register(&self) -> io::Result<()>;
    pub fn acquire(&self) -> Option<FixedBuf>;
    pub async fn acquire_async(&self) -> FixedBuf;
}
```

- `new` builds `FixedBufPool` from `capacity` `Vec<u8>`s of `buffer_size`
  bytes each.
- `register` calls `inner.register()` from inside a `tokio_uring` runtime
  context. Must be called before any I/O.
- `acquire` returns `inner.try_next(buffer_size)` — the first available
  registered buffer at the standard size, or `None` if exhausted.
- `acquire_async` waits on `inner.next(buffer_size)` for backpressure on
  paths that can wait (the accept loop, eventually).

A `FixedBuf` returned from `acquire` is registered with the kernel; it
slots directly into `TcpStream::read_fixed` / `TcpStream::write_fixed_all`.
Dropping the `FixedBuf` returns it to the pool.

## Why no global pool

Three reasons:

1. **`IORING_REGISTER_BUFFERS` is per-ring.** A buffer registered with ring
   A is not addressable from ring B. A global pool would need to either
   re-register on migration (expensive) or skip registration entirely
   (defeats `read_fixed`).
2. **Cache locality.** A buffer freed on core 3 and re-acquired on core 7
   cold-misses on every byte. With 256 B–1 KiB messages the miss penalty
   dominates.
3. **Coordination cost.** Even a "lock-free" global pool is a CAS on a
   shared cache line. At broker rates (millions of ops/sec) those CAS ops
   are the bottleneck, not the syscalls.

## Why fixed-size for v1

`IORING_REGISTER_BUFFERS` requires identical-size regions. Multiple size
classes mean either multiple registered sets per ring or unregistered
fallback for off-class buffers. The complexity isn't justified in v1.

If C2 or C3 measurement shows the 4 KiB choice is leaving memory on the
table, the answer is to add size classes via per-ring secondary pools
(another `FixedBufRegistry` for control-plane messages, say), not to reach
for a global pool.

## Failure mode

Pool exhaustion is observable, not catastrophic. If `acquire` returns
`None`:

- The accept loop applies backpressure by not calling `accept` until a
  buffer returns (`acquire_async` is the natural primitive for this).
- Existing connections continue to serve traffic; their buffers cycle
  through the pool normally.
- An exhaustion event is a metric that drives sizing changes for the next
  iteration.

Pool-exhaustion handling is the only place in the data path where a
"graceful degrade" branch exists. It is documented here so future readers
do not try to remove it as dead code.

## Measurement plan

- Microbenchmark `acquire` / drop round-trip with `criterion`. Target:
  < 50 ns. Recorded as part of the C1 review when it lands on bare metal.
- Echo-time pool occupancy histogram: every N completions, sample
  `capacity − available`. Drives the v1 sizing decision.
- On exhaustion (which should not happen in C1 echo), log once with the
  count of in-flight buffers and the reason — never on the hot path of
  healthy operation.

## Test surface

Two unit tests live alongside the wrapper:

- `capacity_and_buffer_size_are_recorded` — sanity on the policy layer.
- `acquire_before_registration_succeeds` — confirms that `try_next` does
  not access the runtime context, so the wrapper is constructible and
  testable outside of `tokio_uring::start`.

Tests that exercise actual I/O through the registered pool are integration
tests run via `bench/echo.sh`.
