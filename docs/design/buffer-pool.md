# Buffer pool

## Decision

Each runtime thread owns a fixed set of equally-sized buffers, registered with its ring via `IORING_REGISTER_BUFFERS`. The pool is allocated once at startup and never grows or shrinks. Allocation is a free-list of `u16` indices stored in a `!Send` slab. No global pool, no cross-core migration, no resizing.

## Sizing for v1

- 256 buffers per core.
- 4 KiB per buffer.
- 1 MiB total per core.

Rationale: 256 is well above any realistic in-flight count for one connection on one core in C1's echo workload. 4 KiB is one page, large enough to absorb full NATS frames in C2 without fragmenting. These numbers are placeholders for the first measurement; revisit after C1 records actual pool occupancy under load.

## Allocation policy

```text
struct BufferPool {
    storage: Box<[u8]>,            // contiguous backing region, 256 * 4096 bytes
    free: Vec<BufferId>,           // free indices, popped from the back
    capacity: u16,
}
```

- `acquire() -> Option<BufferId>`: `self.free.pop()`. `None` when exhausted.
- `release(id: BufferId)`: `self.free.push(id)`.

`BufferId` is a `u16` newtype that doubles as the `buf_index` argument to `read_fixed` / `write_fixed`. A buffer's lifetime crosses three handoffs:

1. Acquired by the accept loop, handed to `read_fixed`.
2. Read completes. The same `BufferId` is handed to `write_fixed` (echo path) or routed to a subscriber's send queue (C2+).
3. Write completes. The owning core releases the index back to the free list.

Pool, slab, and free-list all live on one core; nothing is `Send`.

## Why no global pool

Three reasons:

1. **`IORING_REGISTER_BUFFERS` is per-ring.** A buffer registered with ring A is not addressable from ring B. A global pool would need to either re-register on migration (expensive) or skip registration entirely (defeats `read_fixed`). Both options give up the latency floor that motivated using io_uring in the first place.
2. **Cache locality.** A buffer freed on core 3 and re-acquired on core 7 cold-misses on every byte. With 256B-1KiB messages the miss penalty dominates.
3. **Coordination cost.** Even a "lock-free" global pool is a CAS on a shared cache line. At broker rates (millions of ops/sec) those CAS ops are the bottleneck, not the syscalls.

## Why fixed-size for v1

`IORING_REGISTER_BUFFERS` requires identical-size regions. Multiple size classes mean either multiple registered sets per ring (Monoio doesn't expose this directly) or unregistered fallback for off-class buffers (gives up `read_fixed`). The complexity isn't justified in v1.

If C2 or C3 measurement shows the 4 KiB choice is leaving memory on the table, the answer is to add size classes via per-ring secondary pools, not to reach for a global pool.

## Failure mode

Pool exhaustion is observable, not catastrophic. If `acquire()` returns `None`:

- The accept loop applies backpressure by not calling `accept` until a buffer returns.
- Existing connections continue to serve traffic; their buffers cycle through the pool normally.
- An exhaustion event is a metric that drives sizing changes for the next iteration.

Pool-exhaustion handling is the only place in the data path where a "graceful degrade" branch exists. It is documented here so future readers don't try to remove it as dead code.

## Measurement plan

- Microbenchmark `acquire` / `release` round-trip with `criterion`. Target: < 5 ns. Recorded in the C1 review.
- Echo-time pool occupancy histogram: every N completions, sample `capacity - free.len()`. Drives the v1 sizing decision.
- On exhaustion (which should not happen in C1 echo), log once with the count of in-flight buffers and the reason — never on the hot path of healthy operation.
