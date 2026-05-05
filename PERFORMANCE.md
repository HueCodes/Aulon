# Performance log

A running record of measurements as Aulon evolves. Each entry pins down: what
was measured, on what hardware, with what command, and what the numbers were.
The goal is reproducibility, not absolute leadership; keep the log honest.

## Methodology

- Histogram: `hdrhistogram` crate, bounds `1 ns – 60 s`, 3 significant figures.
- Latency definition: end-to-end synchronous round-trip time as observed by
  the client, measured per `Instant::now`. Not coordinated-omission corrected
  (synchronous one-shot ping-pong; correction is meaningful for paced /
  concurrent workloads and lands when those benchmarks do).
- Pinning: `taskset -c <cpu>` for both server and client.
- Warm-up: 1,000 iterations excluded from the recorded histogram.
- Reproducer: `bench/echo.sh`.

## C1 baseline — 2026-05-04

First end-to-end echo benchmark. The point of this run is to establish a
floor for later comparison, not to claim a competitive number. In particular,
the host is a macOS laptop with the workload running under an OrbStack-managed
Ubuntu VM, which introduces virtualisation jitter that shows up directly in
the tail percentiles. A bare-metal Linux number lands in C4 when the headline
chart is produced.

### Setup

- Host: MacBook Air, Apple M2, 8 GB RAM, macOS 25.4.0.
- Guest: OrbStack Ubuntu 25.04, kernel 6.19, aarch64, 8 vCPU, 4 GiB RAM.
- Toolchain: `rustc 1.91.1`, `cargo 1.91.1`, `release` profile (`lto = "fat"`,
  `codegen-units = 1`).
- Runtime: monoio 0.2.4 with the `iouring` driver.
- Buffers: per-core `BufferPool` of 256 × 4 KiB owned `Box<[u8]>` chunks,
  passed to `monoio::TcpStream::read` / `write_all`. **Buffers are not yet
  registered with `IORING_REGISTER_BUFFERS`**; see the C1 review for the
  reasoning and the migration plan.
- Connection: 1 TCP connection, single-core.
- Payload: 256 B, ASCII filler.
- Iterations: 100,000 (after 1,000 warm-up).
- Server pinned to CPU 0, client pinned to CPU 1.

### Command

```
CARGO_TARGET_DIR=/tmp/aulon-target bash bench/echo.sh
```

### Result

| Metric | Value (ns) |
| ---: | ---: |
| count | 100,000 |
| min | 11,160 |
| p50 | 25,055 |
| p90 | 27,007 |
| p99 | 34,879 |
| p99.9 | 47,775 |
| p99.99 | 63,647 |
| max | 149,503 |

### Notes

- The min (~11 µs) reflects monoio + io_uring + loopback + virtualisation
  overhead end-to-end. A bare-metal Linux box should compress this
  substantially; the relevant comparison datapoint will be collected in C4.
- p99.99 / max divergence (≈ 2.4×) is consistent with VM scheduling jitter.
- The pool's `acquire`/`release` micro-bench has not yet been written; it
  lands as part of the C1 review's deferred items.
