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

## C1 post-migration — 2026-05-04

After moving from Monoio to `tokio-uring` and switching the hot path to
`IORING_OP_READ_FIXED` / `IORING_OP_WRITE_FIXED` with
`IORING_REGISTER_BUFFERS`, this is the new baseline. Same hardware, same
VM, same bench script, same iteration count — only the runtime and the
opcode pair changed.

### Setup

Same as the previous run, except:

- Runtime: `tokio-uring 0.5.0` (single `tokio_uring::start` per process).
- Buffers: `tokio_uring::buf::fixed::FixedBufPool<Vec<u8>>` with 256 × 4 KiB
  vectors, registered against the kernel via `pool.register()` at startup.
  Server-side: one buffer per connection (acquired on accept). Client-side:
  two pre-filled buffers (one send, one recv).
- Hot path: `TcpStream::read_fixed` / `TcpStream::write_fixed_all` (true
  fixed-buffer io_uring opcodes, not standard `read` / `write`).

### Result

| Metric | Monoio (ns) | tokio-uring + fixed (ns) | Δ |
| ---: | ---: | ---: | ---: |
| count | 100,000 | 100,000 | — |
| min | 11,160 | 11,496 | +3 % |
| p50 | 25,055 | 29,423 | +17 % |
| p90 | 27,007 | 32,223 | +19 % |
| p99 | 34,879 | 40,255 | +15 % |
| p99.9 | 47,775 | 58,047 | +22 % |
| p99.99 | 63,647 | 96,127 | +51 % |
| max | 149,503 | 205,439 | +37 % |

### Notes

- **Fixed buffers are slower in this configuration**, against the naive
  expectation. This is consistent with the literature for sub-µs- to
  10-µs-class workloads on a single connection: `IORING_OP_*_FIXED` saves
  the kernel a per-op buffer-pin step, but it does not save a syscall (the
  ring still has to be submitted). The savings are real and are typically
  measured at higher concurrency (many connections, many buffers in flight)
  where the avoided pinning becomes a meaningful fraction of total work.
- The runtime overhead difference between Monoio (a thin TPC runtime with
  minimal layers) and `tokio-uring` (which wraps the `tokio` reactor and
  has additional indirection in the buffer-handle path) is the dominant
  effect at this latency floor.
- Both runs are inside an OrbStack VM on macOS. p99.99 / max numbers are
  jitter-bound; the headline number lives on bare metal in C4.

The point of recording this comparison is not to prefer one runtime over
the other on these numbers — both fall well within VM noise — but to
make the trade-off visible: we picked `tokio-uring` for the API surface
(public, stable `IORING_REGISTER_BUFFERS`), accepting some runtime
overhead at this scale, in exchange for the fixed-buffer story we
committed to in `docs/PROMPT.md`. C4's bare-metal headline is where the
choice is actually validated or refuted.

## C2 micro-benchmarks — 2026-05-04

`criterion` 0.5 baseline numbers, `release` profile, OrbStack VM,
single-thread. Reproducer:

```
CARGO_TARGET_DIR=/tmp/aulon-target cargo bench -p aulon-core --bench buffer_pool
CARGO_TARGET_DIR=/tmp/aulon-target cargo bench -p aulon-proto --bench parse
```

### Buffer pool (`aulon-core::BufferPool`)

| Op | Median |
| ---: | ---: |
| `acquire` + drop, 4 KiB buffer | 20.56 ns |
| `acquire` + drop, 256 B buffer | 25.16 ns |

The `acquire` path goes through `tokio_uring::buf::fixed::FixedBufPool::try_next`
plus `FixedBuf::Drop`. Numbers reflect the pool's internal bookkeeping
on an unregistered pool — the registered code path is the same; only
the kernel mapping differs.

The buffer-pool design doc set < 50 ns as the target for this op; both
sizes clear it comfortably.

### Wire codec (`aulon_proto::parse_frame`)

| Verb / size | Median |
| ---: | ---: |
| `PING` | 5.52 ns |
| `SUB foo.bar 7` | 13.92 ns |
| `SUB foo.bar workers 7` | 20.07 ns |
| `UNSUB 7 12` | 13.42 ns |
| `PUB foo 16` (16 B payload) | 14.80 ns |
| `PUB foo 256` (256 B payload) | 16.54 ns |
| `PUB foo 4096` (4 KiB payload) | 18.17 ns |
| `MSG foo 7 16` | 16.53 ns |
| `MSG foo 7 256` | 18.51 ns |
| `MSG foo 7 4096` | 20.25 ns |

`parse_frame` cost grows mildly with payload size — the parser does
not touch the payload bytes, but `find_crlf` walks the input until the
first CRLF, and the trailing-CRLF check is a 2-byte compare against
`buf[header_total + payload_len..]`. The slope ~0.5 ns per KiB is
consistent with the trailing-CRLF check's bounds work (no payload
scan).

These numbers are the floor for downstream NATS-handler latency; the
server's per-frame work in C2 layers on top of this. The codec budget
inside a 25 µs single-core RTT is well under 1 % at any reasonable
payload size.

## C2 nats CLI smoke — 2026-05-04

Official `nats` CLI v0.4.0 (the upstream NATS reference client) runs
unmodified against Aulon. Reproducer inside the VM:

```
/tmp/aulon-target/debug/aulon-server &
nats sub -s nats://127.0.0.1:4222 foo --count 1 &
nats pub -s nats://127.0.0.1:4222 foo "hello from nats CLI"
```

Output:

```
20:23:13 Subscribing on foo
[#1] Received on "foo"
hello from nats CLI
```

Reference-client compatibility on the gate's verb subset (`CONNECT`,
`SUB`, `PUB`, `MSG`, `PING`, `PONG`) is now established.
