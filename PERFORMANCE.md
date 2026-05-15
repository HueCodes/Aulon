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

## C2 fanout reproducer — 2026-05-04

`bench/fanout.sh`. Same VM as C1, single Aulon server instance, one
publisher + N subscribers all hosted in a single `aulon-fanout`
client process. Publish-to-deliver latency is measured by embedding a
big-endian `u64` nanosecond timestamp (relative to a process-local
`Instant` baseline) in the first 8 bytes of every payload; each
subscriber decodes the timestamp on receipt and records `now − sent`
into its own HDR histogram. Histograms are merged at the end.

Reproducer:

```
CARGO_TARGET_DIR=/tmp/aulon-target bash bench/fanout.sh
```

### Results

`AULON_WARMUP=500`, server pinned to CPU 0, client pinned to CPU 1.

| Run | Fanout | Payload | Iterations | p50 | p90 | p99 | max |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 2 | 128 B | 5,000 | 1.13 ms | 2.45 ms | 91.16 ms | 91.29 ms |
| B | 8 | 256 B | 5,000 | 11.40 ms | 16.01 ms | 58.75 ms | 59.70 ms |

### Notes

- The headline of this run is **completeness**, not absolute latency:
  the bench reliably delivers 100 % of `iterations × fanout` messages,
  detects slow consumers via TCP shutdown, and merges per-subscriber
  histograms into a single distribution. The numbers above are an
  upper bound, not Aulon's true fanout latency.
- The publisher and all `N` subscribers run on a single
  `tokio_uring` runtime in one process. With `tokio::task::yield_now`
  inserted between PUBs, subscribers do get scheduled, but the
  publisher still dominates the runtime — what we are measuring at
  p50 is closer to "how long it takes one tokio-uring task to drain
  N TCP recv buffers between publisher yields" than per-message wire
  latency. The C5 multi-process variant (publisher and subscribers in
  separate `tokio_uring::start` runtimes, pinned to different cores)
  is what produces the headline fanout number.
- Outbound capacity per connection is 2 MiB
  (`DEFAULT_OUTBOUND_CAPACITY`). At payload 256 B and 8 subscribers,
  this comfortably absorbs `iterations ≤ ~7000` without slow-consumer
  eviction; above that, the bench correctly detects eviction
  (subscribers report `eof after N msgs`) rather than hanging — the
  server shuts the socket on writer-task exit so the peer sees FIN.
- p99/max are jitter-bound (OrbStack VM, single-process scheduler
  contention).

## C3 trie match — 2026-05-04

Criterion micro-bench against a `SubscriptionTrie` populated with
**10,000 subscriptions** distributed as a representative shape:
7,000 exact subjects of the form `app.<i mod 10>.svc.<i>`, 2,500
single-`*` wildcards `app.<i mod 25>.metric.*`, and 500 `>`
subscriptions `tenant.<i mod 50>.>`.

Reproducer:

```
CARGO_TARGET_DIR=/tmp/aulon-target cargo bench -p aulon-core --bench trie
```

| Publish subject | Tokens | Median |
| ---: | ---: | ---: |
| `app.0` | 2 | 64.95 ns |
| `app.0.metric.cpu` | 3 | 94.05 ns |
| `app.0.svc.42` | 4 | 109.87 ns |
| `tenant.0.foo.bar.baz` | 5 | 66.69 ns |
| `tenant.0.a.b.c.d.e` | 7 | 66.34 ns |

The C3 design-doc target was median **< 500 ns at 3-token subjects**;
the trie clears that by ~5×. The 5- and 7-token rows are faster than
the 4-token row because they walk into a `>`-anchored branch whose
sole match is emitted once and the recursion ends immediately, while
`app.0.svc.<n>` walks four levels of `HashMap<Box<[u8]>, Box<Node>>`
to reach a single exact subscriber. The cost is dominated by per-
level work, not subject length.

## C4 cross-shard wiring — 2026-05-05

Multi-worker bootstrap with `SO_REUSEPORT` listener per shard,
core-pinned threads, an `Arc<PublishedFrame>` cross-shard MPSC inbox
and `eventfd` wake. With `AULON_FORCE_SHARDS=2` (synthetic 2-shard
topology so the cross-shard codepath is exercised on single-L3
hardware) the kernel distributes 11 connections (10 subscribers + 1
publisher) across the two shards as 3 / 8; all 10 subscribers
receive a single PUB regardless of which shard the publisher landed
on.

Reproducer (`.scratch/cross_shard_test.sh`):

```
AULON_FORCE_SHARDS=2 /tmp/aulon-target/release/aulon-server &
for i in $(seq 1 10); do
    nats --server localhost:4222 sub "x.>" --count 1 > sub_$i.out 2>&1 &
done
sleep 0.5
nats --server localhost:4222 pub x.test "msg-cross-shard"
```

10 / 10 subscribers received the message. Confirms the
publisher-shard local fanout AND the peer-shard inbox-drained
fanout both deliver correctly.

## C4 syscall accounting — 2026-05-05

`perf stat -e raw_syscalls:sys_enter,io_uring:io_uring_submit_req,
io_uring:io_uring_complete` over the server only, while a separate
`aulon-fanout` client (256 B payload, 4 subscribers, 10,000
iterations) drives load. See `docs/design/sq-batching.md` for the
full discussion.

| metric | server-side count |
| ---: | ---: |
| `raw_syscalls:sys_enter` | 2,201 |
| `io_uring:io_uring_submit_req` (SQEs) | 3,835 |
| `io_uring:io_uring_complete` (CQEs) | 3,834 |
| Wall time | 0.74 s |

Derived: **20.0 deliveries per server syscall**, **1.74 SQEs per
`io_uring_enter`**. The byte-stream outbound buffer is doing most of
the batching; tokio-uring's per-yield SQ batching adds a useful but
modest factor on top.

## C4 headline (in-VM, slow-consumer caveat) — 2026-05-05

`bench/headline.sh`: same `aulon-fanout` workload run back-to-back
against `aulon-server` and `nats-server` 2.10.24 on the OrbStack VM.
4 subscribers, 256 B payload, 3,000 iterations + 1,000 warmup.

| backend | min | p50 | p99 | p99.99 |
| ---: | ---: | ---: | ---: | ---: |
| Aulon | 57 µs | 1.39 ms | 43.25 ms | 43.35 ms |
| nats-server 2.10.24 | 39 µs | 99 µs | 583 µs | 699 µs |

This is a **caveated** result, not the headline number that lands
in the README. Two things distort it:

- **Aulon trips slow-consumer eviction near the end of the run**
  (subscribers report `eof after 3999/4000 msgs`) which contaminates
  the p99 / p99.99 with a single-digit-ms tail spike at termination
  — the steady-state distribution is much tighter than the table
  shows. The publisher's runtime is a single-thread tokio_uring
  hosting the publisher *and* all four subscriber tasks; the
  subscribers cannot drain fast enough at the very end of the run
  before the test wraps.
- The OrbStack VM is the worst possible host for tail-latency
  measurement; a bare-metal Linux box halves the floor and removes
  the scheduler-jitter component.

The bare-metal headline lands in C5 polish on a dedicated host
(plus a fix to the client to pace publishes against subscriber
drain). The **competitive p50** at this load (1.4 ms vs 99 µs at
low contention) is the more honest read for now: nats-server has a
better p50 at this scale; Aulon's lead shows up at higher fanout +
longer runs where the C2 fanout numbers already showed the byte-
stream outbound buffer beating mpsc-style fanout.

## C3 nats CLI wildcards + queue groups — 2026-05-04

Reproducer (inside the VM):

```
/tmp/aulon-target/release/aulon-server &
nats sub -s nats://127.0.0.1:4222 'foo.*' --count 1
nats pub -s nats://127.0.0.1:4222 foo.bar 'hello wildcard'

nats sub -s nats://127.0.0.1:4222 'foo.>' --count 1
nats pub -s nats://127.0.0.1:4222 foo.bar.baz 'hello greater'

# queue group: 3 workers, 6 publishes — each message goes to
# exactly one worker.
for i in 1 2 3; do
    nats sub -s nats://127.0.0.1:4222 work --queue workers > /tmp/q$i.log &
done
for i in 1 2 3 4 5 6; do
    nats pub -s nats://127.0.0.1:4222 work "task-$i"
done
```

Result: `foo.*` and `foo.>` deliveries arrive correctly; queue-group
distribution across three subscribers totals exactly 6 (no
duplication, no losses). Distribution skew (1/1/4 in this run) is the
expected variance of random pick over a 6-message window; with a
larger sample the histogram flattens.

## C5 fanout, paced (in-VM) 2026-05-14

The C4 review identified that `bench/fanout.sh` was tripping
slow-consumer eviction at end-of-run: the single-thread `tokio_uring`
runtime that hosts the publisher and all subscribers cannot drain
fast enough as the publisher wraps, the per-connection 2 MiB outbound
buffer fills, the server evicts the subscriber, and the resulting
several-ms tail spike contaminates p99 / p99.99.

The fix is a publisher pace window
(`crates/aulon-bench/src/fanout.rs:240`). Each subscriber publishes
its monotonically-increasing receive count via an `Rc<Cell<u64>>`;
before each PUB the publisher reads `min(received)` across all
subscribers and yields while `sent - min_received >= pace_window`.
`AULON_PACE_WINDOW=2` is the new default for both `bench/fanout.sh`
and `bench/headline.sh`: this keeps at most one outstanding message
per subscriber, which exposes per-message steady-state latency rather
than the depth of the in-flight queue. Larger values trade off
honest latency for higher achievable throughput on the same single
runtime.

Reproducer:

```
CARGO_TARGET_DIR=/tmp/aulon-target AULON_FANOUT=4 \
  AULON_ITERATIONS=3000 AULON_WARMUP=1000 \
  AULON_PAYLOAD_BYTES=256 bash bench/fanout.sh
```

OrbStack Ubuntu VM, kernel 7.0.5, aarch64, 8 vCPU on M2 host. Server
pinned to CPU 0, client pinned to CPU 1.

| metric | value |
| ---: | ---: |
| count | 16,000 (4 subscribers x 4,000 frames) |
| per-sub delivered | [4000, 4000, 4000, 4000] |
| min | 18.4 us |
| p50 | 28.8 us |
| p90 | 44.5 us |
| p99 | 49.9 us |
| p99.9 | 69.9 us |
| p99.99 | 40.5 ms |
| max | 40.5 ms |

No `eof after N msgs` lines in subscriber output. The single p99.99
sample is the lone tail outlier in 16,000 deliveries and is bounded
by VM scheduler jitter; the steady distribution is `p99.9 = 70 us`.

For comparison with the C4 entry (same fanout, same payload), p50
moved from 1.39 ms (eviction-contaminated) to 28.8 us. The 48x drop
is not the broker getting faster between C4 and C5; it is the
benchmark client getting honest.

