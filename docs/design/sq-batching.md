## SQ batching (C4)

## Decision

**Accept `tokio-uring` 0.5's default submission policy.** No custom
batch driver, no upstream PR, no `submit()` micromanagement. The
broker's existing byte-stream outbound buffer (`docs/design/fanout.md`)
already coalesces many `MSG`s per `write_fixed_all`, and the
read accumulator absorbs many `PUB`s per `read_fixed`. The natural
batching from our buffer geometry dominates whatever `IORING_OP_*`-
level SQ batching we'd squeeze out of a custom policy.

We measured this. The numbers below justify *not* spending the C4
budget on it.

## What `tokio-uring` 0.5 does today

`tokio_uring::start` runs a current-thread `tokio::runtime` plus a
driver that:

1. Pushes operations onto the SQ as their futures are polled.
2. On every yield boundary the runtime calls into the driver, which
   issues an `io_uring_enter(submit, min_complete=0)` covering all
   newly-queued SQEs in one syscall.
3. Completions are drained from the CQ before the scheduler returns
   to user code.

So the runtime already batches per yield. Multiple ops queued
between yields share one `io_uring_enter`. There is no per-op
syscall.

## Measurement

Reproducer (run inside the OrbStack VM):

```bash
sudo perf stat \
  -e raw_syscalls:sys_enter,io_uring:io_uring_submit_req,io_uring:io_uring_complete \
  -o /tmp/perf-server.txt \
  -- taskset -c 0 /tmp/aulon-target/release/aulon-server &

AULON_ITERATIONS=10000 AULON_FANOUT=4 \
  taskset -c 1 /tmp/aulon-target/release/aulon-fanout

sudo kill -INT $(pgrep -f "aulon-server\$")
cat /tmp/perf-server.txt
```

Result on the OrbStack Ubuntu 6.14 VM, 1 publisher × 4 subscribers,
256-byte payload, 10,000 publish iterations + 1,000 warmup =
44,000 deliveries:

| metric | server-side count |
| ---: | ---: |
| `raw_syscalls:sys_enter` | 2,201 |
| `io_uring:io_uring_submit_req` (SQEs) | 3,835 |
| `io_uring:io_uring_complete` (CQEs) | 3,834 |
| Wall time | 0.74 s |

Derived ratios:

- **20.0 deliveries per server syscall.** 44,000 / 2,201. The
  byte-stream outbound buffer is doing most of the work — each
  `write_fixed_all` carries many `MSG`s.
- **1.74 SQEs per `io_uring_enter`.** 3,835 / 2,201. tokio-uring's
  per-yield batching gets us a useful but modest factor on top of
  the buffer-level batching.
- **One CQE per SQE within rounding.** No SQEs lost; the driver
  is draining cleanly.

## Why a custom batch policy is not worth it now

The 1.74× SQE-per-syscall factor caps the marginal win of any custom
batching at ≈1 syscall per io_uring_enter. The publish path's
syscall budget is dominated by:

- The kernel's network stack work behind each `write_fixed_all`
  (TCP send), which is done outside `io_uring_enter`.
- Connection accept and close paths, which can be left at the
  default policy without affecting steady-state throughput.

If we forced one syscall per drain (best-case batching) we'd cut
the 2,201 down to maybe 1,400. ~35% reduction at this load, but the
denominator is already so small (3 syscalls per ms of wall time)
that the absolute saving is in the noise.

The right time to revisit is if a future profile shows
`io_uring_enter` itself in the hot path (e.g. after we've squeezed
the higher-cost paths) or when the headline benchmark stalls on a
syscall-bound regime.

## What we'd do for C5+

If a future workload demands tighter SQ batching, the cleanest
mechanism is a thin wrapper over `tokio_uring::Runtime` that:

1. Exposes a `submit_now()` hook callable at known coalesce points
   (e.g. after a `PUB` fanout completes, before yielding).
2. Uses `IORING_SETUP_SQPOLL` to push SQEs to a kernel poller
   thread and skip `io_uring_enter` entirely on the steady-state
   path. SQPOLL has its own cost (a kernel thread that spins) so
   it's only a win at very high SQ rates.

Both are straightforward extensions; neither is on the C4 critical
path.

## Carry-forward

The C4 headline benchmark uses the default policy. If
`bench/headline.sh` shows Aulon is bottlenecked on `io_uring_enter`
relative to `nats-server`, this doc is the place to come back to.
