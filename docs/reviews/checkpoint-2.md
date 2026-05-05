# Checkpoint 2 review

## What did we ship?

- `aulon_proto` (`crates/aulon-proto/src/`) — NATS-core wire codec.
  `Frame<'a>` enum covering all 10 verbs (`CONNECT`, `PUB`, `SUB`,
  `UNSUB`, `MSG`, `PING`, `PONG`, `INFO`, `+OK`, `-ERR`). Streaming
  `parse_frame(&[u8]) -> ParseOutcome` and per-verb `emit_*` /
  `emit_frame` writers. `#![cfg_attr(not(test), no_std)]` core. No
  allocator, no panics on adversarial input.
- `aulon_core::subscription` — `SubscriptionTable` with
  `HashMap<Box<[u8]>, SmallVec<[Sub; 4]>>`, `ConnectionId(u32)`.
- `aulon_core::connection_state` — shared `Rc<ConnectionState>`
  between reader and writer tasks. Byte-stream outbound buffer
  (`Box<[Cell<u8>]>` + `Cell<usize>` head/tail), `tokio::sync::Notify`
  for wakeups, `CloseReason::{SlowConsumer, ProtocolError, PeerClosed}`.
  Default outbound capacity 2 MiB.
- `aulon-server` (`crates/aulon-server/src/main.rs:1`) — full NATS
  handler. Two tasks per connection (reader, writer) sharing the
  state. `INFO` on accept, `SUB` table maintenance, `PUB` fanout via
  per-subscriber `MSG` re-emit into a worker-local heap-allocated
  emit scratch (sized for the full `MAX_PAYLOAD_BYTES = 1 MiB`,
  allocated once at startup, never reallocated). `MAX_FRAME_SIZE`
  cap on the read accumulator (DoS guard). `PUB` rejects oversized
  payloads with `-ERR maximum payload exceeded`. Writer task
  `shutdown(Both)`s the socket on close so the reader and the peer
  both see EOF.
- `aulon-bench --bin aulon-fanout` (`crates/aulon-bench/src/fanout.rs:1`)
  — 1 publisher + N subscribers in one `tokio_uring` runtime,
  publish-to-deliver latency via embedded big-endian `u64`
  timestamps, per-subscriber HDR histograms merged at the end.
- `bench/fanout.sh` — taskset-pinned reproducer.
- Property tests:
  `crates/aulon-proto/tests/proptest_roundtrip.rs` — 12 properties,
  256 cases each per-variant + 1024 robustness cases; one
  buffer-too-small property at 64 cases.
- Fuzz target: `fuzz/fuzz_targets/parse_frame.rs` running under
  `cargo +nightly fuzz`. 60 s, 47 M iterations, no findings.
- Criterion micro-benches:
  `crates/aulon-core/benches/buffer_pool.rs`,
  `crates/aulon-proto/benches/parse.rs`. Numbers in `PERFORMANCE.md`.
- Design docs: `docs/design/wire-codec.md`,
  `docs/design/routing-v1.md`, `docs/design/fanout.md`.
- Reference-client smoke: official `nats` CLI v0.4.0 connects, subs,
  pubs, receives. Transcript in `PERFORMANCE.md`.

## What did we measure?

### Codec micro-benches (criterion, OrbStack VM)

| Verb / size | Median |
| ---: | ---: |
| `PING` | 5.52 ns |
| `SUB foo.bar 7` | 13.92 ns |
| `PUB foo 256` | 16.54 ns |
| `MSG foo 7 4096` | 20.25 ns |

### Buffer pool

- `acquire` + drop, 4 KiB: 20.56 ns
- `acquire` + drop, 256 B: 25.16 ns

### Fanout

| Run | Fanout | Payload | Iterations | p50 | p90 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 2 | 128 B | 5,000 | 1.13 ms | 2.45 ms | 91.16 ms |
| B | 8 | 256 B | 5,000 | 11.40 ms | 16.01 ms | 58.75 ms |

These are correctness-and-completeness numbers, not headline numbers.
The publisher and all `N` subscribers run on a single `tokio_uring`
runtime; even with `yield_now()` between PUBs, the publisher
dominates the runtime and what's measured at p50 is closer to
"batch drain time" than per-message wire latency. The headline
fanout number lives in C5 with publisher and subscribers in
separate processes pinned to different cores.

### Fuzz

`cargo +nightly fuzz run parse_frame -- -max_total_time=60` —
**47,130,426 iterations, 199 covered branches, no findings.** Gate
target was >1M iterations clean.

### Property tests

`cargo test -p aulon-proto` runs the 12 round-trip properties plus
two robustness/buffer-too-small properties. All pass.

## What did we decide?

- Two-task topology (reader + writer with `Rc<TcpStream>`,
  `Rc<ConnectionState>`) over `select!`-style multiplexing. Cancel-
  safety reasoning is in `docs/design/fanout.md`. The result is
  that no `select!` futures are dropped mid-IO and the writer can
  drive close-and-shutdown deterministically.
- Byte-stream outbound buffer (one allocation per connection at
  accept, lifetime-tied to `ConnectionState`). The publish path
  performs **no allocation** other than the per-PUB subscriber-list
  snapshot (`Vec<(ConnectionId, Box<[u8]>)>`). C3 will revisit the
  snapshot once the routing table is callback-shaped.
- `MSG` is re-emitted per subscriber rather than encoded once and
  fanned out raw, because the `sid` (the subscriber's own
  identifier on `SUB`) varies per recipient. The encode step uses
  a worker-local heap-allocated scratch buffer; one allocation at
  startup, reused for the lifetime of the worker.
- Default per-connection outbound capacity 2 MiB. Sized to absorb
  bursty publishers at fanout ≥ 8 without triggering slow-consumer
  eviction in the common case. Real NATS uses a similar order of
  magnitude.
- Writer task `shutdown(Both)`s the socket on exit. Without this, a
  writer-initiated close (e.g. slow-consumer eviction) would leave
  the reader pinned on a never-completing `read_fixed` and the
  peer would never see FIN. Discovered via the fanout bench
  hanging at 0 % CPU on N=8 fanout × 10K iterations.

## What did we get wrong?

- **Initial fanout doc claimed "encode once, fanout raw"** — this
  was wrong because the `sid` differs per subscriber and is
  embedded in the `MSG` header. Doc was corrected mid-checkpoint.
- **Initial server had no DoS guard on the read accumulator.** A
  malicious or buggy client streaming bytes without ever
  terminating a frame would have grown the accumulator without
  bound. Capped at `MAX_FRAME_SIZE = 2 × MAX_PAYLOAD_BYTES`,
  connection marked `ProtocolError` on overflow.
- **Initial server's `INFO` advertised 1 MiB max payload but the
  emit path used a 4 KiB stack buffer.** Large `MSG` frames would
  have been silently dropped at the emitter. Fixed by allocating
  a per-worker heap scratch sized for the full max payload, plus
  rejecting oversized PUBs at the parser boundary.
- **Initial server did not shutdown the socket on writer close.**
  Discovered when the fanout bench deadlocked. The fix is small
  and is now load-bearing.
- **Proptest found a parser round-trip bug**: `Frame::Err {
  message: " " }` (single space) was emitted as `-ERR  \r\n` and
  parsed back as `Err { message: [] }`. Root cause: `parse_err`
  / `parse_connect` / `parse_info` were calling
  `trim_leading_spaces(rest)` which ate the message's *own* leading
  whitespace. Fix: `split_verb` consumes exactly one separator
  byte; the body parsers no longer trim. All 57 unit + property
  tests now pass.

## What's next?

- **C3 — observability and PING/PONG keepalive.** Server-initiated
  PING on idle, slow-consumer counters, basic stats endpoint.
- **C3.5 — routing callbacks.** Eliminate the per-PUB
  subscriber-list snapshot by fanning out under the
  `SubscriptionTable` borrow with a callback that does the
  per-subscriber `enqueue`.
- **C4 — multi-core sharding.** L3-cache-aware shard placement
  via `hwloc`; per-core workers; shared-nothing routing across
  cores.
- **C5 — multi-process headline bench.** Publisher and N
  subscribers in separate `tokio_uring::start` runtimes pinned to
  different cores. This is the run that produces the real
  publish-to-deliver fanout number.
