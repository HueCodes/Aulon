# Aulon

Aulon is a NATS-core-compatible message broker built thread-per-core on `tokio-uring`, with `io_uring` fixed buffers registered against the kernel via `IORING_REGISTER_BUFFERS` and a subscription router sharded by L3 cache domain. The publish hot path is allocation-free by construction: `PUB` parsing, trie matching, and `MSG` emission all operate over borrowed bytes into pre-registered buffers.

Single-message-in-flight publish-to-deliver latency, 4 subscribers, 256 B payload, OrbStack Ubuntu VM on an Apple M2 (bare-metal pending; see the [C5 review](docs/reviews/checkpoint-5.md)):

| backend | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: |
| Aulon | 29 us | 50 us | 70 us |
| nats-server 2.10.24 | 99 us | 313 us | 378 us |

Reproducer: `bash bench/headline.sh`. The full distribution, the methodology, and the bench-harness caveats (different pace-window settings per backend, single-process bench client) are in [`PERFORMANCE.md`](PERFORMANCE.md).

This is a single-node broker. JetStream, clustering, gateways, leafnodes, TLS, and authentication are out of scope for v1. The wire protocol implements the verbs needed for the official `nats` CLI and `nats bench` to run unmodified (`CONNECT`, `PUB`, `SUB`, `UNSUB`, `MSG`, `PING`, `PONG`, `INFO`, `+OK`, `-ERR`) with full `*` and `>` wildcard and queue-group support. See [`docs/SCOPE.md`](docs/SCOPE.md) for the compatibility matrix and [`docs/design/INDEX.md`](docs/design/INDEX.md) for the decisions behind the implementation.

The workspace: `aulon-proto` (`#![no_std]`-clean, allocation-free wire codec, fuzzed and proptested), `aulon-core` (per-core runtime, fixed-buffer pool, wildcard trie, topology, loom-tested cross-shard inbox), `aulon-server` (the binary), `aulon-bench` (HDR-histogram benchmark client). Dual-licensed under MIT and Apache-2.0.

## Status

Pre-v0.1. Checkpoints C0–C5 closed against the in-VM headline; the bare-metal single-row chart is the remaining carry-forward. Per-checkpoint reviews are under [`docs/reviews/`](docs/reviews/), the measurement log is [`PERFORMANCE.md`](PERFORMANCE.md), and one written-up debugging story is [`docs/war-stories/loom-tokio-cfg.md`](docs/war-stories/loom-tokio-cfg.md). A two-second asciinema cast of the official `nats` CLI publish-and-subscribe round-trip against Aulon is at [`docs/aulon-nats-demo.cast`](docs/aulon-nats-demo.cast) (open with `asciinema play`).

## Build

```
cargo build --release
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Aulon is Linux-only (it depends on `io_uring`). See [`docs/design/dev-environment.md`](docs/design/dev-environment.md) for the macOS-host + OrbStack-VM workflow.

## Reproducing the benchmarks

```
bash bench/echo.sh        # single-connection echo RTT, HDR histogram
bash bench/fanout.sh      # 1-publisher / N-subscriber fanout latency
bash bench/headline.sh    # Aulon vs nats-server back-to-back
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
