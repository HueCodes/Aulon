# Aulon

Aulon is a NATS-core-compatible message broker written in Rust on top of Monoio and io_uring. It is designed thread-per-core, with per-core fixed buffers registered against the kernel ring, and a subscription router sharded by L3 cache domain. The headline metric is p99.99 publish-to-deliver latency on a single core, measured against `nats-server` on the same hardware.

This is a single-node broker. JetStream, clustering, gateways, leafnodes, TLS, and authentication are out of scope for v1. The wire protocol implements the verbs needed for `nats bench` to run unmodified — `CONNECT`, `PUB`, `SUB`, `UNSUB`, `MSG`, `PING`, `PONG`, `INFO`, `+OK`, `-ERR` — with full wildcard and queue-group support. See `docs/SCOPE.md` for the precise compatibility matrix and `docs/MILESTONES.md` for the build sequence.

The project is structured as a Cargo workspace: `aulon-proto` (allocation-free wire codec), `aulon-core` (runtime, buffer pool, routing, topology), `aulon-server` (the binary), and `aulon-bench` (an HDR-histogram-aware benchmark client). Design decisions live in `docs/design/`. Performance evolution lives in `PERFORMANCE.md`. Aulon is dual-licensed under MIT and Apache-2.0.

## Status

Pre-v0.1. Currently at checkpoint C0 (scaffold). Nothing here works yet.

## Build

```
cargo build
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
