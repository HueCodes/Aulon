# Aulon — Milestones

| ID | Title | Done definition |
|----|-------|-----------------|
| C0 | Scaffold | Workspace + CI + design-doc scaffolding. `cargo build`, `cargo clippy -- -D warnings`, `cargo test`, `cargo doc --no-deps` all pass on a clean clone. |
| C1 | Echo on `tokio-uring` with fixed buffers | `bench/echo.sh` reports p99.99 RTT for 256B echoes pinned to one core, recorded in `PERFORMANCE.md`. |
| C2 | Wire codec + flat subscription table | The `nats` CLI completes a `CONNECT` / `SUB` / `PUB` / receive-`MSG` round-trip against Aulon. `cargo fuzz` runs the codec >1M iterations cleanly. |
| C3 | Wildcard trie + queue groups | `nats bench` runs unmodified against Aulon and produces correct results with wildcards and queue groups. |
| C4 | L3-aware sharding + SQ batching | Multi-worker bootstrap with `SO_REUSEPORT`; cross-shard PUB fan-out via lock-free MPSC inbox + eventfd wake; loom-tested. `bench/headline.sh` runs Aulon vs `nats-server` back-to-back. README chart deferred to C5 pending bare-metal run. |
| C5 | Polish | Repo presentable to a hiring manager. First 200 words of README + a p50/p99/p99.9 table for Aulon vs `nats-server` land the value without scrolling. In-VM numbers shipped 2026-05-14 with the bench-client publisher-pacing fix; bare-metal single-row chart deferred. `cargo deny` and `cargo public-api` enforced in CI. War story + asciinema cast committed. |

See `docs/PROMPT.md` for the full working agreement, including entry conditions, gates, and required design docs per checkpoint.
