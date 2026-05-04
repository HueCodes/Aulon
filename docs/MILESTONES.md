# Aulon — Milestones

| ID | Title | Done definition |
|----|-------|-----------------|
| C0 | Scaffold | Workspace + CI + design-doc scaffolding. `cargo build`, `cargo clippy -- -D warnings`, `cargo test`, `cargo doc --no-deps` all pass on a clean clone. |
| C1 | Echo on Monoio with fixed buffers | `bench/echo.sh` reports p99.99 RTT for 256B echoes pinned to one core, recorded in `PERFORMANCE.md`. |
| C2 | Wire codec + flat subscription table | The `nats` CLI completes a `CONNECT` / `SUB` / `PUB` / receive-`MSG` round-trip against Aulon. `cargo fuzz` runs the codec >1M iterations cleanly. |
| C3 | Wildcard trie + queue groups | `nats bench` runs unmodified against Aulon and produces correct results with wildcards and queue groups. |
| C4 | L3-aware sharding + SQ batching | `bench/headline.sh` produces an Aulon-vs.-`nats-server` chart in `README.md` with reproducible commands. |
| C5 | Polish | Repo presentable to a hiring manager. First 200 words of README + one chart land the value without scrolling. |

See `docs/PROMPT.md` for the full working agreement, including entry conditions, gates, and required design docs per checkpoint.
