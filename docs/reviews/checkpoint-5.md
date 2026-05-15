# Checkpoint 5 review

## What did we ship?

- **Bench-client publisher pacing**
  (`crates/aulon-bench/src/fanout.rs:240`). Each subscriber publishes
  its receive count through an `Rc<Cell<u64>>`; before each PUB the
  publisher yields while `sent - min(received) >= AULON_PACE_WINDOW`.
  New default `pace_window = 2` in both
  `bench/headline.sh:57` and the binary. Resolves the C4 carry-
  forward where Aulon tripped slow-consumer eviction at end-of-run.
- **Honest in-VM headline.** `PERFORMANCE.md` C5 entries replace the
  C4 eviction-contaminated numbers: Aulon p50 = 29 us, p99 = 50 us,
  p99.9 = 70 us at fanout=4, 256 B payload. nats-server stays at its
  C4 p50 = 99 us baseline.
- **README chart**
  (`README.md:5`). p50 / p99 / p99.9 table for both backends inside
  the first 200 words, with a one-line pointer to
  `PERFORMANCE.md` for the methodology and caveats.
- **cargo-deny config + CI gate** (`deny.toml:1`,
  `.github/workflows/ci.yml:43`). Permissive licenses allow-list,
  yanked = deny, advisories = deny, single registry. Workspace path
  deps gated behind `allow-wildcard-paths = true` with the
  semantic fix landed alongside: `aulon-server` and `aulon-bench`
  are marked `publish = false`.
- **aulon-core public-API snapshot + drift CI gate**
  (`crates/aulon-core/PUBLIC_API.txt:1`,
  `scripts/snapshot-public-api.sh:1`,
  `.github/workflows/ci.yml:53`). 660-line snapshot from
  `cargo-public-api`; CI regenerates the file and fails on diff so
  every public-surface change is a deliberate review event.
- **War-story write-up**
  (`docs/war-stories/loom-tokio-cfg.md:1`). Standalone post-mortem
  of the `RUSTFLAGS="--cfg loom"` cfg-leak that broke
  `tokio-uring`'s build in C4. Linked from both `README.md:11` and
  `docs/design/INDEX.md:82`.
- **Design INDEX** (`docs/design/INDEX.md:1`). Per-checkpoint index
  of every committed design doc plus the war stories section.
- **asciinema cast** (`docs/aulon-nats-demo.cast:1`). Two-second
  recording of `aulon-server` + `nats sub` + `nats pub` round-
  tripping. Linked from `README.md:11`. Not uploaded to
  asciinema.org; that stays gated on explicit approval.
- **Intra-doc link fix in `shard_inbox.rs`**
  (`crates/aulon-core/src/shard_inbox.rs:32`). Surfaced by
  `cargo-public-api`'s rustdoc invocation; broken `[Arc]` link
  replaced with the absolute path.
- **README + PERFORMANCE.md prose polish.** Em dashes removed from
  the README C5 prose (universal voice rule). PERFORMANCE.md C5
  sections written em-dash-free.
- This review (`docs/reviews/checkpoint-5.md`).

## What did we measure?

### Fanout, paced

`bench/fanout.sh` with `AULON_FANOUT=4 AULON_ITERATIONS=3000
AULON_WARMUP=1000 AULON_PAYLOAD_BYTES=256 AULON_PACE_WINDOW=2`, in
OrbStack Ubuntu VM, server pinned to CPU 0, client pinned to CPU 1.
All 16,000 deliveries land cleanly; no `eof after N msgs` in
subscriber output. min 18 us, p50 29 us, p90 44 us, p99 50 us,
p99.9 70 us, p99.99 40 ms (one outlier in 16,000), max 40 ms.

### Headline, in-VM

Same hardware. Aulon at `pace=2` (clean, all messages delivered),
nats-server at `pace=0` (no eviction, so no pacing needed):

| backend | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: |
| Aulon | 29 us | 50 us | 70 us |
| nats-server 2.10.24 | 99 us | 313 us | 378 us |

The bench-harness constraint that forces different `pace_window`
settings per backend is documented in
`PERFORMANCE.md` "C5 headline, in-VM".

### Supply-chain

`cargo deny --all-features check`: advisories ok, bans ok, licenses
ok, sources ok.

### Verification pass

Inside the VM, `CARGO_TARGET_DIR=/tmp/aulon-target`:

- `cargo fmt --check` clean
- `cargo clippy --all-targets --all-features -- -D warnings` clean
- `cargo test --all-features` passes (no failures, one doctest)
- `cargo doc --no-deps --all-features` with `RUSTDOCFLAGS=-D
  warnings` clean
- `cargo deny --all-features check` clean
- `scripts/snapshot-public-api.sh` produces an
  identical-to-committed `PUBLIC_API.txt`

## What did we decide?

- **`pace_window = 2` as the default.** Single-message-in-flight
  back-pressure is the smallest window that exposes the broker's
  steady-state per-message latency rather than the depth of the
  in-flight queue. Larger windows trade latency honesty for
  throughput; `0` disables pacing entirely and matches the
  C4-era behaviour.
- **Per-backend pace window in the in-VM headline.** Aulon needs
  `pace=2` to avoid the C4 eviction tail; nats-server's outbound
  batching stalls in our single-runtime bench client at `pace=2`,
  but nats-server does not trip Aulon's slow-consumer artefact at
  this scale and can be measured cleanly at `pace=0`. The fair
  apples-to-apples row requires a multi-process bench client (or
  bare metal where the bench has the cycles to keep up), so it
  stays a carry-forward rather than being faked.
- **`allow-wildcard-paths = true` plus `publish = false` on the
  internal binaries.** Workspace-internal path deps are not the
  threat `wildcards = "deny"` is aimed at; the rule still applies
  to anything that could be published. `aulon-server` and
  `aulon-bench` are never published, so dropping their publishable
  status is the honest fix that lets the wildcard allowance stay
  narrow.
- **`cargo-public-api` snapshot in version control, drift gate in
  CI.** A line-level snapshot is the smallest review surface for
  public-API changes; CI's `git diff --exit-code` after re-running
  the script forces every API change to be a deliberate commit.
- **Markdown table in the README, not a chart.** Generating a PNG
  required either a Rust plotting crate (rejected by PROMPT.md's
  anti-pattern list) or dumping the full HDR histogram to disk and
  feeding it through gnuplot, which is a bench-harness change well
  out of C5 scope. A table is sufficient and stays inside the
  first 200 words.
- **No asciinema upload.** The cast file is committed; pushing to
  asciinema.org is a public action and stays gated on per-task
  approval per the working-agreement communication norms.

## What did we get wrong?

- **Pace-window interaction with nats-server.** I went into C5
  expecting `pace=2` to give a single fair row. It produces a
  clean Aulon number, but nats-server's outbound write batching
  starves the publisher's pace check, so the run does not complete.
  This is a bench-client property, not a broker property; the
  honest reading is two rows at two pace windows in-VM and one row
  on bare metal once the bench has the headroom. Either a
  multi-process bench (separate publisher process) or
  `TCP_NODELAY` on the relevant sockets would likely close the
  asymmetry; both are bench-client redesigns and stay out of C5
  scope.
- **The C4 review's 1.39 ms Aulon p50 number was misleading.** Not
  technically wrong, it really was 1.39 ms with eviction in the
  tail, but it under-stated the C5 fix's impact: with the same
  hardware and broker, Aulon p50 drops to 29 us as soon as the
  bench client is honest. The C4 entry's "the steady-state
  distribution is much tighter than the table shows" line was
  correct but should have set the expectation more aggressively.
- **Initial `cargo deny` finding chase.** First instinct was to
  drop `wildcards = "deny"` to silence the workspace-path finding;
  the right fix was to keep the rule and tune the scope. Tuning a
  policy to match the threat it is aimed at is cheaper than
  trusting that no future contributor will introduce a published
  crate with a `*` dep.

## What's deferred?

- **Bare-metal headline.** The single-row, single-pace-window
  apples-to-apples comparison between Aulon and nats-server.
  Requires a Linux box that is not this MacBook (OrbStack is the
  wrong host for tail-latency measurement) and produces the
  promised p99.99 chart. Carry forward; the in-VM table is the
  honest interim.
- **Multi-process bench client.** Separate publisher process so
  publisher and subscribers do not contend on a single
  `tokio_uring` runtime. Independently useful: makes paced runs
  fair against nats-server, and fixes the single-runtime cap on
  achievable throughput. Probably a small `aulon-fanout-pub` /
  `aulon-fanout-sub` split.
- **HDR histogram dump + chart.** Once the multi-process bench
  exists, dump full histograms to CSV and produce a single PNG
  via gnuplot for the README headline. Until then, the table is
  enough.
- **asciinema.org upload.** Gated on explicit per-task approval.
- **`UNSUB max_msgs`.** Carry-over from C3.
- **NUMA-aware buffer-pool placement / hwloc binding.** Carry-over
  from C4.
- **Per-(src, dst) shard inbox sizing.** Carry-over from C4.
- **Subject bloom filter per shard.** Carry-over from C4.
- **Custom SQ-batching policy / SQPOLL.** Carry-over from C4.

## What changed about the plan?

- `bench/headline.sh` and `bench/fanout.sh` defaults: pace_window
  is now `2` rather than unset.
- `crates/aulon-server` and `crates/aulon-bench` are marked
  `publish = false` to keep the deny policy strict on publishable
  crates.
- `docs/MILESTONES.md` C5 row updated to reflect that the in-VM
  numeric table lands in the README and the bare-metal chart is
  the carry-forward; the C0-C4 rows already reflect their state.

## What's next?

The repo is presentable as-is. The single most leverage-positive
next step is the multi-process bench client + bare-metal headline:
together they produce the single-row apples-to-apples chart that
unlocks the C5 done condition's "one chart" framing without
caveats. Everything else (NUMA, bloom filters, SQ tuning) is
optimisation work behind that headline.
