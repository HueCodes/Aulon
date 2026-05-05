# Checkpoint 3 review

## What did we ship?

- `aulon_core::subscription` (`crates/aulon-core/src/subscription.rs:1`)
  — token-keyed wildcard trie with NATS-spec `*` and `>` semantics
  and a `queue_group: Option<Box<[u8]>>` field on `Sub`. Match is a
  single recursive descent: at each level we emit `rest` (the
  `.>`-anchored bucket), then recurse into the literal child and the
  dedicated `star` slot as parallel branches. No backtracking.
  Subscribe walks the same shape and creates nodes on demand. Unsub
  prunes empty descendants on the way back up. **23 unit tests**
  covering every wildcard edge case from the design doc.
- `aulon_core::SubjectError` — typed validation result.
  Differentiates `Empty`, `EmptyToken`, `WildcardInPublish`, and
  `InvalidGreaterPosition` so the server can emit a precise `-ERR`.
- `aulon-server` (`crates/aulon-server/src/main.rs:1`) — switched to
  `SubscriptionTrie`, validates publish subjects (rejects
  wildcards), accepts queue groups on `SUB`, and dispatches `PUB`
  through a queue-group-aware fanout: plain subscribers always
  receive; subscribers with a queue group are bucketed by group and
  exactly one is picked per publish via a per-worker xorshift64
  PRNG. The bucket data structure is two `SmallVec`s and a linear
  scan; spills to the heap only at unusually large fanouts.
- `aulon_core::benches::trie` — criterion micro-bench against a
  10,000-subscription trie.
- `docs/design/subscription-trie.md` — representation choices,
  match algorithm, queue-group dispatch, allocation discipline.

## What did we measure?

### Trie match (10k subs, OrbStack VM)

| Publish subject | Tokens | Median |
| ---: | ---: | ---: |
| `app.0` | 2 | 64.95 ns |
| `app.0.metric.cpu` | 3 | 94.05 ns |
| `app.0.svc.42` | 4 | 109.87 ns |
| `tenant.0.foo.bar.baz` | 5 | 66.69 ns |
| `tenant.0.a.b.c.d.e` | 7 | 66.34 ns |

The C3 design target was median **< 500 ns at 3-token subjects**.
We're 5× under it.

### nats CLI smoke

- `nats sub 'foo.*' --count 1` then `nats pub foo.bar` — delivered.
- `nats sub 'foo.>' --count 1` then `nats pub foo.bar.baz` —
  delivered.
- 3 subscribers in queue group `workers`, 6 publishes — total
  deliveries = 6 (no duplication, no loss). Distribution: `1/1/4`
  on this run (expected variance for random pick over 6 trials).

## What did we decide?

- **Token-keyed trie with a dedicated `*` slot and `rest` bucket
  per node.** The design doc walks through SoA vs. AoS and a few
  alternative layouts; the chosen shape keeps per-level cost to one
  hash + two pointer reads while preserving the hot-path callback
  API.
- **Match emits via callback (`for_each_match`)**, not a `Vec` —
  the trie itself does not allocate during match. The publish-path
  allocations are a per-worker `SmallVec` plus the `Box<[u8]>`
  clones per matched `Sub`. The `SmallVec`s are inline at the
  common fanouts.
- **Random pick for queue groups.** Per-worker xorshift64. The
  alternative — per-(subject, qg) round-robin counters — bloats the
  table and is not required by the NATS spec. We revisit if
  measurement shows bias matters.
- **Validation lives in the trie's public API**, not in
  `aulon-proto`. The parser stays a wire codec; subject semantics
  belong to the routing layer.
- **`UNSUB max_msgs` is still deferred.** Auto-unsub-after-N has
  its own design (per-sub remaining counter, decrement on every
  delivery, drop on zero); not required for the C3 gate.

## What did we get wrong?

- **CI fmt failure on the C2 push.** `cargo fmt --all -- --check`
  was not in my pre-push checklist for this private repo (the
  habit was OSS-PR-only). Fixed in a follow-up commit; lesson:
  every Rust repo with a fmt gate gets the same local check before
  push.
- **The first design pass had `*` and `>` stored as ordinary
  literal children keyed `b"*"` / `b">"`.** Replaced with the
  dedicated `star` slot and `rest` bucket before merge: the slot
  saves a `HashMap` probe on the hot path, and `rest` is
  semantically distinct from a literal child anyway (`>` doesn't
  consume a token in the match step).
- **`Frame::Sub`'s `queue_group` field was already plumbed by the
  C2 codec but the server emitted `-ERR queue groups not supported
  in v1`.** The codec was correct; the server was placeholder.
  C3's wiring is purely additive on the server side.

## What's deferred?

- `UNSUB max_msgs`.
- A worker-local interner for token keys (today every new token
  allocates a `Box<[u8]>`).
- Cross-worker / cross-shard subscription state — lands in C4 with
  L3-aware sharding.
- Round-robin-with-counters as an alternative to random pick. Only
  if measured fairness is a problem.

## What's next?

- **C4 — L3-aware sharding + SQ batching.** `hwloc` for topology;
  per-L3-domain workers; shared-nothing routing across cores.
  `bench/headline.sh` produces the Aulon-vs.-`nats-server` chart
  that lands in the README.
