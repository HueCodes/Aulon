# Design decisions: index

Each entry below names the decision and points to the doc that records the
rejected alternatives, the measurement plan, and (where relevant) the
numbers that justified it. Documents are ordered by the checkpoint that
introduced them; a structural change is expected to update the relevant
doc rather than spawn a new one.

For the working agreement that governs how these decisions get made, see
[`../PROMPT.md`](../PROMPT.md). For the build sequence, see
[`../MILESTONES.md`](../MILESTONES.md). For the v1 compatibility matrix,
see [`../SCOPE.md`](../SCOPE.md).

## C1: Runtime, buffers, connection lifecycle

- [`runtime.md`](runtime.md): **`tokio-uring` over Monoio.** The only
  Rust runtime exposing `IORING_REGISTER_BUFFERS`, `FixedBufPool`, and
  `read_fixed` / `write_fixed_all` in its public API. Includes the
  Monoio attempt and why it was reversed.
- [`buffer-pool.md`](buffer-pool.md): **Per-core wrapper over
  `FixedBufPool`.** Sizing, capacity, and the registration step pinned
  in one named place. No global pool, no cross-core migration.
- [`connection-lifecycle.md`](connection-lifecycle.md): **Typestate
  for connection states.** `Unconnected → Negotiating → Active →
  Closing → Closed`. Calling `write` before reaching `Active` is a
  compile error.

## C2: Wire protocol and routing v1

- [`wire-codec.md`](wire-codec.md): **Allocation-free, `#![no_std]`-
  clean parser** over borrowed byte slices. `Frame<'a>` holds
  `&'a [u8]`. `ParseOutcome::NeedMore` for streaming. Fuzzed and
  proptested.
- [`routing-v1.md`](routing-v1.md): **Flat exact-match
  `HashMap<SubjectKey, SmallVec<[SubId; 4]>>`.** The simplest table
  that's right; the baseline against which the C3 trie is justified.

## C3: Wildcard trie, queue groups, fanout

- [`subscription-trie.md`](subscription-trie.md): **Per-token wildcard
  trie** with `*` and `>` plus queue-group dispatch. Match emits
  through a callback rather than collecting into a `Vec`. Publish
  path stays alloc-free.
- [`fanout.md`](fanout.md): **Two `tokio_uring` tasks per
  connection** (reader + writer) with a pre-allocated per-connection
  `Box<[u8]>` ring instead of an `mpsc` channel. `tokio::sync::Notify`
  for wakeups. Hard property: zero heap allocation on the publish
  hot path.

## C4: Topology and SQ batching

- [`topology-sharding.md`](topology-sharding.md): **One worker per L3
  cache domain.** Sysfs-driven discovery, `SO_REUSEPORT` per shard,
  connection sharding with every-shard PUB fan-out. Cross-shard hop
  is a bounded lock-free MPSC inbox + `eventfd` wake. One
  `Arc<PublishedFrame>` per cross-shard PUB; single-shard hosts hit
  the alloc-free C3 path.
- [`sq-batching.md`](sq-batching.md): **Default `tokio-uring` 0.5
  submission policy.** The byte-stream outbound buffer plus per-yield
  SQ batching already amortise to 20 deliveries per server syscall.
  No custom batch driver in v1; carry-forward documented if the
  measurement changes.

## Cross-cutting

- [`dev-environment.md`](dev-environment.md): **Linux-only by
  construction** (`io_uring` has no cross-platform fallback). macOS
  host + OrbStack VM for daily development; bare metal for headline
  benchmarks.

## Reviews

End-of-checkpoint reviews answer: what shipped, what was measured,
what was decided, what was wrong, what's deferred, what changed about
the plan. See [`../reviews/`](../reviews/).

## War stories

Long-form post-mortems on the most interesting bugs and design
reversals. See [`../war-stories/`](../war-stories/). Current entries:

- [`loom-tokio-cfg.md`](../war-stories/loom-tokio-cfg.md): the
  `RUSTFLAGS="--cfg loom"` global-cfg interaction with `tokio`'s
  internal loom integration that broke `tokio-uring`'s build, and
  the target-cfg dependency split that fixed it.
