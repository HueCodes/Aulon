# Subscription trie + queue groups (C3)

## Decision

Subscription state moves from a flat `HashMap<Subject, Vec<Sub>>` to a
**per-token wildcard trie** keyed on dot-separated tokens. The trie
supports the two NATS-spec wildcards (`*` for one token, `>` for the
remainder) and queue-group dispatch (`SUB foo qgroup sid`).

The publish path stays allocation-free. Match emits subscribers
through a callback rather than collecting into a `Vec`.

## Why a trie

The flat hashmap is O(1) for exact subjects but cannot match wildcards
without scanning every key in the table. Real NATS workloads put a
lot of subscriptions behind wildcards (e.g. `events.user.*` per
service), so wildcard match has to be on the hot path, not the slow
path.

A token-keyed trie matches a publish subject in **O(depth)** trie
walks plus the wildcard branches at each level. Trie depth is the
subject's token count, almost always ≤ 6 in real workloads. At each
level we do at most three lookups: the literal token, the `*` child,
and a check for any `>` subscribers anchored at this node. That's a
fixed small constant per level.

The flat hashmap stays as the **inner storage at each level** —
`HashMap<token, Box<Node>>` — so per-level cost is one hash plus the
two specialised slot reads.

## Shape

```rust
struct Node {
    /// Subscribers whose subscription path ends exactly at this node.
    exact: SmallVec<[Sub; 4]>,

    /// Subscribers anchored with `.>` at this node — they match any
    /// subject reaching this node *and* having at least one more
    /// token. Stored at the parent of where `>` would be a child.
    rest: SmallVec<[Sub; 4]>,

    /// Children keyed on a literal token. The `*` child lives in the
    /// dedicated `star` slot, not in this map.
    children: HashMap<Box<[u8]>, Box<Node>>,

    /// Dedicated slot for the `*` (single-token wildcard) child.
    /// Hot-path read avoids a HashMap probe.
    star: Option<Box<Node>>,
}

pub struct Sub {
    pub conn_id: ConnectionId,
    pub sid: Box<[u8]>,
    pub queue_group: Option<Box<[u8]>>,
}
```

`Subscription paths`:

- `foo.bar` → walk `foo` → `bar`, push `Sub` into `exact` of `bar`'s
  node.
- `foo.*` → walk `foo`, create or fetch the `star` child, push `Sub`
  into its `exact`.
- `foo.>` → walk `foo`, push `Sub` into `rest` of `foo`'s node. (No
  child node is created for `>`; it's a property of the parent.)
- `>` → push `Sub` into `rest` of the root.

## Matching

```text
match(subject):
    walk(node = root, tokens = split(subject), depth = 0)

walk(node, tokens, depth):
    if depth == tokens.len():
        emit each `sub` in node.exact
        return
    # Subscribers anchored with `.>` at the current node match any
    # remaining suffix of length >= 1. We are about to consume one
    # more token, so they match.
    emit each `sub` in node.rest
    if let Some(child) = node.children.get(tokens[depth]):
        walk(child, tokens, depth + 1)
    if let Some(star) = node.star:
        walk(star, tokens, depth + 1)
```

That's three reads per level (`rest`, `children`, `star`) plus the
recursion for any wildcard branch that exists. There is no
backtracking — `*` and the literal child are independent walks down
parallel branches.

## Queue groups

A subscription with `queue_group = Some(g)` participates in a
load-balanced group named `g`. Subscriptions with `queue_group =
None` always receive every match.

We do **not** group at the trie level. The trie emits all matching
`Sub`s through the callback; the server's publish dispatch does the
grouping:

```rust
let mut by_qg: HashMap<&[u8], SmallVec<[&Sub; 4]>> = HashMap::new();
let mut plain: SmallVec<[&Sub; 4]> = SmallVec::new();
trie.for_each_match(subject, |sub| {
    match &sub.queue_group {
        Some(g) => by_qg.entry(g).or_default().push(sub),
        None => plain.push(sub),
    }
});
for sub in plain { deliver(sub); }
for group in by_qg.values() {
    let pick = rng.gen_range(0..group.len());
    deliver(group[pick]);
}
```

Random pick (per-worker `Xoshiro256++`) is the simplest fair
distribution and matches what `nats-server` does on a default
configuration. Round-robin is documented in the NATS spec but not
required, and per-(subject, qg) RR counters bloat the table. We
revisit if measurement says randomness is biased in practice.

The grouping itself uses two `SmallVec`s and a small `HashMap`. **The
grouping allocates** — this is the cost we accept for queue groups
in v1. If `cargo flamegraph` shows it on the hot path, we move to a
`SmallVec<[(Option<&[u8]>, &Sub); N]>` with manual lookup; not worth
doing pre-emptively.

## Subject + token validation

Subscriber side (`SUB <subject> ...`):
- Empty subject: rejected.
- Empty token (leading `.`, trailing `.`, `..`): rejected.
- A `>` token must be the **last** token: `foo.>` is valid; `foo.>.bar`
  is rejected.
- `*` and `>` are wildcards **only when they are the entire token**.
  `foo.*` is wildcard; `foo.b*r` is the literal three-byte token
  `b*r`. NATS-spec compliant.

Publisher side (`PUB <subject> ...`):
- Empty subject and empty tokens: rejected as before.
- `*` or `>` tokens in publish subjects: rejected. Publishers cannot
  publish to a wildcard.

The validation lives in `aulon-proto::subject` (new module),
returning a typed `SubjectKind::{Literal, Wildcard}` — the parser
already classifies subjects but C3 lifts that into a real type.

## Allocation discipline

The publish path remains alloc-free **per match**: the
`for_each_match` callback receives `&Sub` and does not collect.

The single allocation we accept on the publish path is the queue-group
grouping `HashMap`, scoped per-call. A worker-local `RefCell<HashMap>`
that we `.clear()` between publishes keeps the allocation count at
zero in steady state.

The subscribe path allocates one `Box<Node>` per new trie level (rare
after warm-up), one `Box<[u8]>` per new token (interned), and one
`Sub` per `SUB`. Not on the hot path.

## Operations summary

### `SUB <subject> [queue_group] <sid>`

1. Validate `subject` (token shape, wildcard placement). Reject with
   `-ERR invalid subject` on failure.
2. Walk the trie, creating nodes as needed.
3. Push `Sub { conn_id, sid, queue_group }` into the target node's
   `exact` (or the parent's `rest` for a `.>` anchor).
4. Update the reverse index `by_connection[conn_id][sid] =
   parsed_subject`.

### `UNSUB <sid> [max_msgs]`

1. `max_msgs` is **still not** supported in v1; we return `-ERR
   UNSUB max_msgs not supported`. Auto-unsub-after-N is a separate
   feature with its own design (counter per sub, decrement on every
   delivery, drop on zero); deferred.
2. Look up `subject` in `by_connection[conn_id][sid]`.
3. Walk the trie, removing the `Sub` entry. If the target node and
   all its descendants are empty after removal, prune.

### `PUB <subject>`

1. Validate `subject` (no wildcards allowed).
2. Run match on the trie.
3. Group + dispatch (above).

### Connection close

Walk `by_connection[conn_id]` and remove each subscription from the
trie. Drop the connection's entry.

## Carry-forward to C4

The trie's worker-local layout is preserved in C4 — sharding happens
*above* it. Each L3-domain worker owns its own trie; cross-shard
publishes are forwarded via a routing primitive. The trie itself is
unaware of sharding.

## Carry-forward to C5

`UNSUB max_msgs` is the obvious pickup. JetStream-style durable
consumers are out of scope for the broker's v1.

## Measurement

Criterion micro-bench in `aulon-core::benches::trie`:
- Build a trie with **10,000 subscriptions** distributed across a
  realistic shape: 7000 exact, 2500 `*`, 500 `>`.
- Measure `for_each_match` for representative subjects: 2-token, 3-
  token, 5-token, and a token that hits both a literal and a `*`
  branch.
- Target: median match cost **< 500 ns** at 3-token subjects on the
  OrbStack VM.

The exact-match cost on the trie should be ≈ the flat hashmap cost
plus one per-level wildcard slot read. The wildcard cost is the cost
of being able to do wildcards at all.

## What we are *not* doing in C3

- Subject interning across workers (each worker owns its own tree;
  cross-core sharing lands in C4).
- Auto-unsub via `UNSUB max_msgs`.
- `HMSG` / `HPUB` (NATS headers).
- `CONNECT` body handling beyond `verbose=false`-equivalent.
