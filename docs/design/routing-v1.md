# Routing — v1 (flat exact-match table)

## Decision

Per worker thread, maintain a flat hashmap keyed on the **exact subject
bytes**, mapping to a list of subscriber IDs:

```text
HashMap<SubjectKey, SmallVec<[SubId; 4]>>
```

A `Pub` is delivered by hashing the subject, looking up the bucket,
walking the (small) `SmallVec`, and writing one `MSG` frame per
subscriber. **No wildcard matching.** Wildcards (`*`, `>`) and queue
groups land in C3.

This is the simplest table that is right; it is the baseline against
which the C3 trie is justified.

## Why this is the right v1

- **Exact-match `Pub` is O(1) average.** The hot path is a hash + a
  fanout walk, nothing else.
- **Subscription state is per-worker, `!Send`.** Same shape as
  everything else in `aulon-core`. No cross-core coordination.
- **It exposes the hard problems early.** Even without wildcards, we
  have to solve: per-connection subscription bookkeeping, fanout into
  per-connection write buffers, backpressure when a subscriber is slow,
  cleanup on disconnect. These are real and are not made easier by
  picking a fancier data structure first.
- **Trie performance is meaningful only after a flat baseline.** The
  C3 trie's value is the wildcard support; its raw exact-match
  performance has to *match* a hashmap to be worth it. We need the
  flat baseline to compare against.

## Data shape

```text
pub struct SubscriptionTable {
    // exact-match index
    by_subject: HashMap<SubjectKey, SmallVec<[SubId; 4]>>,

    // reverse index: per-connection sid -> (subject, slot)
    by_connection: HashMap<ConnectionId, HashMap<Sid, SubjectKey>>,
}
```

`SubjectKey` is a small owned wrapper around the subject's bytes. We
intern subjects (interning = single allocation per distinct subject the
worker has ever seen) so that the hashmap key is `Cow`-free and stable
across re-subscription. v1 does the simplest thing: `Box<[u8]>`. If
subject churn becomes a measurable cost, swap for a real interner.

`ConnectionId` is a `u32` assigned by the worker on `accept`.

`Sid` is the bytes the client sent on `SUB`. Clients pick their own
sids; v1 stores them as `Box<[u8]>` keys.

## Operations

### `SUB <subject> [queue] <sid>`

1. If `queue` is set in v1 → return `-ERR queue groups not supported in
   v1` (deferred to C3).
2. Look up or create the bucket for `subject`.
3. Append `(connection_id, sid)` to the bucket's `SmallVec`.
4. Update the reverse index `by_connection[connection_id][sid] =
   subject`.

### `UNSUB <sid> [max_msgs]`

1. `max_msgs` is **not** supported in v1 (auto-unsub after N messages).
   Deferred to C3 alongside queue groups.
2. Look up `subject` in `by_connection[connection_id][sid]`.
3. Remove `(connection_id, sid)` from the bucket's `SmallVec`. If the
   bucket is now empty, remove the bucket.
4. Remove the entry from the reverse index.

### `PUB <subject> [reply] <#bytes>\r\n<payload>\r\n`

1. Look up `subject` in `by_subject`. If empty, drop the message
   (NATS-spec behaviour: publishing to a subject with no subscribers
   is silently OK).
2. For each `(connection_id, sid)` in the bucket: deliver one
   `MSG <subject> <sid> [reply] <#bytes>\r\n<payload>\r\n` to the
   subscriber's connection.
3. Self-delivery: yes by default (NATS-spec). A publisher that
   subscribes to its own subject sees its own messages.

### Connection close

On disconnect, walk `by_connection[connection_id]`, removing each
subscription from the `by_subject` table. Remove the connection's
entry from `by_connection`.

## Fanout: how `MSG` actually goes out

This is the hard part of C2 — not the parse, not the lookup, but the
**delivery**.

For each delivery, the fanout path needs:

1. A registered fixed buffer to write the encoded `MSG` frame into.
   Drawn from the same per-core `BufferPool` introduced in C1.
2. A handle to the subscriber's `Connection<Active>` (or some send-side
   primitive we expose from `aulon-core`) so the bytes can be queued
   for `write_fixed_all`.
3. Backpressure on slow consumers — if a subscriber's write queue is
   full, we either drop the message (NATS pre-JetStream behaviour) or
   close the slow consumer with `-ERR slow consumer`. v1 picks the
   second; matches `nats-server`'s default and is simpler to reason
   about.

The fanout layer's API design lands in `aulon-core` during C2's
implementation phase; it is sketched here only so the routing layer's
contract is visible.

## What we are *not* doing in v1

- Wildcards (`*`, `>`).
- Queue groups (load-balanced delivery).
- `UNSUB max_msgs`.
- `Connect` JSON option handling (`verbose`, `pedantic`,
  `tls_required`, `auth_token`, etc.). v1 accepts `CONNECT` and
  ignores its body; clients that require `+OK` echoes will get them
  because the v1 default is `verbose=false`-equivalent (no `+OK` for
  `PUB` / `SUB` / `UNSUB`).
- TLS, auth, headers (`HMSG` / `HPUB`).

All of these are out of `docs/SCOPE.md` for v1 (or land in C3).

## Failure modes and limits

- **Hash table churn.** Exact-match keys are cheap to allocate
  (`Box<[u8]>`) but allocation under load is bad. Mitigation: when a
  client subscribes, intern the subject; when the bucket is dropped,
  drop the interned key. v1's churn is bounded by client behaviour;
  measure in C2 and revisit if needed.
- **Slow consumer.** Detection: per-connection write queue depth
  exceeds a configured threshold (default: 64 MiB pending). Action:
  send `-ERR slow consumer` and close the connection.
- **Subject explosion.** No defence in v1. C3's trie helps for
  wildcards; C4's L3-aware sharding partitions the table across cores.

## Measurement

`bench/fanout.sh` (added in C2): pin one publisher and N subscribers
to one core; publish 1M messages; measure end-to-end p99.99 delivery
latency as a function of N. Plot in `PERFORMANCE.md`.

The number to watch is **fanout cost as a function of subscriber
count**. The flat table is O(N) per publish where N is the number of
matching subscribers; C3's trie is also O(N) on the subscriber count
but adds wildcards. Per-publish cost should be linear with no surprise
constants.

## Carry-forward to C3

- Wildcards (`*`, `>`) — replace `HashMap` with a radix/trie data
  structure.
- Queue groups — add a "group" axis to bucket entries; pick one
  subscriber per group via round-robin or random.
- `UNSUB max_msgs` — per-subscription remaining-count.
- `Connect` JSON — selectively parse the fields we care about
  (`verbose`, `pedantic`, `name`).
