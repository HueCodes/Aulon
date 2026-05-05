# Fanout

## Decision

For each accepted TCP connection, the server spawns **two `tokio_uring`
tasks**:

- A **reader task** that owns the inbound `FixedBuf` and the parser state.
  It runs `read_fixed` in a straight-line loop, never cancelling.
- A **writer task** that owns an outbound `FixedBuf` and a per-connection
  byte-stream outbound buffer. It runs `write_fixed_all` in a straight-
  line loop, never cancelling.

Cross-task communication is **not** an `mpsc` channel. Each connection
owns a pre-allocated `Box<[u8]>` ring; publishers `copy_from_slice` into
it and bump a `Cell<usize>` tail; the writer reads from a `Cell<usize>`
head. A `tokio::sync::Notify` is the cross-task wakeup primitive.

The publish hot path performs **no heap allocation**. This is a hard
property of the design, not an aspiration.

## Why two tasks (not single-task `select!`)

Tokio-uring's `read_fixed` is technically cancel-safe — dropping the
in-flight future submits `IORING_OP_ASYNC_CANCEL` and detaches the
buffer until the cancel completes. But:

1. The bytes already read on the cancelled op are **discarded**. Any
   `select!` that loses to the channel branch wastes the partial read.
2. The buffer is unusable for the duration of the cancel completion,
   adding tail-latency under load.
3. Each cancel burns an SQE.

A `select!` loop that wakes whenever an outbound message arrives would
cancel-and-resubmit reads constantly. That is the wrong shape for a
completion-based I/O model — completion-based runtimes want cooperative
tasks, not cancel-driven multiplexing.

Two tasks read top-to-bottom, never cancel each other, and own their
buffers for the full connection lifetime. The cost is one extra
`tokio_uring::Task` per connection (~200 bytes overhead), which is
trivial against the latency wins.

## Why a byte-stream outbound buffer (not an mpsc of `Rc<Vec<u8>>`)

The natural reach for "fanout to N subscribers" is an `mpsc::Sender<T>`
per subscriber and `T = Rc<Vec<u8>>` shared across all sends. That
allocates per published message (one `Vec<u8>`, one `Rc`). The working
agreement (`docs/PROMPT.md`) forbids hot-path allocation, and we mean
that literally.

The byte-stream design moves all allocation to startup:

- Per worker: one `encode_scratch: Vec<u8>` reused across publishes via
  `clear()`. Capacity is preserved.
- Per connection: one `outbound_data: Box<[u8]>` allocated when the
  connection is accepted. Default 256 KiB per connection. Sized to
  cover the realistic backlog of a healthy subscriber.
- Per worker: one `tokio::sync::Notify` per connection. `Notify` does
  not allocate per call.

A publish encodes once into `encode_scratch`, then for each subscriber
copies the bytes into their `outbound_data` ring (advancing `tail`).
That is the only data motion. There is no per-publish `Vec`, no `Rc`,
no `Box`.

## Layout

```text
Per worker:
    pool:           BufferPool                  // existing, registered with io_uring
    encode_scratch: Vec<u8>                     // grow once, clear-and-reuse
    connections:    Slab<ConnectionState>       // indexed by ConnectionId
    table:          SubscriptionTable           // exact-match flat hashmap (routing-v1.md)

Per connection (ConnectionState):
    outbound_data:  Box<[u8]>                   // pre-allocated, default 256 KiB
    outbound_head:  Cell<usize>                 // writer's read cursor
    outbound_tail:  Cell<usize>                 // publishers' write cursor
    notify:         Rc<Notify>                  // wake the writer on enqueue / close
    close_with:     Cell<Option<CloseReason>>   // SlowConsumer / ProtocolError
    id:             ConnectionId
```

`Cell<usize>` is sound here because the worker is `!Send`: head and
tail are mutated by different tasks but on the same thread, so no
ordering or visibility hazard arises. The `Cell` is documentation for
the reader, nothing more.

## Publish path (allocation-free)

```text
fn publish(worker, subject_bytes, reply_to, payload_bytes):
    1. worker.encode_scratch.clear()
    2. emit_msg_into(&mut worker.encode_scratch, subject_bytes, sid, reply_to, payload_bytes)
    3. for sub in worker.table.subscribers_of(subject_bytes):
        let conn = &worker.connections[sub.conn_id]
        let needed = worker.encode_scratch.len()
        let pending = conn.outbound_tail.get() - conn.outbound_head.get()
        if pending + needed > conn.outbound_data.len():
            conn.close_with.set(Some(CloseReason::SlowConsumer))
            conn.notify.notify_one()
            continue
        let tail = conn.outbound_tail.get()
        conn.outbound_data[tail..tail+needed].copy_from_slice(&worker.encode_scratch)
        conn.outbound_tail.set(tail + needed)
        conn.notify.notify_one()
```

The only steps that touch memory are `clear`, `emit_msg_into` (writes
into the existing `Vec`), and `copy_from_slice` (writes into the existing
`Box<[u8]>`). No allocator is involved.

To keep the byte-stream model simple, **the outbound buffer does not
wrap**. When `tail` reaches `outbound_data.len()`, the writer drains
the entire backlog in a single `write_fixed_all` and resets both head
and tail to 0. Wrapping would buy slightly more headroom at the cost
of two-segment writes; not worth it for v1.

(See "Wrap or reset?" below for when this becomes interesting.)

## Writer task

```text
loop:
    conn.notify.notified().await

    if let Some(reason) = conn.close_with.take():
        emit -ERR into write buf
        write_fixed_all
        return

    let head = conn.outbound_head.get()
    let tail = conn.outbound_tail.get()
    if head == tail: continue

    let pending = &conn.outbound_data[head..tail]
    copy pending into write_buf
    write_fixed_all(&write_buf[..pending.len()])

    on completion:
        if head + written == tail:
            conn.outbound_head.set(0)
            conn.outbound_tail.set(0)
        else:
            conn.outbound_head.set(head + written)
```

The writer never cancels. Notifies that arrive while a `write_fixed_all`
is in flight coalesce into the existing `Notify` permit; the next
iteration drains everything that accumulated.

## Slow-consumer policy

`close_with` set to `SlowConsumer` is the only signal that ever closes
a connection from the publisher's side. The reader task may also set
`close_with = ProtocolError` when it sees malformed input. In both
cases the writer is responsible for emitting a final `-ERR` frame and
exiting.

The default outbound buffer size is **256 KiB**, which holds ~64 × 4 KiB
messages. Tunable per-connection at startup; not configurable per-CONNECT
in v1.

## "Wrap or reset?"

Resetting head and tail to 0 only when `head == tail` means a steady
stream of small messages can keep `tail` advancing toward
`outbound_data.len()` without the writer ever catching up. The window
of safe headroom shrinks over time. Wrapping (a true ring buffer)
avoids this but requires two-segment writes when the data straddles
the wrap point.

For v1, **reset on drain** is the chosen policy — it is dramatically
simpler and the default 256 KiB sizing gives generous headroom. If
production load shows the buffer pressing the high-water mark, swap
in a ring with two-segment writes. This is the kind of decision worth
revisiting with measurement, not preemptively.

## Failure modes

- **Subject explosion → publisher fanout walks a long bucket.** O(N) in
  the subscriber count for that subject; bounded by the table size.
  C3's trie helps for wildcard subscriptions, not for the per-subject
  bucket walk.
- **Slow consumer.** Detected by the publisher seeing a full outbound
  buffer; marked, the writer task delivers `-ERR slow consumer` and
  closes. The publisher continues with the remaining subscribers.
- **Publisher disconnect mid-PUB.** Reader task observes EOF, drops its
  parse state, removes the connection from the table on cleanup. Any
  outbound buffer bytes already written are still delivered by the
  writer task before it observes the connection has been removed and
  exits.
- **`Notify` lost wakeup.** `Notify::notify_one` is idempotent under
  concurrent `notified()` polls; a notify that arrives between
  `notified()` returning and the writer re-awaiting is held and
  delivered to the next `notified()`. No lost wakeups.

## What this does *not* do

- **No multi-segment outbound ring.** Reset-on-drain only.
- **No batched fanout.** Each PUB walks subscribers serially. Batching
  multiple PUBs into one fanout pass is a C4 optimisation.
- **No priority lanes.** Every subscriber is equal. Per-tenant or per-
  priority queues are out of scope.
- **No cross-core fanout.** Subscribers are per-worker; in C4, when
  workers shard the subject space, cross-shard publish becomes a
  separate problem with its own design doc.

## Carry-forward

- Wrap-style ring with two-segment writes (if measurement demands it).
- Coalesced fanout pass (multiple PUBs encoded once if the same
  subscriber is present on multiple subject buckets — unlikely common
  case, but worth measuring).
- Adaptive outbound buffer sizing per connection based on observed
  throughput. v1 is fixed-size.
