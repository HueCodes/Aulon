//! Aulon broker server binary.
//!
//! Thread-per-L3-cache-domain. Each worker pins its OS thread to one
//! of the L3's logical CPUs, drives its own `tokio_uring` runtime, and
//! binds an `SO_REUSEPORT` listener so the kernel distributes accepts.
//! Each accepted connection runs as two cooperative tasks (reader and
//! writer) sharing an `Rc<ConnectionState>`. The publish hot path
//! performs no heap allocation on the local-fanout path; cross-shard
//! delivery uses a single `Arc<PublishedFrame>` per `PUB`.
//!
//! See `docs/design/topology-sharding.md` for the multi-worker
//! architecture and `docs/design/fanout.md` for the per-connection
//! task topology.

#![forbid(unsafe_code)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use aulon_core::{
    eventfd_pair, BufferPool, CloseReason, ConnectionId, ConnectionState, EventfdReader,
    EventfdWaker, PublishedFrame, ShardInbox, Sub, SubscriptionTrie, DEFAULT_BUFFER_SIZE,
    DEFAULT_OUTBOUND_CAPACITY, DEFAULT_POOL_CAPACITY,
};
use aulon_proto::{emit_err, emit_info, emit_msg, emit_pong, parse_frame, Frame, ParseOutcome};
use smallvec::SmallVec;
use socket2::{Domain, Protocol, Socket, Type};
use tokio_uring::buf::fixed::FixedBuf;
use tokio_uring::buf::BoundedBuf;
use tokio_uring::net::{TcpListener, TcpStream};

const LISTEN_ADDR: &str = "0.0.0.0:4222";

/// Maximum payload size we accept and advertise. Matches `nats-server`'s
/// default; the `INFO` greeting advertises this number.
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Maximum total frame size on the read accumulator before we reject the
/// connection with a protocol error. A publisher's `PUB` is the largest
/// possible frame: `PUB <subject> [reply] <#bytes>\r\n<payload>\r\n`.
/// Cap at 2 × `MAX_PAYLOAD_BYTES` so the header has generous headroom
/// and no malicious client can grow the accumulator without bound.
const MAX_FRAME_SIZE: usize = MAX_PAYLOAD_BYTES * 2;

/// Per-worker heap-allocated emit scratch buffer. Sized to fit one
/// encoded `MSG` at the maximum payload, plus header. One allocation
/// at startup; reused for the lifetime of the worker.
const EMIT_SCRATCH_CAPACITY: usize = MAX_PAYLOAD_BYTES + 4096;

/// Default per-shard cross-shard inbox capacity (in `Arc<PublishedFrame>`
/// slots). At the headline-bench rate (>1M PUB/s) a 4096-deep inbox
/// holds several milliseconds of buffering — comfortably above the
/// per-publish jitter we expect.
const SHARD_INBOX_CAPACITY: usize = 4096;

/// Minimal `INFO` greeting body. Real `nats-server` includes more fields
/// (`server_id`, version, etc.); the official `nats` CLI is forgiving as
/// long as the JSON is well-formed and the protocol version is set.
const SERVER_INFO_JSON: &[u8] = br#"{"server_id":"aulon","server_name":"aulon","version":"0.0.1","host":"127.0.0.1","port":4222,"max_payload":1048576,"proto":1,"headers":false,"jetstream":false}"#;

/// Stack buffer size for emitting short control frames (INFO, PONG,
/// `-ERR`). Distinct from the per-worker MSG scratch (which is heap-
/// allocated and large enough for the full `MAX_PAYLOAD_BYTES`).
const SHORT_FRAME_BUF: usize = 4096;

/// One match collected for the queue-group dispatch step. Borrowed
/// from the trie's `Sub` only long enough to clone the few bytes we
/// need (`sid` and `queue_group`), so the trie's `RefCell` borrow is
/// released before any `enqueue` call.
type Snapshot = (ConnectionId, Box<[u8]>);

/// One queue-group bucket: the group name plus its candidate
/// subscribers for the current publish.
type GroupBucket = (Box<[u8]>, SmallVec<[Snapshot; PUB_INLINE_GROUP]>);

/// Inline capacity for the per-PUB plain-subscriber list. 8 covers
/// the common case (small fanouts); larger fanouts spill to the heap.
const PUB_INLINE_PLAIN: usize = 8;
/// Inline capacity for one queue-group bucket.
const PUB_INLINE_GROUP: usize = 4;
/// Inline capacity for the list of distinct queue groups on a single
/// publish. Linear scan is faster than a `HashMap` while this stays
/// small, which it does in real workloads.
const PUB_INLINE_GROUPS: usize = 4;

/// Cross-shard inbox carries owned `Arc<PublishedFrame>` from peer
/// shards' publishers to this shard's drain task.
type ShardFanoutInbox = ShardInbox<Arc<PublishedFrame>>;

/// Per-worker shared state. The single `tokio_uring` thread per shard
/// owns one instance; reader and writer tasks hold an `Rc<Worker>`.
struct Worker {
    /// Stable shard id assigned by the topology pass. Used to identify
    /// our own slot in `peer_inboxes` so cross-shard fan-out skips it.
    shard_id: u32,
    pool: BufferPool,
    table: RefCell<SubscriptionTrie>,
    connections: RefCell<HashMap<ConnectionId, Rc<ConnectionState>>>,
    next_conn_id: Cell<u32>,
    /// Pre-allocated MSG-encoding scratch. Heap-allocated once at
    /// startup; resized to its capacity so direct slice indexing is
    /// always in bounds. Each `PUB` fanout writes the encoded MSG
    /// into this buffer once per delivery. No allocator is touched.
    emit_scratch: RefCell<Vec<u8>>,
    /// xorshift64 PRNG state for queue-group dispatch. Per-worker so
    /// the picks are independent across shards. Seeded with a non-zero
    /// constant XOR'd with the shard id; entropy quality is not
    /// load-bearing — fairness within a worker is.
    rng: Cell<u64>,
    /// One inbox per shard in the running broker, including ours.
    /// Indexed by `shard_id`. Cross-shard PUB pushes into every peer's
    /// inbox; our own slot is read by the drain task on this thread.
    peer_inboxes: Arc<[Arc<ShardFanoutInbox>]>,
    /// One `eventfd` waker per shard in the running broker, paired
    /// with `peer_inboxes[i]`. After a non-empty cross-shard push we
    /// kick the peer so its drain task wakes.
    peer_wakers: Arc<[Arc<EventfdWaker>]>,
}

impl Worker {
    fn new(
        shard_id: u32,
        peer_inboxes: Arc<[Arc<ShardFanoutInbox>]>,
        peer_wakers: Arc<[Arc<EventfdWaker>]>,
    ) -> Self {
        let emit_scratch = vec![0u8; EMIT_SCRATCH_CAPACITY];
        let seed = 0x1234_5678_9abc_def0_u64 ^ u64::from(shard_id).wrapping_mul(0x9E37_79B9);
        Self {
            shard_id,
            pool: BufferPool::new(DEFAULT_POOL_CAPACITY, DEFAULT_BUFFER_SIZE),
            table: RefCell::new(SubscriptionTrie::new()),
            connections: RefCell::new(HashMap::new()),
            next_conn_id: Cell::new(0),
            emit_scratch: RefCell::new(emit_scratch),
            rng: Cell::new(seed),
            peer_inboxes,
            peer_wakers,
        }
    }

    fn shard_count(&self) -> usize {
        self.peer_inboxes.len()
    }

    fn next_id(&self) -> ConnectionId {
        let id = self.next_conn_id.get();
        self.next_conn_id.set(id.wrapping_add(1));
        ConnectionId::new(id)
    }

    fn lookup(&self, id: ConnectionId) -> Option<Rc<ConnectionState>> {
        self.connections.borrow().get(&id).cloned()
    }

    /// Picks an index in `0..n` using the worker's xorshift64 stream.
    /// `n` must be non-zero; the queue-group dispatch only calls this
    /// for non-empty buckets.
    fn pick_index(&self, n: usize) -> usize {
        debug_assert!(n > 0);
        let mut s = self.rng.get();
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.rng.set(s);
        // The mod-n result fits in usize on every supported target;
        // truncation here is the desired narrowing of a 64-bit
        // xorshift output into an index, not a precision loss.
        #[allow(clippy::cast_possible_truncation)]
        let truncated = s as usize;
        truncated % n
    }
}

fn main() -> std::io::Result<()> {
    let topology = match std::env::var("AULON_FORCE_SHARDS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|n| *n > 0)
    {
        Some(forced) => {
            eprintln!("aulon-server: AULON_FORCE_SHARDS={forced} (topology override)");
            synthetic_topology(forced)
        }
        None => aulon_core::topology::detect(),
    };
    let shard_count = topology.shard_count();
    eprintln!(
        "aulon-server: discovered {shard_count} L3 shard{}",
        if shard_count == 1 { "" } else { "s" }
    );

    // Per-shard cross-shard inbox + eventfd pair. The reader half of
    // each eventfd stays on its shard; the waker half is shared across
    // all shards via `peer_wakers` so any publisher can kick any peer.
    let mut inbox_vec: Vec<Arc<ShardFanoutInbox>> = Vec::with_capacity(shard_count);
    let mut waker_vec: Vec<Arc<EventfdWaker>> = Vec::with_capacity(shard_count);
    let mut readers: Vec<Option<EventfdReader>> = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let (reader, waker) = eventfd_pair()?;
        inbox_vec.push(Arc::new(ShardInbox::with_capacity(SHARD_INBOX_CAPACITY)));
        waker_vec.push(Arc::new(waker));
        readers.push(Some(reader));
    }
    let inboxes: Arc<[Arc<ShardFanoutInbox>]> = inbox_vec.into();
    let wakers: Arc<[Arc<EventfdWaker>]> = waker_vec.into();

    if shard_count == 1 {
        // Common dev case: skip the OS-thread spawn so logs and panics
        // surface on the main thread.
        let shard = topology
            .shards
            .into_iter()
            .next()
            .expect("shard_count == 1 implies one entry");
        let reader = readers[0]
            .take()
            .expect("reader[0] populated above for shard 0");
        return worker_main(
            shard.shard_id,
            shard.cpus.first().copied(),
            inboxes,
            wakers,
            reader,
        );
    }

    let mut handles = Vec::with_capacity(shard_count);
    for shard in topology.shards {
        let inboxes = Arc::clone(&inboxes);
        let wakers = Arc::clone(&wakers);
        let cpu_id = shard.cpus.first().copied();
        let shard_id = shard.shard_id;
        let reader = readers[shard_id as usize]
            .take()
            .expect("each shard's reader populated above");
        let handle = std::thread::Builder::new()
            .name(format!("aulon-shard-{shard_id}"))
            .spawn(move || worker_main(shard_id, cpu_id, inboxes, wakers, reader))
            .expect("spawn shard thread");
        handles.push(handle);
    }
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("aulon-server: shard thread exited: {e}"),
            Err(e) => eprintln!("aulon-server: shard thread panicked: {e:?}"),
        }
    }
    Ok(())
}

/// Per-shard worker entry point: pins the thread, starts the
/// `tokio_uring` runtime, opens an `SO_REUSEPORT` listener on
/// [`LISTEN_ADDR`], spawns the cross-shard drain task (when
/// `shard_count > 1`), and runs the accept loop.
fn worker_main(
    shard_id: u32,
    cpu_id: Option<u32>,
    inboxes: Arc<[Arc<ShardFanoutInbox>]>,
    wakers: Arc<[Arc<EventfdWaker>]>,
    reader: EventfdReader,
) -> std::io::Result<()> {
    if let Some(cpu) = cpu_id {
        let core = core_affinity::CoreId { id: cpu as usize };
        if !core_affinity::set_for_current(core) {
            eprintln!(
                "aulon-shard-{shard_id}: warning: failed to pin thread to CPU {cpu}; continuing unpinned"
            );
        }
    }

    tokio_uring::start(async move {
        let addr: SocketAddr = LISTEN_ADDR
            .parse()
            .expect("LISTEN_ADDR is a valid socket address literal");
        let listener = bind_so_reuseport(addr)?;
        let worker = Rc::new(Worker::new(shard_id, inboxes, wakers));
        worker.pool.register()?;
        let shard_count = worker.shard_count();
        eprintln!(
            "aulon-shard-{shard_id}: listening on {addr} (NATS-core, pool {DEFAULT_POOL_CAPACITY} x {DEFAULT_BUFFER_SIZE} bytes, IORING_REGISTER_BUFFERS)"
        );
        if shard_count > 1 {
            tokio_uring::spawn(drain_task(Rc::clone(&worker), reader.into_uring_file()));
        } else {
            // Single-shard: the cross-shard inbox is unused and we drop
            // the reader so its underlying fd is released.
            drop(reader);
        }
        loop {
            let (stream, peer) = listener.accept().await?;
            eprintln!("aulon-shard-{shard_id}: accepted {peer}");
            let id = worker.next_id();
            let Some(read_buf) = worker.pool.acquire() else {
                eprintln!("aulon-shard-{shard_id}: pool exhausted (read), dropping {peer}");
                continue;
            };
            let Some(write_buf) = worker.pool.acquire() else {
                eprintln!("aulon-shard-{shard_id}: pool exhausted (write), dropping {peer}");
                drop(read_buf);
                continue;
            };
            let state = Rc::new(ConnectionState::new(id, DEFAULT_OUTBOUND_CAPACITY));
            worker
                .connections
                .borrow_mut()
                .insert(id, Rc::clone(&state));

            let stream = Rc::new(stream);
            tokio_uring::spawn(writer_task(
                Rc::clone(&stream),
                Rc::clone(&state),
                write_buf,
            ));
            tokio_uring::spawn(reader_task(stream, state, read_buf, Rc::clone(&worker)));
        }
    })
}

/// Build a synthetic [`aulon_core::Topology`] with `n` shards, each
/// hosting one logical CPU (`0..n`). Used by the `AULON_FORCE_SHARDS`
/// override to exercise the multi-worker code path on single-L3
/// hardware. Real deployments use [`aulon_core::topology::detect`].
fn synthetic_topology(n: u32) -> aulon_core::Topology {
    let shards = (0..n)
        .map(|i| aulon_core::Shard {
            shard_id: i,
            cpus: vec![i],
        })
        .collect();
    aulon_core::Topology { shards }
}

/// Creates a TCP listener with `SO_REUSEPORT` (and `SO_REUSEADDR`) set
/// so multiple shards can bind the same address; the kernel
/// distributes accepted connections across the shards.
fn bind_so_reuseport(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let std_listener: std::net::TcpListener = socket.into();
    Ok(TcpListener::from_std(std_listener))
}

/// Reader task. Owns the inbound `FixedBuf` and the per-connection
/// subscription map. Parses frames, dispatches handlers, and on exit
/// removes the connection's subscriptions from the worker's table and
/// signals the writer to drain and quit.
async fn reader_task(
    stream: Rc<TcpStream>,
    state: Rc<ConnectionState>,
    mut read_buf: FixedBuf,
    worker: Rc<Worker>,
) {
    // 1. Send INFO greeting before reading anything.
    {
        let mut emit_buf = [0u8; SHORT_FRAME_BUF];
        let Ok(n) = emit_info(&mut emit_buf, SERVER_INFO_JSON) else {
            state.mark_close(CloseReason::ProtocolError);
            return;
        };
        state.enqueue(&emit_buf[..n]);
    }

    let mut accum: Vec<u8> = Vec::with_capacity(DEFAULT_BUFFER_SIZE * 2);
    let mut subscriptions: HashMap<Box<[u8]>, Box<[u8]>> = HashMap::new();
    let read_cap = read_buf.bytes_total();

    loop {
        // Slice the read buffer to its full capacity; read_fixed will
        // overwrite from offset 0 and report n bytes read.
        let (read_res, returned) = stream.read_fixed(read_buf.slice(..read_cap)).await;
        read_buf = returned.into_inner();
        let n = match read_res {
            Ok(n) if n > 0 => n,
            _ => break,
        };

        accum.extend_from_slice(&read_buf[..n]);
        if accum.len() > MAX_FRAME_SIZE {
            // A pathological client is streaming bytes without ever
            // terminating a frame, or sending a single oversized frame.
            // Drop the connection rather than let the accumulator grow
            // without bound.
            state.mark_close(CloseReason::ProtocolError);
            break;
        }
        let mut consumed_total = 0usize;
        loop {
            let outcome = parse_frame(&accum[consumed_total..]);
            match outcome {
                ParseOutcome::Frame { frame, consumed } => {
                    handle_frame(&frame, &state, &worker, &mut subscriptions);
                    consumed_total += consumed;
                    if state.close_reason().is_some() {
                        break;
                    }
                }
                ParseOutcome::NeedMore => break,
                ParseOutcome::Err(_) => {
                    state.mark_close(CloseReason::ProtocolError);
                    break;
                }
            }
        }
        if consumed_total > 0 {
            accum.drain(..consumed_total);
        }
        if state.close_reason().is_some() {
            break;
        }
    }

    // Clean up: remove our subscriptions, drop ourselves from the
    // worker's connection map, signal the writer to drain and exit.
    {
        let mut table = worker.table.borrow_mut();
        // Pass each (subject, sid) to unsubscribe so connection-specific
        // entries are removed without dropping subscriptions belonging
        // to other sids on the same subject.
        for (sid, subject) in &subscriptions {
            table.unsubscribe(subject, state.id(), sid);
        }
    }
    worker.connections.borrow_mut().remove(&state.id());
    state.mark_close(CloseReason::PeerClosed);
    drop(read_buf);
}

/// Writer task. Owns the outbound `FixedBuf`. Awaits notifications from
/// `ConnectionState`; on each wake, drains any pending bytes via
/// `write_fixed_all`. When `close_reason` is set, emits the
/// corresponding `-ERR` (if any), shuts the socket down so the reader
/// task's pending `read_fixed` resolves with EOF, and exits.
async fn writer_task(stream: Rc<TcpStream>, state: Rc<ConnectionState>, write_buf: FixedBuf) {
    let mut buf = write_buf;
    loop {
        state.wait_for_event().await;

        // Drain any data that has piled up.
        loop {
            let n = state.drain_into(&mut buf[..]);
            if n == 0 {
                break;
            }
            let (res, slice) = stream.write_fixed_all(buf.slice(..n)).await;
            buf = slice.into_inner();
            if res.is_err() {
                let _ = stream.shutdown(std::net::Shutdown::Both);
                drop(buf);
                return;
            }
        }

        // If close was signalled, emit -ERR (when applicable) and exit.
        if let Some(reason) = state.close_reason() {
            if let Some(text) = reason.err_text() {
                if let Ok(n) = emit_err(&mut buf[..], text) {
                    let (_res, slice) = stream.write_fixed_all(buf.slice(..n)).await;
                    buf = slice.into_inner();
                }
            }
            // Shut the socket down so the reader task's outstanding
            // `read_fixed` returns with 0 bytes and the connection is
            // fully cleaned up. Without this, a writer-initiated close
            // (e.g. slow consumer) would leave the reader pinned on a
            // never-completing read and the peer never seeing FIN.
            let _ = stream.shutdown(std::net::Shutdown::Both);
            drop(buf);
            return;
        }
    }
}

/// Dispatches one parsed frame against the connection's reader-task
/// state. Synchronous: any outbound side effect is an `enqueue` into
/// `state` (or some other connection's state, for `PUB` fanout) which
/// the corresponding writer task picks up.
fn handle_frame(
    frame: &Frame<'_>,
    state: &Rc<ConnectionState>,
    worker: &Rc<Worker>,
    subscriptions: &mut HashMap<Box<[u8]>, Box<[u8]>>,
) {
    match *frame {
        Frame::Connect { .. } | Frame::Pong => {
            // v1 ignores CONNECT options; client PONGs are silently
            // accepted (server-initiated PING lands in C3+).
        }
        Frame::Ping => {
            let mut buf = [0u8; 8];
            if let Ok(n) = emit_pong(&mut buf) {
                state.enqueue(&buf[..n]);
            }
        }
        Frame::Sub {
            subject,
            queue_group,
            sid,
        } => {
            let sid_box: Box<[u8]> = sid.into();
            let subject_box: Box<[u8]> = subject.into();
            let new_sub = Sub {
                conn_id: state.id(),
                sid: sid_box.clone(),
                queue_group: queue_group.map(Into::into),
            };
            if let Err(e) = worker.table.borrow_mut().subscribe(subject, new_sub) {
                emit_err_to(state, subject_error_text(e));
                return;
            }
            subscriptions.insert(sid_box, subject_box);
        }
        Frame::Unsub { sid, max_msgs } => {
            if max_msgs.is_some() {
                emit_err_to(state, b"UNSUB max_msgs not supported in v1");
                return;
            }
            if let Some(subject) = subscriptions.remove(sid) {
                worker
                    .table
                    .borrow_mut()
                    .unsubscribe(&subject, state.id(), sid);
            }
        }
        Frame::Pub {
            subject,
            reply_to,
            payload,
        } => {
            if payload.len() > MAX_PAYLOAD_BYTES {
                emit_err_to(state, b"maximum payload exceeded");
                return;
            }
            handle_pub(subject, reply_to, payload, state, worker);
        }
        Frame::Msg { .. } | Frame::Info { .. } | Frame::Ok | Frame::Err { .. } => {
            // Server-to-client direction; clients should not send these.
            emit_err_to(state, b"unexpected verb");
        }
    }
}

fn emit_err_to(state: &Rc<ConnectionState>, msg: &[u8]) {
    let mut buf = [0u8; SHORT_FRAME_BUF];
    if let Ok(n) = emit_err(&mut buf, msg) {
        state.enqueue(&buf[..n]);
    }
}

fn subject_error_text(e: aulon_core::SubjectError) -> &'static [u8] {
    match e {
        aulon_core::SubjectError::Empty => b"empty subject",
        aulon_core::SubjectError::EmptyToken => b"empty token in subject",
        aulon_core::SubjectError::WildcardInPublish => b"wildcard not allowed in publish subject",
        aulon_core::SubjectError::InvalidGreaterPosition => b"`>` must be the last token",
    }
}

/// Top-level `PUB` handler on the publishing shard.
///
/// Runs the local fanout for our own shard's subscribers, then (when
/// `shard_count > 1`) encodes a single `Arc<PublishedFrame>` and pushes
/// it into every peer shard's inbox. Each peer's drain task runs its
/// own trie + queue-group dispatch on its own thread.
///
/// Subject validation (no wildcards in publish) is performed by the
/// trie's `for_each_match` inside `dispatch_local`; on the cross-shard
/// path the receiving shard does not re-validate.
fn handle_pub(
    subject: &[u8],
    reply_to: Option<&[u8]>,
    payload: &[u8],
    state: &Rc<ConnectionState>,
    worker: &Rc<Worker>,
) {
    if let Err(e) = dispatch_local(worker, subject, reply_to, payload) {
        emit_err_to(state, subject_error_text(e));
        return;
    }

    if worker.shard_count() > 1 {
        let frame = Arc::new(PublishedFrame {
            subject: Box::from(subject),
            reply_to: reply_to.map(Box::from),
            payload: Box::from(payload),
        });
        let our_idx = worker.shard_id as usize;
        let mut back_pressure_seen = false;
        for (i, inbox) in worker.peer_inboxes.iter().enumerate() {
            if i == our_idx {
                continue;
            }
            match inbox.push(Arc::clone(&frame)) {
                Ok(was_empty) => {
                    if was_empty {
                        if let Some(waker) = worker.peer_wakers.get(i) {
                            let _ = waker.wake();
                        }
                    }
                }
                Err(_full) => {
                    back_pressure_seen = true;
                }
            }
        }
        if back_pressure_seen {
            emit_err_to(state, b"shard back-pressure");
        }
    }
}

/// Runs subscriber match + queue-group dispatch on the current shard.
/// Used both by the publisher's own shard (via `handle_pub`) and by
/// the cross-shard drain task (via `drain_task`).
///
/// Returns `Err(SubjectError)` when subject validation fails; only the
/// publisher's shard surfaces this — peer shards trust the publisher's
/// validation result.
fn dispatch_local(
    worker: &Rc<Worker>,
    subject: &[u8],
    reply_to: Option<&[u8]>,
    payload: &[u8],
) -> Result<(), aulon_core::SubjectError> {
    let mut plain: SmallVec<[Snapshot; PUB_INLINE_PLAIN]> = SmallVec::new();
    let mut groups: SmallVec<[GroupBucket; PUB_INLINE_GROUPS]> = SmallVec::new();

    {
        let table = worker.table.borrow();
        table.for_each_match(subject, |sub| {
            let entry = (sub.conn_id, sub.sid.clone());
            match &sub.queue_group {
                Some(qg) => {
                    if let Some(bucket) = groups.iter_mut().find(|(k, _)| **k == **qg) {
                        bucket.1.push(entry);
                    } else {
                        let mut bucket = SmallVec::new();
                        bucket.push(entry);
                        groups.push((qg.clone(), bucket));
                    }
                }
                None => plain.push(entry),
            }
        })?;
    }

    let mut scratch = worker.emit_scratch.borrow_mut();
    for (conn_id, sid) in &plain {
        let Ok(n) = emit_msg(&mut scratch, subject, sid, reply_to, payload) else {
            continue;
        };
        if let Some(target) = worker.lookup(*conn_id) {
            target.enqueue(&scratch[..n]);
        }
    }
    for (_qg, members) in &groups {
        let pick = worker.pick_index(members.len());
        let (conn_id, sid) = &members[pick];
        let Ok(n) = emit_msg(&mut scratch, subject, sid, reply_to, payload) else {
            continue;
        };
        if let Some(target) = worker.lookup(*conn_id) {
            target.enqueue(&scratch[..n]);
        }
    }
    Ok(())
}

/// Cross-shard drain task. One per shard (when `shard_count > 1`),
/// owns the eventfd file and our shard's inbox slot. Runs `read_at`
/// on the eventfd in a loop; each successful read indicates one or
/// more `wake()` calls from peer shards, after which we drain the
/// inbox and dispatch each frame locally.
async fn drain_task(worker: Rc<Worker>, efd_file: tokio_uring::fs::File) {
    let our_idx = worker.shard_id as usize;
    let mut buf: Vec<u8> = vec![0u8; 8];
    loop {
        let (res, returned) = efd_file.read_at(buf, 0).await;
        buf = returned;
        match res {
            Ok(8) => {}
            // Partial read or error: cannot recover from a non-8-byte
            // eventfd read — the fd is dead. Exit the task.
            _ => break,
        }
        let inbox = &worker.peer_inboxes[our_idx];
        while let Some(frame) = inbox.pop() {
            // Subject validation already happened on the publisher.
            // Ignore the SubjectError result; a peer cannot have
            // forwarded an invalid subject because we never push for
            // a publisher's failed validation.
            let _ = dispatch_local(
                &worker,
                &frame.subject,
                frame.reply_to.as_deref(),
                &frame.payload,
            );
        }
    }
}
