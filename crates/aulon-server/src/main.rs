//! Aulon broker server binary.
//!
//! NATS-core-compatible broker, single-threaded `tokio_uring` runtime.
//! Each accepted connection runs as two cooperative tasks (reader and
//! writer) sharing an [`Rc<ConnectionState>`]. The publish hot path
//! performs no heap allocation: each `MSG` is encoded into a stack
//! buffer per subscriber and copied into that subscriber's pre-
//! allocated outbound ring.
//!
//! See `docs/design/fanout.md` for the full architecture.

#![forbid(unsafe_code)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use aulon_core::{
    BufferPool, CloseReason, ConnectionId, ConnectionState, Sub, SubscriptionTable,
    DEFAULT_BUFFER_SIZE, DEFAULT_OUTBOUND_CAPACITY, DEFAULT_POOL_CAPACITY,
};
use aulon_proto::{emit_err, emit_info, emit_msg, emit_pong, parse_frame, Frame, ParseOutcome};
use tokio_uring::buf::fixed::FixedBuf;
use tokio_uring::buf::BoundedBuf;
use tokio_uring::net::{TcpListener, TcpStream};

const LISTEN_ADDR: &str = "127.0.0.1:4222";

/// Advertised and enforced maximum payload size. Matches `nats-server`'s
/// default; `SERVER_INFO_JSON` advertises this same number so clients
/// will refuse to send larger PUBs locally.
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Maximum total frame size on the read accumulator before we reject the
/// connection with a protocol error. A publisher's `PUB` is the largest
/// possible frame: `PUB <subject> [reply] <#bytes>\r\n<payload>\r\n`.
/// Cap at 2 × `MAX_PAYLOAD_BYTES` to give the header generous headroom
/// and protect against a malicious client that streams bytes without
/// ever terminating a frame.
const MAX_FRAME_SIZE: usize = MAX_PAYLOAD_BYTES * 2;

/// Per-worker heap-allocated emit scratch buffer. Sized to fit one
/// encoded `MSG` at the maximum payload, plus header. One allocation
/// at startup; reused for the lifetime of the worker.
const EMIT_SCRATCH_CAPACITY: usize = MAX_PAYLOAD_BYTES + 4096;

/// Minimal `INFO` greeting body. Real `nats-server` includes more fields
/// (`server_id`, version, etc.); the official `nats` CLI is forgiving as
/// long as the JSON is well-formed and the protocol version is set.
const SERVER_INFO_JSON: &[u8] = br#"{"server_id":"aulon","server_name":"aulon","version":"0.0.1","host":"127.0.0.1","port":4222,"max_payload":1048576,"proto":1,"headers":false,"jetstream":false}"#;

/// Stack buffer size for emitting short control frames (INFO, PONG,
/// `-ERR`). Distinct from the per-worker MSG scratch (which is heap-
/// allocated and large enough for the full `MAX_PAYLOAD_BYTES`).
const SHORT_FRAME_BUF: usize = 4096;

/// Per-worker shared state. The single `tokio_uring` thread owns one
/// instance; reader and writer tasks hold an `Rc<Worker>`.
struct Worker {
    pool: BufferPool,
    table: RefCell<SubscriptionTable>,
    connections: RefCell<HashMap<ConnectionId, Rc<ConnectionState>>>,
    next_conn_id: Cell<u32>,
    /// Pre-allocated MSG-encoding scratch. Heap-allocated once at
    /// startup; resized to its capacity so direct slice indexing is
    /// always in bounds. Each `PUB` fanout `clear()`s it then writes
    /// the encoded MSG once per subscriber. No allocator is touched.
    emit_scratch: RefCell<Vec<u8>>,
}

impl Worker {
    fn new() -> Self {
        let emit_scratch = vec![0u8; EMIT_SCRATCH_CAPACITY];
        Self {
            pool: BufferPool::new(DEFAULT_POOL_CAPACITY, DEFAULT_BUFFER_SIZE),
            table: RefCell::new(SubscriptionTable::new()),
            connections: RefCell::new(HashMap::new()),
            next_conn_id: Cell::new(0),
            emit_scratch: RefCell::new(emit_scratch),
        }
    }

    fn next_id(&self) -> ConnectionId {
        let id = self.next_conn_id.get();
        self.next_conn_id.set(id.wrapping_add(1));
        ConnectionId::new(id)
    }

    fn lookup(&self, id: ConnectionId) -> Option<Rc<ConnectionState>> {
        self.connections.borrow().get(&id).cloned()
    }
}

fn main() -> std::io::Result<()> {
    tokio_uring::start(async move {
        let addr: SocketAddr = LISTEN_ADDR
            .parse()
            .expect("LISTEN_ADDR is a valid socket address literal");
        let worker = Rc::new(Worker::new());
        worker.pool.register()?;
        let listener = TcpListener::bind(addr)?;
        eprintln!(
            "aulon-server: listening on {LISTEN_ADDR} (NATS-core, pool {DEFAULT_POOL_CAPACITY} x {DEFAULT_BUFFER_SIZE} bytes, IORING_REGISTER_BUFFERS)"
        );
        loop {
            let (stream, peer) = listener.accept().await?;
            eprintln!("aulon-server: accepted {peer}");
            let id = worker.next_id();
            let Some(read_buf) = worker.pool.acquire() else {
                eprintln!("aulon-server: pool exhausted (read), dropping {peer}");
                continue;
            };
            let Some(write_buf) = worker.pool.acquire() else {
                eprintln!("aulon-server: pool exhausted (write), dropping {peer}");
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
/// corresponding `-ERR` (if any) and exits.
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
            if queue_group.is_some() {
                emit_err_to(state, b"queue groups not supported in v1");
                return;
            }
            let sid_box: Box<[u8]> = sid.into();
            let subject_box: Box<[u8]> = subject.into();
            subscriptions.insert(sid_box.clone(), subject_box);
            worker.table.borrow_mut().subscribe(
                subject,
                Sub {
                    conn_id: state.id(),
                    sid: sid_box,
                },
            );
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
            // Snapshot the matching subscriber list under a short borrow
            // so the publish loop is free to call `enqueue` on
            // ConnectionStates (which the subscription table does not
            // own; no aliasing risk).
            //
            // HOT-PATH ALLOC: `sub_list` is the only heap allocation on
            // the publish path. One Box clone per matching subscriber.
            // The alternative — calling `enqueue` while the
            // SubscriptionTable RefCell is borrowed — is sound today
            // (no handler re-enters the table) but the invariant is
            // fragile; the snapshot keeps the borrow tight. C3 will
            // revisit this with a callback-based fanout.
            let sub_list: Vec<(ConnectionId, Box<[u8]>)> = {
                let table = worker.table.borrow();
                table
                    .subscribers(subject)
                    .iter()
                    .map(|sub| (sub.conn_id, sub.sid.clone()))
                    .collect()
            };
            // Use the worker's pre-allocated scratch for MSG encoding;
            // sized to fit the full MAX_PAYLOAD_BYTES + header overhead.
            // No reallocation happens here — capacity is fixed at startup.
            let mut scratch = worker.emit_scratch.borrow_mut();
            for (sub_conn_id, sub_sid) in &sub_list {
                let Ok(n) = emit_msg(&mut scratch, subject, sub_sid, reply_to, payload) else {
                    // Should not happen given MAX_PAYLOAD_BYTES check above;
                    // skip rather than panic if the assumption is ever wrong.
                    continue;
                };
                if let Some(target) = worker.lookup(*sub_conn_id) {
                    target.enqueue(&scratch[..n]);
                }
            }
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
