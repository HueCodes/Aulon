//! Shared per-connection state between the reader and writer tasks.
//!
//! Each connection has two `tokio_uring` tasks: a reader (parses incoming
//! frames, mutates the subscription table, enqueues outbound `MSG` bytes
//! on other connections' [`ConnectionState`]s) and a writer (drains its
//! own [`ConnectionState`]'s outbound buffer into `write_fixed_all`).
//!
//! `ConnectionState` is the only object the two tasks share. Both hold an
//! `Rc<ConnectionState>`. All state lives in `Cell` because the worker is
//! `!Send`: the two tasks run cooperatively on the same thread, so we
//! never need atomics or locks.
//!
//! See `docs/design/fanout.md` for the full architecture rationale.

use std::cell::Cell;

use tokio::sync::Notify;

use crate::subscription::ConnectionId;

/// Default per-connection outbound buffer capacity.
pub const DEFAULT_OUTBOUND_CAPACITY: usize = 256 * 1024;

/// Why a connection is being closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    /// Outbound buffer would overflow on a publish; the connection is
    /// not draining fast enough.
    SlowConsumer,
    /// The reader task observed a malformed frame on the wire.
    ProtocolError,
    /// Peer disconnected (or read returned an unrecoverable error). The
    /// writer task should drain whatever is pending and exit without
    /// emitting a `-ERR` frame.
    PeerClosed,
}

impl CloseReason {
    /// Text for the `-ERR` frame the writer task emits before closing,
    /// or `None` if no `-ERR` should be sent (e.g., the peer is already
    /// gone).
    #[must_use]
    pub fn err_text(self) -> Option<&'static [u8]> {
        match self {
            Self::SlowConsumer => Some(b"slow consumer"),
            Self::ProtocolError => Some(b"protocol error"),
            Self::PeerClosed => None,
        }
    }
}

/// Outcome of an [`ConnectionState::enqueue`] attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// Bytes were appended to the outbound buffer; the writer was
    /// notified.
    Ok,
    /// The outbound buffer would overflow. The connection has been
    /// marked for close with [`CloseReason::SlowConsumer`].
    SlowConsumerClosed,
    /// A close was already pending; the bytes were not enqueued. The
    /// caller should skip this subscriber.
    AlreadyClosing,
}

/// State shared between a connection's reader and writer tasks.
pub struct ConnectionState {
    id: ConnectionId,
    outbound_data: Box<[Cell<u8>]>,
    outbound_head: Cell<usize>,
    outbound_tail: Cell<usize>,
    notify: Notify,
    close_with: Cell<Option<CloseReason>>,
}

impl ConnectionState {
    /// Allocates a new state with `outbound_capacity` bytes of pre-
    /// allocated outbound buffer space.
    #[must_use]
    pub fn new(id: ConnectionId, outbound_capacity: usize) -> Self {
        // `Cell<u8>` is `repr(transparent)` over `u8`; the allocation
        // cost is identical to `Box<[u8]>`. We use `Cell<u8>` here so
        // both writers (publishers, via copy_into) and readers (the
        // writer task, via copy_from) have safe shared access without
        // taking a `&mut`. All access is single-threaded by construction.
        let mut storage: Vec<Cell<u8>> = Vec::with_capacity(outbound_capacity);
        storage.resize_with(outbound_capacity, || Cell::new(0));
        Self {
            id,
            outbound_data: storage.into_boxed_slice(),
            outbound_head: Cell::new(0),
            outbound_tail: Cell::new(0),
            notify: Notify::new(),
            close_with: Cell::new(None),
        }
    }

    /// Worker-local connection identifier.
    #[must_use]
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Outbound buffer total capacity in bytes.
    #[must_use]
    pub fn outbound_capacity(&self) -> usize {
        self.outbound_data.len()
    }

    /// Pending bytes (tail − head).
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.outbound_tail.get() - self.outbound_head.get()
    }

    /// Appends `bytes` to the outbound buffer if there is room. On
    /// overflow, marks the connection for close with `SlowConsumer`
    /// (the writer task picks this up and emits `-ERR slow consumer`
    /// before closing).
    ///
    /// Always notifies the writer task — either there is new data to
    /// send, or there is a pending close to act on.
    pub fn enqueue(&self, bytes: &[u8]) -> EnqueueOutcome {
        if self.close_with.get().is_some() {
            return EnqueueOutcome::AlreadyClosing;
        }
        let tail = self.outbound_tail.get();
        let needed = bytes.len();
        if tail.saturating_add(needed) > self.outbound_data.len() {
            self.close_with.set(Some(CloseReason::SlowConsumer));
            self.notify.notify_one();
            return EnqueueOutcome::SlowConsumerClosed;
        }
        for (i, &b) in bytes.iter().enumerate() {
            self.outbound_data[tail + i].set(b);
        }
        self.outbound_tail.set(tail + needed);
        self.notify.notify_one();
        EnqueueOutcome::Ok
    }

    /// Marks the connection for close with `reason` and notifies the
    /// writer. Idempotent: subsequent calls do nothing.
    pub fn mark_close(&self, reason: CloseReason) {
        if self.close_with.get().is_none() {
            self.close_with.set(Some(reason));
        }
        self.notify.notify_one();
    }

    /// Returns the pending close reason, if any. Does not clear it; the
    /// reason is cleared automatically when the buffer drains and the
    /// writer exits.
    #[must_use]
    pub fn close_reason(&self) -> Option<CloseReason> {
        self.close_with.get()
    }

    /// Awaits a notification: either new outbound data, a close
    /// transition, or both.
    pub async fn wait_for_event(&self) {
        self.notify.notified().await;
    }

    /// Copies the currently pending bytes (head..tail) into `dst`,
    /// advancing the head cursor by the number of bytes copied. Returns
    /// the number of bytes written. If `dst` is shorter than the
    /// pending region, only `dst.len()` bytes are copied; the remainder
    /// stays for the next call.
    ///
    /// When the call drains the buffer completely (head == tail after
    /// the copy), both head and tail are reset to 0 so subsequent
    /// publishes start fresh at offset 0.
    pub fn drain_into(&self, dst: &mut [u8]) -> usize {
        let head = self.outbound_head.get();
        let tail = self.outbound_tail.get();
        let pending = tail - head;
        let n = pending.min(dst.len());
        for (i, slot) in dst.iter_mut().enumerate().take(n) {
            *slot = self.outbound_data[head + i].get();
        }
        let new_head = head + n;
        if new_head == tail {
            self.outbound_head.set(0);
            self.outbound_tail.set(0);
        } else {
            self.outbound_head.set(new_head);
        }
        n
    }
}

impl std::fmt::Debug for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionState")
            .field("id", &self.id)
            .field("outbound_capacity", &self.outbound_data.len())
            .field("pending_bytes", &self.pending_bytes())
            .field("close_reason", &self.close_with.get())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cs(cap: usize) -> ConnectionState {
        ConnectionState::new(ConnectionId::new(1), cap)
    }

    #[test]
    fn fresh_state_is_empty() {
        let s = cs(64);
        assert_eq!(s.pending_bytes(), 0);
        assert_eq!(s.outbound_capacity(), 64);
        assert!(s.close_reason().is_none());
    }

    #[test]
    fn enqueue_then_drain_round_trips_bytes() {
        let s = cs(64);
        assert_eq!(s.enqueue(b"hello"), EnqueueOutcome::Ok);
        assert_eq!(s.pending_bytes(), 5);
        let mut buf = [0u8; 16];
        let n = s.drain_into(&mut buf);
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");
        assert_eq!(s.pending_bytes(), 0);
    }

    #[test]
    fn drain_resets_cursors_to_zero_on_full_drain() {
        let s = cs(8);
        s.enqueue(b"abcd");
        let mut buf = [0u8; 8];
        s.drain_into(&mut buf);
        // Cursors are reset; 8 fresh bytes can be enqueued.
        assert_eq!(s.enqueue(b"12345678"), EnqueueOutcome::Ok);
        let n = s.drain_into(&mut buf);
        assert_eq!(n, 8);
        assert_eq!(&buf[..n], b"12345678");
    }

    #[test]
    fn partial_drain_keeps_remainder() {
        let s = cs(64);
        s.enqueue(b"abcdefgh");
        let mut buf = [0u8; 4];
        let n = s.drain_into(&mut buf);
        assert_eq!(n, 4);
        assert_eq!(&buf[..n], b"abcd");
        assert_eq!(s.pending_bytes(), 4);
        let n = s.drain_into(&mut buf);
        assert_eq!(n, 4);
        assert_eq!(&buf[..n], b"efgh");
        assert_eq!(s.pending_bytes(), 0);
    }

    #[test]
    fn enqueue_overflow_marks_slow_consumer() {
        let s = cs(8);
        assert_eq!(s.enqueue(b"abcd"), EnqueueOutcome::Ok);
        assert_eq!(s.enqueue(b"efghij"), EnqueueOutcome::SlowConsumerClosed);
        assert_eq!(s.close_reason(), Some(CloseReason::SlowConsumer));
        assert_eq!(s.enqueue(b"more"), EnqueueOutcome::AlreadyClosing);
    }

    #[test]
    fn mark_close_is_idempotent() {
        let s = cs(8);
        s.mark_close(CloseReason::ProtocolError);
        s.mark_close(CloseReason::SlowConsumer);
        // First reason wins.
        assert_eq!(s.close_reason(), Some(CloseReason::ProtocolError));
    }

    #[test]
    fn close_reason_text_is_stable() {
        assert_eq!(
            CloseReason::SlowConsumer.err_text(),
            Some(&b"slow consumer"[..])
        );
        assert_eq!(
            CloseReason::ProtocolError.err_text(),
            Some(&b"protocol error"[..])
        );
        assert_eq!(CloseReason::PeerClosed.err_text(), None);
    }
}
