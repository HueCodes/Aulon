//! Bounded lock-free MPSC inbox for cross-shard publish handoff.
//!
//! Vyukov-style bounded MPMC ring restricted to single-consumer use. Each
//! cell carries a sequence number that producers and the consumer use to
//! order writes and reads without locks.
//!
//! Producers (any peer shard's worker thread) push an `Arc<PublishedFrame>`;
//! the owning shard's worker pops them in FIFO order on each wake. The
//! primitive never blocks: a full inbox returns [`ShardInboxFull`] so the
//! caller can apply back-pressure (return `-ERR shard back-pressure` to
//! the publisher).
//!
//! Loom invariants are exercised by `tests/loom_inbox.rs`. See
//! `docs/design/topology-sharding.md` for the broader integration.

#![allow(unsafe_code)]

#[cfg(loom)]
use loom::cell::UnsafeCell;
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(loom))]
use std::cell::UnsafeCell;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

use std::mem::MaybeUninit;

/// One frame in flight from a publisher shard to a peer shard.
///
/// Shared via [`Arc`] so the wire payload is encoded exactly once per
/// `PUB` regardless of fan-out.
#[derive(Debug)]
pub struct PublishedFrame {
    /// The publish subject (pre-validated by the publisher's shard).
    pub subject: Box<[u8]>,
    /// Optional `reply-to` subject from the original `PUB`.
    pub reply_to: Option<Box<[u8]>>,
    /// The publish payload bytes.
    pub payload: Box<[u8]>,
}

/// Returned by [`ShardInbox::push`] when the inbox is at capacity.
///
/// Carries the rejected `Arc` back to the caller so it can drop it
/// without a second allocation.
#[derive(Debug)]
pub struct ShardInboxFull<T>(pub T);

/// Bounded lock-free MPSC ring.
///
/// Capacity is fixed at construction and rounded up to the next power
/// of two so head/tail wrap with `&` instead of `%`. Element type is
/// generic so the loom test can run with `usize` and the production
/// code can run with `Arc<PublishedFrame>`.
pub struct ShardInbox<T> {
    mask: usize,
    cells: Box<[Slot<T>]>,
    /// Producers CAS-bump `tail` to claim a slot.
    tail: AtomicUsize,
    /// Consumer-owned read counter. Producers may load it (Relaxed) to
    /// approximate "was empty before my push" for kick suppression.
    head: AtomicUsize,
}

struct Slot<T> {
    /// Vyukov sequence: producers expect `seq == tail` to claim;
    /// consumer expects `seq == head + 1` to take. After consumer
    /// takes, `seq` is bumped by `cap` so the next round's producer
    /// can see "ready to claim".
    seq: AtomicUsize,
    val: UnsafeCell<MaybeUninit<T>>,
}

// Safety: pushing and popping coordinate through `seq` and the
// CAS on `tail`. Cross-thread access to `val` is gated on the
// matching `seq` transition.
unsafe impl<T: Send> Send for ShardInbox<T> {}
unsafe impl<T: Send> Sync for ShardInbox<T> {}

impl<T> ShardInbox<T> {
    /// Create an inbox whose capacity is the next power of two `>=
    /// requested_capacity`, with a minimum of 2.
    #[must_use]
    pub fn with_capacity(requested_capacity: usize) -> Self {
        let cap = requested_capacity.max(2).next_power_of_two();
        let mut cells: Vec<Slot<T>> = Vec::with_capacity(cap);
        for i in 0..cap {
            cells.push(Slot {
                seq: AtomicUsize::new(i),
                val: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        Self {
            mask: cap - 1,
            cells: cells.into_boxed_slice(),
            tail: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
        }
    }

    /// Total slot count.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.cells.len()
    }

    /// Push one element. Returns `Ok(was_empty_before_push)` on
    /// success, `Err(ShardInboxFull(value))` if the ring is at
    /// capacity.
    ///
    /// `was_empty_before_push` is best-effort and used by the caller
    /// to decide whether to kick the consumer (e.g. write `1` to an
    /// eventfd). False positives just cause one redundant kick;
    /// false negatives are prevented by the `Acquire`-load of `head`
    /// after the slot is published.
    #[allow(clippy::cast_possible_wrap)]
    pub fn push(&self, value: T) -> Result<bool, ShardInboxFull<T>> {
        // The wrap cast on `seq - tail` is the *desired* operation:
        // both counters wrap on overflow and we want signed difference
        // semantics so that "ahead by < cap" and "behind by < cap" are
        // distinguishable.
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.cells[tail & self.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(tail) as isize;
            match diff.cmp(&0) {
                std::cmp::Ordering::Equal => {
                    match self.tail.compare_exchange_weak(
                        tail,
                        tail.wrapping_add(1),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            slot_write(slot, value);
                            slot.seq.store(tail.wrapping_add(1), Ordering::Release);
                            let head_after = self.head.load(Ordering::Acquire);
                            return Ok(head_after == tail);
                        }
                        Err(observed) => tail = observed,
                    }
                }
                std::cmp::Ordering::Less => {
                    // Slot's seq is behind tail by a full lap: ring full.
                    return Err(ShardInboxFull(value));
                }
                std::cmp::Ordering::Greater => {
                    // Another producer has advanced past us; refresh.
                    tail = self.tail.load(Ordering::Relaxed);
                }
            }
        }
    }

    /// Pop one element. Single-consumer only; concurrent calls are a
    /// logic bug.
    #[allow(clippy::cast_possible_wrap)]
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let slot = &self.cells[head & self.mask];
        let seq = slot.seq.load(Ordering::Acquire);
        let diff = seq.wrapping_sub(head.wrapping_add(1)) as isize;
        if diff == 0 {
            let value = slot_read(slot);
            slot.seq
                .store(head.wrapping_add(self.mask + 1), Ordering::Release);
            self.head.store(head.wrapping_add(1), Ordering::Release);
            Some(value)
        } else {
            None
        }
    }

    /// True if the inbox currently looks empty to the consumer.
    /// Single-consumer only.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let slot = &self.cells[head & self.mask];
        let seq = slot.seq.load(Ordering::Acquire);
        seq != head.wrapping_add(1)
    }
}

impl<T> Drop for ShardInbox<T> {
    fn drop(&mut self) {
        // Drain remaining elements so their `Drop` runs.
        while self.pop().is_some() {}
    }
}

/// Manual debug because `UnsafeCell<MaybeUninit<T>>` is not.
impl<T> std::fmt::Debug for ShardInbox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardInbox")
            .field("capacity", &self.cells.len())
            .field("head", &self.head.load(Ordering::Relaxed))
            .field("tail", &self.tail.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[cfg(loom)]
fn slot_write<T>(slot: &Slot<T>, value: T) {
    slot.val.with_mut(|ptr| {
        // Safety: we hold the slot exclusively (we won the CAS on tail)
        // and the consumer is gated on `seq` advancing.
        unsafe { (*ptr).write(value) };
    });
}

#[cfg(not(loom))]
fn slot_write<T>(slot: &Slot<T>, value: T) {
    // Safety: we hold the slot exclusively (we won the CAS on tail)
    // and the consumer is gated on `seq` advancing.
    unsafe { (*slot.val.get()).write(value) };
}

#[cfg(loom)]
fn slot_read<T>(slot: &Slot<T>) -> T {
    slot.val.with_mut(|ptr| {
        // Safety: the consumer holds the slot exclusively after
        // observing `seq == head + 1`. Read once then mark seq.
        unsafe { (*ptr).assume_init_read() }
    })
}

#[cfg(not(loom))]
fn slot_read<T>(slot: &Slot<T>) -> T {
    // Safety: the consumer holds the slot exclusively after
    // observing `seq == head + 1`. Read once then bump seq.
    unsafe { (*slot.val.get()).assume_init_read() }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn capacity_rounds_up_to_power_of_two() {
        assert_eq!(ShardInbox::<u32>::with_capacity(1).capacity(), 2);
        assert_eq!(ShardInbox::<u32>::with_capacity(3).capacity(), 4);
        assert_eq!(ShardInbox::<u32>::with_capacity(64).capacity(), 64);
        assert_eq!(ShardInbox::<u32>::with_capacity(65).capacity(), 128);
    }

    #[test]
    fn push_pop_single_thread_fifo() {
        let q = ShardInbox::<u32>::with_capacity(4);
        for i in 0..4 {
            assert!(q.push(i).is_ok(), "slot {i}");
        }
        assert!(matches!(q.push(99), Err(ShardInboxFull(99))));
        for i in 0..4 {
            assert_eq!(q.pop(), Some(i));
        }
        assert!(q.pop().is_none());
        assert!(q.is_empty());
    }

    #[test]
    fn push_returns_was_empty_flag() {
        let q = ShardInbox::<u32>::with_capacity(4);
        assert!(q.push(1).unwrap(), "first push: was empty");
        assert!(!q.push(2).unwrap(), "second push: not empty");
        assert_eq!(q.pop(), Some(1));
        // Now there's still 1 element; next push is non-empty insert.
        assert!(!q.push(3).unwrap());
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
        assert!(q.push(4).unwrap(), "after drain: empty again");
    }

    #[test]
    fn full_then_drain_then_refill() {
        let q = ShardInbox::<u32>::with_capacity(2);
        assert!(q.push(1).is_ok());
        assert!(q.push(2).is_ok());
        assert!(q.push(3).is_err());
        assert_eq!(q.pop(), Some(1));
        assert!(q.push(3).is_ok());
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
        assert!(q.pop().is_none());
    }

    #[test]
    fn drop_drains_remaining_elements() {
        // Use Arc to confirm refcount returns to 1 after drop.
        let payload = std::sync::Arc::new(PublishedFrame {
            subject: Box::from(&b"x"[..]),
            reply_to: None,
            payload: Box::from(&b"y"[..]),
        });
        let q = ShardInbox::<std::sync::Arc<PublishedFrame>>::with_capacity(4);
        for _ in 0..3 {
            q.push(std::sync::Arc::clone(&payload)).unwrap();
        }
        assert_eq!(std::sync::Arc::strong_count(&payload), 4);
        drop(q);
        assert_eq!(std::sync::Arc::strong_count(&payload), 1);
    }
}
