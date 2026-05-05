//! Per-worker subscription table — flat exact-match v1.
//!
//! See `docs/design/routing-v1.md`. Wildcard subjects (`*`, `>`) and
//! queue-group load balancing land in C3.

use std::collections::HashMap;

use smallvec::SmallVec;

/// Worker-local connection identifier, assigned at accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(u32);

impl ConnectionId {
    /// Wraps a raw `u32`.
    #[must_use]
    pub fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Returns the underlying `u32`.
    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

/// One subscription on a specific subject.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sub {
    /// Owning connection.
    pub conn_id: ConnectionId,
    /// Client-chosen subscription identifier.
    pub sid: Box<[u8]>,
}

/// How many subscribers per bucket fit inline before falling back to the heap.
const INLINE_SUBS: usize = 4;

/// Flat per-subject subscription table.
///
/// `subscribers(subject)` is `O(1)` average over the subject space and
/// `O(N)` over the count of subscribers on that subject (which is
/// expected to be small).
#[derive(Debug, Default)]
pub struct SubscriptionTable {
    by_subject: HashMap<Box<[u8]>, SmallVec<[Sub; INLINE_SUBS]>>,
}

impl SubscriptionTable {
    /// Creates an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct subjects with at least one subscriber.
    #[must_use]
    pub fn distinct_subjects(&self) -> usize {
        self.by_subject.len()
    }

    /// Total subscription count across all subjects.
    #[must_use]
    pub fn total_subscriptions(&self) -> usize {
        self.by_subject.values().map(SmallVec::len).sum()
    }

    /// Adds a subscription. The same `(conn_id, sid)` pair can appear
    /// twice on the same subject if the caller subscribes twice; this is
    /// the caller's responsibility to police.
    pub fn subscribe(&mut self, subject: &[u8], sub: Sub) {
        if let Some(bucket) = self.by_subject.get_mut(subject) {
            bucket.push(sub);
        } else {
            let mut bucket = SmallVec::new();
            bucket.push(sub);
            self.by_subject.insert(subject.into(), bucket);
        }
    }

    /// Removes a single `(conn_id, sid)` pair from a subject's bucket.
    /// Drops the bucket entirely when it goes empty so empty buckets
    /// never accumulate. Returns `true` if a subscription was removed.
    pub fn unsubscribe(&mut self, subject: &[u8], conn_id: ConnectionId, sid: &[u8]) -> bool {
        let Some(bucket) = self.by_subject.get_mut(subject) else {
            return false;
        };
        let before = bucket.len();
        bucket.retain(|sub| !(sub.conn_id == conn_id && sub.sid.as_ref() == sid));
        let removed = bucket.len() != before;
        if bucket.is_empty() {
            self.by_subject.remove(subject);
        }
        removed
    }

    /// Removes every subscription owned by `conn_id`. The caller passes
    /// the subjects the connection was subscribed to (typically tracked
    /// in the connection's local state); we iterate only those buckets
    /// rather than scanning the whole table.
    pub fn remove_connection<'a>(
        &mut self,
        conn_id: ConnectionId,
        subjects: impl IntoIterator<Item = &'a [u8]>,
    ) {
        for subject in subjects {
            let Some(bucket) = self.by_subject.get_mut(subject) else {
                continue;
            };
            bucket.retain(|sub| sub.conn_id != conn_id);
            if bucket.is_empty() {
                self.by_subject.remove(subject);
            }
        }
    }

    /// Returns the subscribers for a subject, or an empty slice if there
    /// are none.
    #[must_use]
    pub fn subscribers(&self, subject: &[u8]) -> &[Sub] {
        self.by_subject
            .get(subject)
            .map_or(&[][..], |bucket| bucket.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(conn: u32, sid: &[u8]) -> Sub {
        Sub {
            conn_id: ConnectionId::new(conn),
            sid: sid.into(),
        }
    }

    #[test]
    fn empty_lookup_returns_empty_slice() {
        let t = SubscriptionTable::new();
        assert!(t.subscribers(b"foo").is_empty());
        assert_eq!(t.distinct_subjects(), 0);
        assert_eq!(t.total_subscriptions(), 0);
    }

    #[test]
    fn subscribe_and_lookup() {
        let mut t = SubscriptionTable::new();
        t.subscribe(b"foo.bar", sub(1, b"7"));
        t.subscribe(b"foo.bar", sub(2, b"42"));
        let subs = t.subscribers(b"foo.bar");
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].conn_id, ConnectionId::new(1));
        assert_eq!(subs[1].conn_id, ConnectionId::new(2));
        assert_eq!(t.distinct_subjects(), 1);
        assert_eq!(t.total_subscriptions(), 2);
    }

    #[test]
    fn unsubscribe_specific_pair() {
        let mut t = SubscriptionTable::new();
        t.subscribe(b"foo", sub(1, b"7"));
        t.subscribe(b"foo", sub(1, b"8"));
        t.subscribe(b"foo", sub(2, b"7"));
        assert!(t.unsubscribe(b"foo", ConnectionId::new(1), b"7"));
        let subs = t.subscribers(b"foo");
        assert_eq!(subs.len(), 2);
        assert!(subs
            .iter()
            .all(|s| !(s.conn_id == ConnectionId::new(1) && s.sid.as_ref() == b"7")));
    }

    #[test]
    fn unsubscribe_drops_empty_bucket() {
        let mut t = SubscriptionTable::new();
        t.subscribe(b"foo", sub(1, b"7"));
        assert!(t.unsubscribe(b"foo", ConnectionId::new(1), b"7"));
        assert_eq!(t.distinct_subjects(), 0);
        assert!(t.subscribers(b"foo").is_empty());
    }

    #[test]
    fn unsubscribe_unknown_returns_false() {
        let mut t = SubscriptionTable::new();
        t.subscribe(b"foo", sub(1, b"7"));
        assert!(!t.unsubscribe(b"foo", ConnectionId::new(1), b"99"));
        assert!(!t.unsubscribe(b"bar", ConnectionId::new(1), b"7"));
    }

    #[test]
    fn remove_connection_walks_only_named_subjects() {
        let mut t = SubscriptionTable::new();
        t.subscribe(b"foo", sub(1, b"7"));
        t.subscribe(b"bar", sub(1, b"8"));
        t.subscribe(b"baz", sub(2, b"9"));
        t.remove_connection(ConnectionId::new(1), [b"foo".as_ref(), b"bar".as_ref()]);
        assert!(t.subscribers(b"foo").is_empty());
        assert!(t.subscribers(b"bar").is_empty());
        assert_eq!(t.subscribers(b"baz").len(), 1);
        assert_eq!(t.distinct_subjects(), 1);
    }
}
