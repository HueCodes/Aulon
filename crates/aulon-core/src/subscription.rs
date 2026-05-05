//! Per-worker subscription trie with NATS-spec wildcards and queue groups.
//!
//! See `docs/design/subscription-trie.md` for representation choices and
//! match algorithm rationale.
//!
//! The trie is keyed on dot-separated tokens. At each level a node has:
//!
//! - `exact`: subscribers whose subscription path ends exactly here.
//! - `rest`: subscribers anchored with `.>` at this node — they match
//!   any concrete subject reaching this point with at least one more
//!   token to consume.
//! - `children`: literal-token children, keyed on `Box<[u8]>`.
//! - `star`: a dedicated slot for the `*` (single-token) wildcard
//!   child; hot-path read avoids a `HashMap` probe.
//!
//! Match is a recursive descent with no backtracking: at each level
//! we emit `rest` subscribers, then recurse into the literal child
//! and the `star` child as parallel branches.

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

/// One subscription on a (possibly wildcarded) subject.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sub {
    /// Owning connection.
    pub conn_id: ConnectionId,
    /// Client-chosen subscription identifier.
    pub sid: Box<[u8]>,
    /// Optional queue group: subscribers in the same group are
    /// load-balanced; subscribers without a group always receive.
    pub queue_group: Option<Box<[u8]>>,
}

/// Why a subject was rejected at validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubjectError {
    /// Subject was empty.
    Empty,
    /// A token was empty (leading, trailing, or doubled `.`).
    EmptyToken,
    /// `*` or `>` appeared in a publish subject (wildcards are
    /// subscriber-only).
    WildcardInPublish,
    /// `>` was not the last token of a subscriber subject.
    InvalidGreaterPosition,
}

/// How many subscribers per `exact` / `rest` bucket fit inline before
/// falling back to the heap.
const INLINE_SUBS: usize = 4;

/// One token in a parsed subscriber subject.
#[derive(Debug, PartialEq, Eq)]
enum Token<'a> {
    Literal(&'a [u8]),
    Star,
    Greater,
}

#[derive(Default, Debug)]
struct Node {
    exact: SmallVec<[Sub; INLINE_SUBS]>,
    rest: SmallVec<[Sub; INLINE_SUBS]>,
    children: HashMap<Box<[u8]>, Box<Node>>,
    star: Option<Box<Node>>,
}

impl Node {
    fn is_empty(&self) -> bool {
        self.exact.is_empty()
            && self.rest.is_empty()
            && self.children.is_empty()
            && self.star.is_none()
    }
}

/// NATS-style subscription trie.
#[derive(Default, Debug)]
pub struct SubscriptionTrie {
    root: Node,
    total_subscriptions: usize,
}

impl SubscriptionTrie {
    /// Creates an empty trie.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of subscriptions across all subjects.
    #[must_use]
    pub fn total_subscriptions(&self) -> usize {
        self.total_subscriptions
    }

    /// Adds `sub` under the (possibly wildcarded) `subject`.
    ///
    /// Returns `Err(SubjectError)` if the subject is malformed:
    /// empty, contains an empty token (leading/trailing/`..`), or has
    /// `>` anywhere other than the final token. `*` and `>` tokens
    /// are accepted as wildcards on this side.
    pub fn subscribe(&mut self, subject: &[u8], sub: Sub) -> Result<(), SubjectError> {
        let tokens: SmallVec<[Token<'_>; 8]> = parse_subscriber_subject(subject)?;
        let mut node = &mut self.root;
        let last_idx = tokens.len() - 1;
        for (i, tok) in tokens.iter().enumerate() {
            match tok {
                Token::Greater => {
                    debug_assert!(i == last_idx, "validator guarantees `>` is last");
                    node.rest.push(sub);
                    self.total_subscriptions += 1;
                    return Ok(());
                }
                Token::Star => {
                    if node.star.is_none() {
                        node.star = Some(Box::default());
                    }
                    node = node.star.as_deref_mut().expect("just inserted star child");
                }
                Token::Literal(bytes) => {
                    if !node.children.contains_key(*bytes) {
                        node.children.insert((*bytes).into(), Box::default());
                    }
                    node = node
                        .children
                        .get_mut(*bytes)
                        .expect("just inserted literal child")
                        .as_mut();
                }
            }
            if i == last_idx {
                node.exact.push(sub);
                self.total_subscriptions += 1;
                return Ok(());
            }
        }
        // Empty `tokens` is impossible: `parse_subscriber_subject`
        // returns `Empty` first.
        unreachable!("non-empty tokens reach return inside the loop")
    }

    /// Removes one `(conn_id, sid)` subscription from `subject`.
    /// Returns `true` if a subscription was removed. Prunes any
    /// trie nodes that become empty as a result.
    pub fn unsubscribe(&mut self, subject: &[u8], conn_id: ConnectionId, sid: &[u8]) -> bool {
        let Ok(tokens): Result<SmallVec<[Token<'_>; 8]>, _> = parse_subscriber_subject(subject)
        else {
            return false;
        };
        unsubscribe_walk(
            &mut self.root,
            &tokens,
            0,
            conn_id,
            sid,
            &mut self.total_subscriptions,
        )
    }

    /// Calls `f` for every subscriber matching the literal `subject`.
    ///
    /// Returns `Err(SubjectError)` if `subject` is malformed or
    /// contains a wildcard token (publishers are not allowed to
    /// publish to wildcards).
    pub fn for_each_match<F: FnMut(&Sub)>(
        &self,
        subject: &[u8],
        mut f: F,
    ) -> Result<(), SubjectError> {
        let tokens: SmallVec<[&[u8]; 8]> = parse_publisher_subject(subject)?;
        walk_match(&self.root, &tokens, 0, &mut f);
        Ok(())
    }
}

fn walk_match<F: FnMut(&Sub)>(node: &Node, tokens: &[&[u8]], depth: usize, f: &mut F) {
    if depth == tokens.len() {
        for sub in &node.exact {
            f(sub);
        }
        return;
    }
    // Subscribers anchored with `.>` at this node match because the
    // remaining suffix has length >= 1 (we are about to consume one
    // more token).
    for sub in &node.rest {
        f(sub);
    }
    if let Some(child) = node.children.get(tokens[depth]) {
        walk_match(child, tokens, depth + 1, f);
    }
    if let Some(star) = node.star.as_deref() {
        walk_match(star, tokens, depth + 1, f);
    }
}

fn unsubscribe_walk(
    node: &mut Node,
    tokens: &[Token<'_>],
    depth: usize,
    conn_id: ConnectionId,
    sid: &[u8],
    total: &mut usize,
) -> bool {
    let last = tokens.len() - 1;
    let tok = &tokens[depth];

    if matches!(tok, Token::Greater) {
        let before = node.rest.len();
        node.rest
            .retain(|s| !(s.conn_id == conn_id && s.sid.as_ref() == sid));
        let removed = node.rest.len() != before;
        if removed {
            *total -= 1;
        }
        return removed;
    }

    match tok {
        Token::Literal(bytes) => {
            let Some(child) = node.children.get_mut(*bytes) else {
                return false;
            };
            let removed = step_or_remove(child, tokens, depth, last, conn_id, sid, total);
            if child.is_empty() {
                node.children.remove(*bytes);
            }
            removed
        }
        Token::Star => {
            let Some(child) = node.star.as_deref_mut() else {
                return false;
            };
            let removed = step_or_remove(child, tokens, depth, last, conn_id, sid, total);
            if child.is_empty() {
                node.star = None;
            }
            removed
        }
        Token::Greater => unreachable!("handled above"),
    }
}

fn step_or_remove(
    child: &mut Node,
    tokens: &[Token<'_>],
    depth: usize,
    last: usize,
    conn_id: ConnectionId,
    sid: &[u8],
    total: &mut usize,
) -> bool {
    if depth == last {
        let before = child.exact.len();
        child
            .exact
            .retain(|s| !(s.conn_id == conn_id && s.sid.as_ref() == sid));
        let removed = child.exact.len() != before;
        if removed {
            *total -= 1;
        }
        removed
    } else {
        unsubscribe_walk(child, tokens, depth + 1, conn_id, sid, total)
    }
}

fn parse_subscriber_subject(s: &[u8]) -> Result<SmallVec<[Token<'_>; 8]>, SubjectError> {
    if s.is_empty() {
        return Err(SubjectError::Empty);
    }
    let mut tokens: SmallVec<[Token<'_>; 8]> = SmallVec::new();
    for tok in s.split(|&b| b == b'.') {
        if tok.is_empty() {
            return Err(SubjectError::EmptyToken);
        }
        tokens.push(match tok {
            b"*" => Token::Star,
            b">" => Token::Greater,
            literal => Token::Literal(literal),
        });
    }
    let last = tokens.len() - 1;
    for (i, t) in tokens.iter().enumerate() {
        if matches!(t, Token::Greater) && i != last {
            return Err(SubjectError::InvalidGreaterPosition);
        }
    }
    Ok(tokens)
}

fn parse_publisher_subject(s: &[u8]) -> Result<SmallVec<[&[u8]; 8]>, SubjectError> {
    if s.is_empty() {
        return Err(SubjectError::Empty);
    }
    let mut tokens: SmallVec<[&[u8]; 8]> = SmallVec::new();
    for tok in s.split(|&b| b == b'.') {
        if tok.is_empty() {
            return Err(SubjectError::EmptyToken);
        }
        if tok == b"*" || tok == b">" {
            return Err(SubjectError::WildcardInPublish);
        }
        tokens.push(tok);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(conn: u32, sid: &[u8]) -> Sub {
        Sub {
            conn_id: ConnectionId::new(conn),
            sid: sid.into(),
            queue_group: None,
        }
    }

    fn sub_qg(conn: u32, sid: &[u8], qg: &[u8]) -> Sub {
        Sub {
            conn_id: ConnectionId::new(conn),
            sid: sid.into(),
            queue_group: Some(qg.into()),
        }
    }

    fn collect(t: &SubscriptionTrie, subject: &[u8]) -> Vec<Sub> {
        let mut out = Vec::new();
        t.for_each_match(subject, |s| out.push(s.clone()))
            .expect("valid publisher subject");
        out
    }

    #[test]
    fn empty_trie_matches_nothing() {
        let t = SubscriptionTrie::new();
        assert!(collect(&t, b"foo").is_empty());
        assert_eq!(t.total_subscriptions(), 0);
    }

    #[test]
    fn exact_match_one_token() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo", sub(1, b"7")).unwrap();
        let m = collect(&t, b"foo");
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].conn_id, ConnectionId::new(1));
    }

    #[test]
    fn exact_match_multi_token() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.bar.baz", sub(1, b"7")).unwrap();
        assert_eq!(collect(&t, b"foo.bar.baz").len(), 1);
        assert!(collect(&t, b"foo.bar").is_empty());
        assert!(collect(&t, b"foo.bar.baz.qux").is_empty());
    }

    #[test]
    fn star_matches_one_token() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.*", sub(1, b"7")).unwrap();
        assert_eq!(collect(&t, b"foo.bar").len(), 1);
        assert_eq!(collect(&t, b"foo.baz").len(), 1);
        assert!(collect(&t, b"foo").is_empty());
        assert!(collect(&t, b"foo.bar.baz").is_empty());
    }

    #[test]
    fn star_in_middle() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.*.baz", sub(1, b"7")).unwrap();
        assert_eq!(collect(&t, b"foo.bar.baz").len(), 1);
        assert_eq!(collect(&t, b"foo.qux.baz").len(), 1);
        assert!(collect(&t, b"foo.bar.qux").is_empty());
        assert!(collect(&t, b"foo.bar").is_empty());
    }

    #[test]
    fn greater_matches_remainder() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.>", sub(1, b"7")).unwrap();
        assert_eq!(collect(&t, b"foo.bar").len(), 1);
        assert_eq!(collect(&t, b"foo.bar.baz").len(), 1);
        assert_eq!(collect(&t, b"foo.bar.baz.qux").len(), 1);
        // `foo.>` does NOT match `foo` alone.
        assert!(collect(&t, b"foo").is_empty());
    }

    #[test]
    fn greater_at_root_matches_anything() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b">", sub(1, b"7")).unwrap();
        assert_eq!(collect(&t, b"foo").len(), 1);
        assert_eq!(collect(&t, b"foo.bar.baz").len(), 1);
        assert_eq!(collect(&t, b"a.b.c.d.e.f.g").len(), 1);
    }

    #[test]
    fn star_alone_matches_one_token_only() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"*", sub(1, b"7")).unwrap();
        assert_eq!(collect(&t, b"foo").len(), 1);
        assert_eq!(collect(&t, b"bar").len(), 1);
        assert!(collect(&t, b"foo.bar").is_empty());
    }

    #[test]
    fn literal_and_star_both_match_disjoint_subscribers() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.bar", sub(1, b"7")).unwrap();
        t.subscribe(b"foo.*", sub(2, b"8")).unwrap();
        let m = collect(&t, b"foo.bar");
        assert_eq!(m.len(), 2);
        let conns: Vec<_> = m.iter().map(|s| s.conn_id.get()).collect();
        assert!(conns.contains(&1));
        assert!(conns.contains(&2));
    }

    #[test]
    fn star_does_not_double_match_with_literal() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.*", sub(1, b"7")).unwrap();
        // `foo.bar` reaches the `*` child once via the star slot;
        // there is no literal `bar` child, so it is one match.
        assert_eq!(collect(&t, b"foo.bar").len(), 1);
    }

    #[test]
    fn greater_and_star_compose() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.*.>", sub(1, b"7")).unwrap();
        assert!(collect(&t, b"foo.bar").is_empty());
        assert_eq!(collect(&t, b"foo.bar.baz").len(), 1);
        assert_eq!(collect(&t, b"foo.bar.baz.qux").len(), 1);
    }

    #[test]
    fn unsubscribe_specific() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.bar", sub(1, b"7")).unwrap();
        t.subscribe(b"foo.bar", sub(1, b"8")).unwrap();
        t.subscribe(b"foo.bar", sub(2, b"7")).unwrap();
        assert!(t.unsubscribe(b"foo.bar", ConnectionId::new(1), b"7"));
        let m = collect(&t, b"foo.bar");
        assert_eq!(m.len(), 2);
        assert!(m
            .iter()
            .all(|s| !(s.conn_id == ConnectionId::new(1) && s.sid.as_ref() == b"7")));
        assert_eq!(t.total_subscriptions(), 2);
    }

    #[test]
    fn unsubscribe_prunes_empty_branches() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.bar.baz", sub(1, b"7")).unwrap();
        assert!(t.unsubscribe(b"foo.bar.baz", ConnectionId::new(1), b"7"));
        assert!(t.root.is_empty(), "trie should be fully pruned");
        assert_eq!(t.total_subscriptions(), 0);
    }

    #[test]
    fn unsubscribe_does_not_prune_shared_branches() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.bar.baz", sub(1, b"7")).unwrap();
        t.subscribe(b"foo.bar.qux", sub(2, b"8")).unwrap();
        assert!(t.unsubscribe(b"foo.bar.baz", ConnectionId::new(1), b"7"));
        assert_eq!(collect(&t, b"foo.bar.qux").len(), 1);
        assert_eq!(t.total_subscriptions(), 1);
    }

    #[test]
    fn unsubscribe_greater() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.>", sub(1, b"7")).unwrap();
        assert!(t.unsubscribe(b"foo.>", ConnectionId::new(1), b"7"));
        assert!(collect(&t, b"foo.bar").is_empty());
        assert!(t.root.is_empty());
    }

    #[test]
    fn unsubscribe_star() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo.*", sub(1, b"7")).unwrap();
        assert!(t.unsubscribe(b"foo.*", ConnectionId::new(1), b"7"));
        assert!(collect(&t, b"foo.bar").is_empty());
        assert!(t.root.is_empty());
    }

    #[test]
    fn invalid_subscriber_subjects_rejected() {
        let mut t = SubscriptionTrie::new();
        assert_eq!(t.subscribe(b"", sub(1, b"7")), Err(SubjectError::Empty));
        assert_eq!(
            t.subscribe(b".foo", sub(1, b"7")),
            Err(SubjectError::EmptyToken)
        );
        assert_eq!(
            t.subscribe(b"foo.", sub(1, b"7")),
            Err(SubjectError::EmptyToken)
        );
        assert_eq!(
            t.subscribe(b"foo..bar", sub(1, b"7")),
            Err(SubjectError::EmptyToken)
        );
        assert_eq!(
            t.subscribe(b"foo.>.bar", sub(1, b"7")),
            Err(SubjectError::InvalidGreaterPosition)
        );
    }

    #[test]
    fn invalid_publisher_subjects_rejected() {
        let t = SubscriptionTrie::new();
        let nop = |_: &Sub| {};
        assert_eq!(t.for_each_match(b"", nop), Err(SubjectError::Empty));
        assert_eq!(
            t.for_each_match(b"foo..bar", nop),
            Err(SubjectError::EmptyToken)
        );
        assert_eq!(
            t.for_each_match(b"foo.*", nop),
            Err(SubjectError::WildcardInPublish)
        );
        assert_eq!(
            t.for_each_match(b"foo.>", nop),
            Err(SubjectError::WildcardInPublish)
        );
    }

    #[test]
    fn star_and_greater_only_when_token_is_exactly_them() {
        let mut t = SubscriptionTrie::new();
        // `f*` is a literal two-byte token, not a wildcard.
        t.subscribe(b"f*", sub(1, b"7")).unwrap();
        assert!(collect(&t, b"foo").is_empty());
        assert_eq!(collect(&t, b"f*").len(), 1);
        // `f>` likewise literal.
        t.subscribe(b"f>", sub(2, b"8")).unwrap();
        assert_eq!(collect(&t, b"f>").len(), 1);
    }

    #[test]
    fn queue_group_is_carried_through_to_callback() {
        let mut t = SubscriptionTrie::new();
        t.subscribe(b"foo", sub_qg(1, b"7", b"workers")).unwrap();
        t.subscribe(b"foo", sub(2, b"8")).unwrap();
        let mut grouped: Vec<Option<Vec<u8>>> = Vec::new();
        t.for_each_match(b"foo", |s| {
            grouped.push(s.queue_group.as_ref().map(|q| q.to_vec()));
        })
        .unwrap();
        assert_eq!(grouped.len(), 2);
        assert!(grouped.contains(&Some(b"workers".to_vec())));
        assert!(grouped.contains(&None));
    }

    #[test]
    fn many_subscribers_overflow_inline_smallvec() {
        let mut t = SubscriptionTrie::new();
        for i in 0..32u32 {
            t.subscribe(b"foo.bar", sub(i, b"7")).unwrap();
        }
        assert_eq!(collect(&t, b"foo.bar").len(), 32);
        assert_eq!(t.total_subscriptions(), 32);
    }
}
