//! Loom concurrency tests for [`aulon_core::ShardInbox`].
//!
//! Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" \
//!   cargo test -p aulon-core --test loom_inbox --release
//! ```
//!
//! Loom's exhaustive interleaving is exponential in operation count, so
//! each test keeps producer + consumer ops below ~5 total. The two
//! tests separate the two race shapes that matter:
//!
//! 1. `producer_producer_race` — multiple producers, single drain on
//!    main thread *after* both producers join. Exercises slot-claim
//!    contention between producers; verifies all elements land.
//! 2. `producer_consumer_race` — one producer thread pushing two
//!    elements, main thread popping concurrently. Exercises
//!    visibility of the published-slot `seq` write to the consumer.
//!
//! Together they rule out lost / duplicated elements under either
//! race shape.
//!
//! These tests are inert under the normal `cargo test` build (no
//! `--cfg loom`); the file just defines the harness.

#![cfg(loom)]

use std::collections::HashSet;

use aulon_core::ShardInbox;
use loom::sync::Arc;
use loom::thread;

#[test]
fn producer_producer_race() {
    loom::model(|| {
        let inbox: Arc<ShardInbox<u32>> = Arc::new(ShardInbox::with_capacity(4));

        let p1 = {
            let inbox = Arc::clone(&inbox);
            thread::spawn(move || {
                inbox.push(10).expect("inbox capacity");
            })
        };
        let p2 = {
            let inbox = Arc::clone(&inbox);
            thread::spawn(move || {
                inbox.push(20).expect("inbox capacity");
            })
        };

        p1.join().expect("p1 join");
        p2.join().expect("p2 join");

        // Drain on the main thread after both producers have published.
        let mut seen: HashSet<u32> = HashSet::new();
        while let Some(v) = inbox.pop() {
            assert!(seen.insert(v), "duplicate element popped: {v}");
        }
        assert_eq!(seen.len(), 2);
        assert!(seen.contains(&10));
        assert!(seen.contains(&20));
    });
}

#[test]
fn producer_consumer_race() {
    loom::model(|| {
        let inbox: Arc<ShardInbox<u32>> = Arc::new(ShardInbox::with_capacity(4));

        let producer = {
            let inbox = Arc::clone(&inbox);
            thread::spawn(move || {
                inbox.push(1).expect("inbox capacity");
                inbox.push(2).expect("inbox capacity");
            })
        };

        // Consumer attempts up to 3 pops; whichever arrive arrive,
        // remainder drains after join.
        let mut seen: HashSet<u32> = HashSet::new();
        for _ in 0..3 {
            if let Some(v) = inbox.pop() {
                assert!(seen.insert(v), "duplicate element popped: {v}");
            }
        }
        producer.join().expect("producer join");
        while let Some(v) = inbox.pop() {
            assert!(seen.insert(v), "duplicate element popped after join: {v}");
        }

        assert_eq!(seen.len(), 2, "saw {seen:?}");
        assert!(seen.contains(&1));
        assert!(seen.contains(&2));
    });
}
