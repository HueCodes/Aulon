//! Subscription-trie match micro-benchmarks.
//!
//! Builds a [`SubscriptionTrie`] populated with 10,000 subscriptions
//! distributed across a representative shape — 7,000 exact, 2,500 `*`,
//! 500 `>` — then measures `for_each_match` for several publish
//! subjects of varying token depth.
//!
//! See `docs/design/subscription-trie.md` for the perf target
//! (median < 500 ns at 3-token subjects).

#![allow(missing_docs)]

use aulon_core::{ConnectionId, Sub, SubscriptionTrie};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

const TOTAL: u32 = 10_000;
const EXACT_COUNT: u32 = 7_000;
const STAR_COUNT: u32 = 2_500;
// Implicit: GREATER_COUNT = TOTAL - EXACT_COUNT - STAR_COUNT == 500.

fn build_trie() -> SubscriptionTrie {
    let mut t = SubscriptionTrie::new();
    let sid: Box<[u8]> = b"1".as_slice().into();

    // 7,000 exact subjects of the form `app.<i mod 10>.svc.<i>`.
    for i in 0..EXACT_COUNT {
        let subject = format!("app.{}.svc.{}", i % 10, i);
        t.subscribe(
            subject.as_bytes(),
            Sub {
                conn_id: ConnectionId::new(i),
                sid: sid.clone(),
                queue_group: None,
            },
        )
        .expect("valid subject");
    }
    // 2,500 single-`*` wildcards under different parents:
    // `app.<i mod 25>.metric.*`.
    for i in 0..STAR_COUNT {
        let subject = format!("app.{}.metric.*", i % 25);
        t.subscribe(
            subject.as_bytes(),
            Sub {
                conn_id: ConnectionId::new(EXACT_COUNT + i),
                sid: sid.clone(),
                queue_group: None,
            },
        )
        .expect("valid subject");
    }
    // 500 `>` wildcards: `tenant.<i mod 50>.>`.
    let greater_count = TOTAL - EXACT_COUNT - STAR_COUNT;
    for i in 0..greater_count {
        let subject = format!("tenant.{}.>", i % 50);
        t.subscribe(
            subject.as_bytes(),
            Sub {
                conn_id: ConnectionId::new(EXACT_COUNT + STAR_COUNT + i),
                sid: sid.clone(),
                queue_group: None,
            },
        )
        .expect("valid subject");
    }
    t
}

fn bench_match(c: &mut Criterion) {
    let trie = build_trie();
    assert_eq!(trie.total_subscriptions(), TOTAL as usize);

    let mut group = c.benchmark_group("trie_match");

    // 2-token subject hitting the `app.*.metric.*` arm only via the
    // root/`app` literal then no further match (no exact at depth 2).
    group.bench_function("2token_app.0", |b| {
        b.iter(|| {
            let mut count = 0usize;
            trie.for_each_match(b"app.0", |_| count += 1).unwrap();
            black_box(count);
        });
    });

    // 3-token subject hitting the wildcard arm.
    group.bench_function("3token_app.0.metric.cpu", |b| {
        b.iter(|| {
            let mut count = 0usize;
            trie.for_each_match(b"app.0.metric.cpu", |_| count += 1)
                .unwrap();
            black_box(count);
        });
    });

    // 4-token subject hitting both an exact subscription and (no) wildcard.
    group.bench_function("4token_app.0.svc.42", |b| {
        b.iter(|| {
            let mut count = 0usize;
            trie.for_each_match(b"app.0.svc.42", |_| count += 1)
                .unwrap();
            black_box(count);
        });
    });

    // 5-token subject under `tenant.<n>.>`.
    group.bench_function("5token_tenant.0.foo.bar.baz", |b| {
        b.iter(|| {
            let mut count = 0usize;
            trie.for_each_match(b"tenant.0.foo.bar.baz", |_| count += 1)
                .unwrap();
            black_box(count);
        });
    });

    // 7-token subject under a deep `tenant.<n>.>`.
    group.bench_function("7token_tenant.0.a.b.c.d.e", |b| {
        b.iter(|| {
            let mut count = 0usize;
            trie.for_each_match(b"tenant.0.a.b.c.d.e", |_| count += 1)
                .unwrap();
            black_box(count);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_match);
criterion_main!(benches);
