//! Buffer-pool acquire / drop micro-benchmarks.
//!
//! Measures the cost of `BufferPool::acquire` followed by the implicit
//! `FixedBuf::drop` that returns the buffer to the pool. Runs against an
//! unregistered pool — `try_next` does not touch the `io_uring` runtime
//! context, so this is the same code path the registered pool uses for
//! acquire/release bookkeeping.

#![allow(missing_docs)]

use aulon_core::BufferPool;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_acquire_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool");

    let pool = BufferPool::new(256, 4096);
    group.bench_function("acquire_drop_4k", |b| {
        b.iter(|| {
            let buf = pool.acquire().expect("pool not exhausted");
            black_box(&buf);
        });
    });

    let pool_small = BufferPool::new(64, 256);
    group.bench_function("acquire_drop_256b", |b| {
        b.iter(|| {
            let buf = pool_small.acquire().expect("pool not exhausted");
            black_box(&buf);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_acquire_drop);
criterion_main!(benches);
