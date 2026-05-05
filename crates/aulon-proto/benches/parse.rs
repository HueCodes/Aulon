//! Codec parse latency micro-benchmarks.
//!
//! Each bench measures `parse_frame` against a representative pre-built
//! input buffer. Inputs are built once outside the iter closure so the
//! reported number is the parser's cost, not the cost of constructing
//! the input.

#![allow(missing_docs)]

use aulon_proto::parse_frame;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn build_pub(payload_len: usize) -> Vec<u8> {
    let mut buf = format!("PUB foo {payload_len}\r\n").into_bytes();
    buf.extend(std::iter::repeat_n(b'x', payload_len));
    buf.extend_from_slice(b"\r\n");
    buf
}

fn build_msg(payload_len: usize) -> Vec<u8> {
    let mut buf = format!("MSG foo 7 {payload_len}\r\n").into_bytes();
    buf.extend(std::iter::repeat_n(b'x', payload_len));
    buf.extend_from_slice(b"\r\n");
    buf
}

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse");

    let ping = b"PING\r\n";
    group.bench_function("ping", |b| {
        b.iter(|| {
            let r = parse_frame(black_box(ping));
            black_box(r);
        });
    });

    let sub = b"SUB foo.bar 7\r\n";
    group.bench_function("sub", |b| {
        b.iter(|| {
            let r = parse_frame(black_box(sub));
            black_box(r);
        });
    });

    let sub_q = b"SUB foo.bar workers 7\r\n";
    group.bench_function("sub_with_queue", |b| {
        b.iter(|| {
            let r = parse_frame(black_box(sub_q));
            black_box(r);
        });
    });

    let unsub = b"UNSUB 7 12\r\n";
    group.bench_function("unsub", |b| {
        b.iter(|| {
            let r = parse_frame(black_box(unsub));
            black_box(r);
        });
    });

    for &n in &[16usize, 256, 4096] {
        let buf = build_pub(n);
        group.bench_function(format!("pub_{n}b"), |b| {
            b.iter(|| {
                let r = parse_frame(black_box(&buf));
                black_box(r);
            });
        });
    }

    for &n in &[16usize, 256, 4096] {
        let buf = build_msg(n);
        group.bench_function(format!("msg_{n}b"), |b| {
            b.iter(|| {
                let r = parse_frame(black_box(&buf));
                black_box(r);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
