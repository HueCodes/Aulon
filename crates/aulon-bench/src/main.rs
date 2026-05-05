//! Aulon ping-pong benchmark client.
//!
//! Single-connection synchronous ping-pong against an Aulon (or any TCP
//! echo) server. Each iteration writes a fixed payload, reads it back, and
//! records the round-trip time in an HDR histogram. Reports p50, p90, p99,
//! p99.9, p99.99, max.
//!
//! Configuration is via environment variables to keep this tool dependency-
//! free at the args layer:
//!
//! - `AULON_ADDR` (default `127.0.0.1:4222`)
//! - `AULON_PAYLOAD_BYTES` (default `256`)
//! - `AULON_ITERATIONS` (default `100000`)
//! - `AULON_WARMUP` (default `1000`)
//!
//! Coordinated-omission correction is not applied: this is a synchronous
//! one-shot ping-pong, so every send waits for the prior reply. The
//! correction matters for paced/multi-connection workloads and lands when
//! those benchmarks do.

#![forbid(unsafe_code)]

use std::iter;
use std::net::SocketAddr;
use std::time::Instant;

use hdrhistogram::Histogram;
use tokio_uring::buf::fixed::{FixedBuf, FixedBufPool};
use tokio_uring::buf::BoundedBuf;
use tokio_uring::net::TcpStream;

#[derive(Debug)]
struct Config {
    addr: SocketAddr,
    payload_bytes: usize,
    iterations: u64,
    warmup: u64,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let addr_str =
            std::env::var("AULON_ADDR").unwrap_or_else(|_| "127.0.0.1:4222".to_string());
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| format!("AULON_ADDR={addr_str:?}: {e}"))?;
        let payload_bytes = usize::try_from(parse_env_u64("AULON_PAYLOAD_BYTES", 256)?)
            .map_err(|e| format!("AULON_PAYLOAD_BYTES out of range: {e}"))?;
        let iterations = parse_env_u64("AULON_ITERATIONS", 100_000)?;
        let warmup = parse_env_u64("AULON_WARMUP", 1_000)?;
        Ok(Self {
            addr,
            payload_bytes,
            iterations,
            warmup,
        })
    }
}

fn parse_env_u64(key: &str, default: u64) -> Result<u64, String> {
    match std::env::var(key) {
        Ok(s) => s.parse().map_err(|e| format!("{key}={s:?}: {e}")),
        Err(_) => Ok(default),
    }
}

fn main() {
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("aulon-bench: invalid config: {e}");
            std::process::exit(2);
        }
    };
    eprintln!(
        "aulon-bench: connecting to {} (payload {} B, warmup {}, iterations {}, IORING_OP_*_FIXED)",
        config.addr, config.payload_bytes, config.warmup, config.iterations
    );

    let exit = tokio_uring::start(async move {
        // Pool of two registered buffers, both pre-filled with payload-size
        // ASCII bytes so they each have `bytes_init = payload_bytes` on
        // acquisition. The send path slices to ..payload_bytes; the read
        // path overwrites bytes 0..n in-place. No `unsafe` needed.
        let pool = FixedBufPool::<Vec<u8>>::new(
            iter::repeat_with(|| vec![b'a'; config.payload_bytes]).take(2),
        );
        if let Err(e) = pool.register() {
            eprintln!("aulon-bench: register_buffers failed: {e}");
            return 1;
        }

        let stream = match TcpStream::connect(config.addr).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("aulon-bench: connect failed: {e}");
                return 1;
            }
        };

        let mut send = pool
            .try_next(config.payload_bytes)
            .expect("pool has two buffers; first acquire succeeds");
        let mut recv = pool
            .try_next(config.payload_bytes)
            .expect("pool has two buffers; second acquire succeeds");

        for _ in 0..config.warmup {
            let (s, r) = match ping_pong(&stream, send, recv, config.payload_bytes).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("aulon-bench: warmup failed: {e}");
                    return 1;
                }
            };
            send = s;
            recv = r;
        }

        let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("histogram bounds: 1 ns to 60 s with 3 sig figs is valid");

        for _ in 0..config.iterations {
            let t0 = Instant::now();
            let (s, r) = match ping_pong(&stream, send, recv, config.payload_bytes).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("aulon-bench: measure failed: {e}");
                    return 1;
                }
            };
            let elapsed_ns = u64::try_from(t0.elapsed().as_nanos())
                .expect("RTT fits in u64 ns (60 s ceiling)");
            send = s;
            recv = r;
            hist.record(elapsed_ns).expect("RTT within histogram bounds");
        }

        report(&hist, &config);
        drop(send);
        drop(recv);
        0
    });
    if exit != 0 {
        std::process::exit(exit);
    }
}

async fn ping_pong(
    stream: &TcpStream,
    send: FixedBuf,
    recv: FixedBuf,
    expected: usize,
) -> std::io::Result<(FixedBuf, FixedBuf)> {
    let (write_res, send_slice) = stream.write_fixed_all(send.slice(..expected)).await;
    write_res?;
    let send = send_slice.into_inner();

    let mut total = 0;
    let mut buf = recv;
    while total < expected {
        let (read_res, returned) = stream.read_fixed(buf).await;
        buf = returned;
        let n = read_res?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "server closed connection mid-payload",
            ));
        }
        total += n;
    }
    Ok((send, buf))
}

fn report(hist: &Histogram<u64>, config: &Config) {
    let pct = |p: f64| hist.value_at_percentile(p);
    println!("aulon-bench results");
    println!("  payload_bytes : {}", config.payload_bytes);
    println!("  iterations    : {}", config.iterations);
    println!("  count         : {}", hist.len());
    println!("  min ns        : {}", hist.min());
    println!("  p50 ns        : {}", pct(50.0));
    println!("  p90 ns        : {}", pct(90.0));
    println!("  p99 ns        : {}", pct(99.0));
    println!("  p99.9 ns      : {}", pct(99.9));
    println!("  p99.99 ns     : {}", pct(99.99));
    println!("  max ns        : {}", hist.max());
}
