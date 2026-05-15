//! Aulon multi-subscriber fanout benchmark.
//!
//! Spawns one publisher and `N` subscribers against an Aulon server, all on
//! a single-threaded `tokio_uring` runtime in this process. Every published
//! `PUB foo` message is fanned out by the server to all `N` subscribers,
//! each of which records publish-to-deliver latency in its own HDR histogram.
//! The histograms are aggregated at the end and the merged percentiles are
//! printed.
//!
//! Latency definition: publisher writes the current monotonic timestamp
//! (nanoseconds since a process-local `Instant` baseline, encoded as
//! big-endian `u64`) into the first 8 bytes of the payload before the
//! `PUB`. Each subscriber decodes that timestamp on receipt and records
//! `now - sent_ts`. Both endpoints share the same `Instant` baseline because
//! they run in the same process; this is what lets us measure one-way
//! delivery latency without coordinating two clocks.
//!
//! Coordinated-omission correction is **not** applied here. The publisher
//! waits for its own `write_all` to complete before issuing the next PUB,
//! and `AULON_PACE_WINDOW` bounds how far ahead of the slowest subscriber
//! the publisher is allowed to run. The window is a hard back-pressure
//! signal against the per-connection outbound ring rather than a
//! paced-load model; a paced multi-publisher variant is still future
//! work.
//!
//! Configuration is environment-driven, mirroring `aulon-bench`:
//!
//! - `AULON_ADDR` (default `127.0.0.1:4222`)
//! - `AULON_FANOUT` (default `8`) — number of subscribers
//! - `AULON_PAYLOAD_BYTES` (default `256`) — must be `>= 8` to fit timestamp
//! - `AULON_ITERATIONS` (default `50000`)
//! - `AULON_WARMUP` (default `1000`)
//! - `AULON_PACE_WINDOW` (default `2`, `0` disables): maximum number of
//!   messages the publisher is allowed to be ahead of the slowest
//!   subscriber. Acts as back-pressure against the per-connection
//!   outbound buffer; without it, single-process runs trip slow-consumer
//!   eviction near the end of the run and contaminate the tail
//!   percentiles. Tighter values (1-2) expose steady-state per-message
//!   latency; larger values (16+) reflect the depth of the in-flight
//!   queue. See `docs/reviews/checkpoint-4.md`.

#![forbid(unsafe_code)]

use std::cell::{Cell, RefCell};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;

use hdrhistogram::Histogram;
use tokio_uring::net::TcpStream;

const SUBJECT: &[u8] = b"foo";
const SID: &[u8] = b"1";
const READ_CHUNK: usize = 16 * 1024;

#[derive(Debug)]
struct Config {
    addr: SocketAddr,
    fanout: usize,
    payload_bytes: usize,
    iterations: u64,
    warmup: u64,
    pace_window: u64,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let addr_str = std::env::var("AULON_ADDR").unwrap_or_else(|_| "127.0.0.1:4222".to_string());
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| format!("AULON_ADDR={addr_str:?}: {e}"))?;
        let fanout = usize::try_from(parse_env_u64("AULON_FANOUT", 8)?)
            .map_err(|e| format!("AULON_FANOUT out of range: {e}"))?;
        if fanout == 0 {
            return Err("AULON_FANOUT must be >= 1".into());
        }
        let payload_bytes = usize::try_from(parse_env_u64("AULON_PAYLOAD_BYTES", 256)?)
            .map_err(|e| format!("AULON_PAYLOAD_BYTES out of range: {e}"))?;
        if payload_bytes < 8 {
            return Err("AULON_PAYLOAD_BYTES must be >= 8 (timestamp prefix)".into());
        }
        let iterations = parse_env_u64("AULON_ITERATIONS", 50_000)?;
        let warmup = parse_env_u64("AULON_WARMUP", 1_000)?;
        let pace_window = parse_env_u64("AULON_PACE_WINDOW", 2)?;
        Ok(Self {
            addr,
            fanout,
            payload_bytes,
            iterations,
            warmup,
            pace_window,
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
            eprintln!("aulon-fanout: invalid config: {e}");
            std::process::exit(2);
        }
    };
    eprintln!(
        "aulon-fanout: {} subscribers + 1 publisher on {} (payload {} B, warmup {}, iterations {}, pace_window {})",
        config.fanout, config.addr, config.payload_bytes, config.warmup, config.iterations, config.pace_window
    );

    let exit = tokio_uring::start(async move {
        let baseline = Instant::now();
        let total_msgs = config.iterations + config.warmup;

        // Each subscriber owns a histogram in a Rc<RefCell<...>> so the
        // main task can drain it once the subscriber exits. The
        // received-counter is the publisher's pacing signal: each
        // subscriber bumps it after every successful MSG decode, and the
        // publisher reads min(received) before each PUB to enforce the
        // pace window.
        let mut sub_histograms: Vec<Rc<RefCell<Histogram<u64>>>> =
            Vec::with_capacity(config.fanout);
        let mut sub_received: Vec<Rc<Cell<u64>>> = Vec::with_capacity(config.fanout);
        let mut sub_handles = Vec::with_capacity(config.fanout);

        for sub_idx in 0..config.fanout {
            let hist = Rc::new(RefCell::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                    .expect("histogram bounds: 1 ns to 60 s, 3 sig figs"),
            ));
            sub_histograms.push(Rc::clone(&hist));
            let received = Rc::new(Cell::new(0u64));
            sub_received.push(Rc::clone(&received));
            let stream = match TcpStream::connect(config.addr).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("aulon-fanout: subscriber {sub_idx} connect failed: {e}");
                    return 1;
                }
            };
            let handle = tokio_uring::spawn(subscriber_task(
                stream,
                hist,
                received,
                baseline,
                total_msgs,
                config.payload_bytes,
                sub_idx,
            ));
            sub_handles.push(handle);
        }

        let pub_stream = match TcpStream::connect(config.addr).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("aulon-fanout: publisher connect failed: {e}");
                return 1;
            }
        };

        if let Err(e) = publisher_drive(&pub_stream, baseline, &config, &sub_received).await {
            eprintln!("aulon-fanout: publisher failed: {e}");
            return 1;
        }

        for (i, handle) in sub_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("aulon-fanout: subscriber {i} task error: {e}"),
                Err(e) => eprintln!("aulon-fanout: subscriber {i} join error: {e}"),
            }
        }

        let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("histogram bounds valid");
        let mut samples_per_sub = Vec::with_capacity(sub_histograms.len());
        for hist in &sub_histograms {
            let h = hist.borrow();
            samples_per_sub.push(h.len());
            merged.add(&*h).expect("merge cannot exceed bounds");
        }
        report(&merged, &samples_per_sub, &config);
        0
    });
    if exit != 0 {
        std::process::exit(exit);
    }
}

async fn subscriber_task(
    stream: TcpStream,
    hist: Rc<RefCell<Histogram<u64>>>,
    received_counter: Rc<Cell<u64>>,
    baseline: Instant,
    expected_msgs: u64,
    payload_bytes: usize,
    sub_idx: usize,
) -> std::io::Result<()> {
    // Drain INFO line first (server greets on accept). We keep a reusable
    // accumulator so we can re-feed leftover bytes after each parse pass.
    let mut accum: Vec<u8> = Vec::with_capacity(READ_CHUNK);

    // Send CONNECT + SUB. Both fit in one small write.
    let connect_sub = build_connect_sub();
    let (write_res, _) = stream.write_all(connect_sub).await;
    write_res?;

    // Wait for INFO to clear (one line ending in CRLF).
    drain_one_line(&stream, &mut accum).await?;

    let mut received: u64 = 0;
    while received < expected_msgs {
        if let Some((ts, _)) = try_take_msg(&mut accum, payload_bytes) {
            let now_ns = u64::try_from(baseline.elapsed().as_nanos())
                .expect("elapsed nanos fit in u64 over a bench run");
            if now_ns >= ts {
                hist.borrow_mut()
                    .record(now_ns - ts)
                    .expect("delivery latency within histogram bounds");
            }
            received += 1;
            received_counter.set(received);
            continue;
        }

        let buf = vec![0u8; READ_CHUNK];
        let (read_res, returned) = stream.read(buf).await;
        let n = read_res?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("subscriber {sub_idx} eof after {received} msgs"),
            ));
        }
        accum.extend_from_slice(&returned[..n]);
    }
    Ok(())
}

async fn publisher_drive(
    stream: &TcpStream,
    baseline: Instant,
    config: &Config,
    sub_received: &[Rc<Cell<u64>>],
) -> std::io::Result<()> {
    let mut accum: Vec<u8> = Vec::with_capacity(READ_CHUNK);

    let connect = b"CONNECT {}\r\n".to_vec();
    let (write_res, _) = stream.write_all(connect).await;
    write_res?;
    drain_one_line(stream, &mut accum).await?;

    let total = config.warmup + config.iterations;
    let mut frame = Vec::with_capacity(64 + config.payload_bytes);
    for sent in 0..total {
        // Back-pressure: don't get more than `pace_window` messages
        // ahead of the slowest subscriber. Without this, the publisher
        // races ahead, fills the per-connection outbound ring, and
        // trips slow-consumer eviction at the tail of the run. With
        // pace_window=0 the pacing is disabled (legacy behaviour).
        if config.pace_window > 0 {
            while sent.saturating_sub(min_received(sub_received)) >= config.pace_window {
                tokio::task::yield_now().await;
            }
        }

        frame.clear();
        let header = format!("PUB {} {}\r\n", str_subject(), config.payload_bytes);
        frame.extend_from_slice(header.as_bytes());
        let ts_ns = u64::try_from(baseline.elapsed().as_nanos())
            .expect("elapsed nanos fit in u64 over a bench run");
        frame.extend_from_slice(&ts_ns.to_be_bytes());
        // Pad with filler so the frame is exactly payload_bytes long.
        frame.resize(header.len() + config.payload_bytes, b'a');
        frame.extend_from_slice(b"\r\n");
        let (write_res, returned) = stream.write_all(frame).await;
        write_res?;
        frame = returned;
        // Yield so the spawned subscriber tasks get a chance to drain
        // their TCP recv buffers in between PUBs. Without this, the
        // publisher's tight `write_all` loop dominates the single-
        // threaded runtime and per-message delivery latency is reported
        // as the time the whole batch takes to drain rather than
        // per-message wire latency.
        tokio::task::yield_now().await;
    }
    Ok(())
}

fn min_received(sub_received: &[Rc<Cell<u64>>]) -> u64 {
    sub_received
        .iter()
        .map(|c| c.get())
        .min()
        .unwrap_or(u64::MAX)
}

fn build_connect_sub() -> Vec<u8> {
    let mut v = Vec::with_capacity(64);
    v.extend_from_slice(b"CONNECT {}\r\n");
    v.extend_from_slice(b"SUB ");
    v.extend_from_slice(SUBJECT);
    v.extend_from_slice(b" ");
    v.extend_from_slice(SID);
    v.extend_from_slice(b"\r\n");
    v
}

fn str_subject() -> &'static str {
    // SAFETY (logical, not unsafe-block): SUBJECT is ASCII by construction.
    std::str::from_utf8(SUBJECT).expect("SUBJECT is ASCII")
}

/// Reads from `stream` until `accum` contains at least one CRLF, then
/// removes that line (including its CRLF) from the front of `accum`.
async fn drain_one_line(stream: &TcpStream, accum: &mut Vec<u8>) -> std::io::Result<()> {
    loop {
        if let Some(end) = find_crlf(accum) {
            accum.drain(..end + 2);
            return Ok(());
        }
        let buf = vec![0u8; READ_CHUNK];
        let (read_res, returned) = stream.read(buf).await;
        let n = read_res?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof while draining greeting",
            ));
        }
        accum.extend_from_slice(&returned[..n]);
    }
}

fn find_crlf(b: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i + 1 < b.len() {
        if b[i] == b'\r' && b[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// If `accum` starts with a complete `MSG ... \r\n<payload>\r\n` frame,
/// extract the timestamp prefix from the payload and consume those bytes.
/// Returns `Some((ts_ns, frame_len))` on success.
///
/// Skips any non-MSG control lines (INFO, PING, etc.) by dropping them
/// from the buffer when encountered.
fn try_take_msg(accum: &mut Vec<u8>, payload_bytes: usize) -> Option<(u64, usize)> {
    loop {
        let header_end = find_crlf(accum)?;
        let header = &accum[..header_end];
        if header.starts_with(b"MSG ") {
            // Need header + CRLF + payload + CRLF total.
            let frame_total = header_end + 2 + payload_bytes + 2;
            if accum.len() < frame_total {
                return None;
            }
            let payload_start = header_end + 2;
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&accum[payload_start..payload_start + 8]);
            let ts = u64::from_be_bytes(ts_bytes);
            accum.drain(..frame_total);
            return Some((ts, frame_total));
        }
        if header.starts_with(b"PING") {
            // Server may PING; the server doesn't actually expect us to
            // PONG, but skip the line cleanly anyway.
            accum.drain(..header_end + 2);
            continue;
        }
        // Unknown line — drop it to stay in sync. This is a bench, not a
        // production client.
        accum.drain(..header_end + 2);
    }
}

fn report(hist: &Histogram<u64>, samples_per_sub: &[u64], config: &Config) {
    let pct = |p: f64| hist.value_at_percentile(p);
    println!("aulon-fanout results");
    println!("  subscribers   : {}", config.fanout);
    println!("  payload_bytes : {}", config.payload_bytes);
    println!("  iterations    : {}", config.iterations);
    println!("  per_sub_msgs  : {samples_per_sub:?}");
    println!("  count         : {}", hist.len());
    println!("  min ns        : {}", hist.min());
    println!("  p50 ns        : {}", pct(50.0));
    println!("  p90 ns        : {}", pct(90.0));
    println!("  p99 ns        : {}", pct(99.0));
    println!("  p99.9 ns      : {}", pct(99.9));
    println!("  p99.99 ns     : {}", pct(99.99));
    println!("  max ns        : {}", hist.max());
}
