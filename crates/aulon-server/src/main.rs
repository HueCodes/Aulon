//! Aulon broker server binary.
//!
//! C1 milestone: a TCP echo server backed by a per-core, `io_uring`-
//! registered fixed-buffer pool, with the connection lifecycle encoded as
//! typestate. Reads use `IORING_OP_READ_FIXED`; writes use
//! `IORING_OP_WRITE_FIXED`.

#![forbid(unsafe_code)]

use std::net::SocketAddr;

use aulon_core::{
    BufferPool, Connection, ReadOutcome, DEFAULT_BUFFER_SIZE, DEFAULT_POOL_CAPACITY,
};
use tokio_uring::net::TcpListener;

const LISTEN_ADDR: &str = "127.0.0.1:4222";

fn main() -> std::io::Result<()> {
    tokio_uring::start(async move {
        let addr: SocketAddr = LISTEN_ADDR
            .parse()
            .expect("LISTEN_ADDR is a valid socket address literal");
        let pool = BufferPool::new(DEFAULT_POOL_CAPACITY, DEFAULT_BUFFER_SIZE);
        pool.register()?;
        let listener = TcpListener::bind(addr)?;
        eprintln!(
            "aulon-server: listening on {LISTEN_ADDR} (pool {DEFAULT_POOL_CAPACITY} x {DEFAULT_BUFFER_SIZE} bytes, IORING_REGISTER_BUFFERS)"
        );
        loop {
            let (stream, peer) = listener.accept().await?;
            eprintln!("aulon-server: accepted {peer}");
            let pool = pool.clone();
            let Some(buf) = pool.acquire() else {
                eprintln!("aulon-server: pool exhausted, dropping {peer}");
                continue;
            };
            tokio_uring::spawn(async move {
                let mut conn = Connection::new(stream, buf);
                while let Ok(ReadOutcome::Bytes(n)) = conn.read().await {
                    if conn.write_all(n).await.is_err() {
                        break;
                    }
                }
                let (_closing, buf) = conn.shutdown();
                drop(buf);
                let _ = pool;
            });
        }
    })
}
