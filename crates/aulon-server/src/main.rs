//! Aulon broker server binary.
//!
//! C1 milestone: a TCP echo server backed by a per-core fixed-size buffer
//! pool, with the connection lifecycle encoded as typestate. `io_uring`
//! fixed-buffer registration lands next inside this checkpoint.

#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::rc::Rc;

use aulon_core::{
    BufferPool, Connection, ReadOutcome, DEFAULT_BUFFER_SIZE, DEFAULT_POOL_CAPACITY,
};
use monoio::net::TcpListener;

const LISTEN_ADDR: &str = "127.0.0.1:4222";

#[monoio::main(driver = "iouring")]
async fn main() -> std::io::Result<()> {
    let pool = Rc::new(RefCell::new(BufferPool::new(
        DEFAULT_POOL_CAPACITY,
        DEFAULT_BUFFER_SIZE,
    )));
    let listener = TcpListener::bind(LISTEN_ADDR)?;
    eprintln!(
        "aulon-server: listening on {LISTEN_ADDR} (pool {DEFAULT_POOL_CAPACITY} x {DEFAULT_BUFFER_SIZE} bytes)"
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("aulon-server: accepted {peer}");
        let pool = Rc::clone(&pool);
        let Some(buf) = pool.borrow_mut().acquire() else {
            eprintln!("aulon-server: pool exhausted, dropping {peer}");
            continue;
        };
        monoio::spawn(async move {
            let mut conn = Connection::new(stream, buf);
            while let Ok(ReadOutcome::Bytes(n)) = conn.read().await {
                if conn.write_all(n).await.is_err() {
                    break;
                }
            }
            let (_closing, buf) = conn.shutdown();
            pool.borrow_mut().release(buf);
        });
    }
}
