//! Aulon broker server binary.
//!
//! C1 milestone: a TCP echo server backed by a per-core fixed-size buffer
//! pool. Typestate connection and `io_uring` fixed-buffer registration land
//! in the next steps inside this checkpoint.

#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::rc::Rc;

use aulon_core::{BufferPool, PooledBuffer, DEFAULT_BUFFER_SIZE, DEFAULT_POOL_CAPACITY};
use monoio::buf::IoBuf;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};

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
        monoio::spawn(handle(stream, pool));
    }
}

async fn handle(mut stream: TcpStream, pool: Rc<RefCell<BufferPool>>) {
    let Some(mut buf) = pool.borrow_mut().acquire() else {
        eprintln!("aulon-server: pool exhausted, dropping connection");
        return;
    };
    loop {
        let (read_res, returned) = stream.read(buf).await;
        buf = returned;
        let n = match read_res {
            Ok(n) if n > 0 => n,
            _ => break,
        };
        let (write_res, slice) = stream.write_all(buf.slice(..n)).await;
        buf = slice.into_inner();
        if write_res.is_err() {
            break;
        }
    }
    release(&pool, buf);
}

fn release(pool: &Rc<RefCell<BufferPool>>, buf: PooledBuffer) {
    pool.borrow_mut().release(buf);
}
