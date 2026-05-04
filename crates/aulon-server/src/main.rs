//! Aulon broker server binary.
//!
//! C1 milestone: a minimal TCP echo on a single Monoio runtime. The buffer
//! pool, typestate connection, and `io_uring` fixed-buffer registration are
//! introduced in subsequent commits inside this checkpoint.

#![forbid(unsafe_code)]

use monoio::buf::IoBuf;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};

const LISTEN_ADDR: &str = "127.0.0.1:4222";
const BUFFER_SIZE: usize = 4096;

#[monoio::main(driver = "iouring")]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(LISTEN_ADDR)?;
    eprintln!("aulon-server: listening on {LISTEN_ADDR}");
    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("aulon-server: accepted {peer}");
        monoio::spawn(handle(stream));
    }
}

async fn handle(mut stream: TcpStream) {
    let mut buf = vec![0u8; BUFFER_SIZE];
    loop {
        let (read_res, returned) = stream.read(buf).await;
        buf = returned;
        let n = match read_res {
            Ok(n) if n > 0 => n,
            _ => return,
        };
        let (write_res, returned) = stream.write_all(buf.slice(..n)).await;
        buf = returned.into_inner();
        if write_res.is_err() {
            return;
        }
    }
}
