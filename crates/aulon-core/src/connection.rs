//! Typestate-encoded connection lifecycle.
//!
//! See `docs/design/connection-lifecycle.md` for the rationale. In C1 only
//! [`Active`] and [`Closing`] are real types; `Negotiating` lands in C2
//! when the NATS `CONNECT` exchange is implemented.

use std::fmt;
use std::io;
use std::marker::PhantomData;

use tokio_uring::buf::fixed::FixedBuf;
use tokio_uring::buf::BoundedBuf;
use tokio_uring::net::TcpStream;

mod sealed {
    pub trait Sealed {}
}

/// Marker trait for connection lifecycle states.
///
/// Sealed: external code cannot define new states.
pub trait State: sealed::Sealed + 'static {}

/// Connection has completed any handshake and is ready for application
/// traffic. All read and write methods live here.
#[derive(Debug)]
pub enum Active {}
impl sealed::Sealed for Active {}
impl State for Active {}

/// Connection is winding down. The local side has signalled close; the
/// stream is held only long enough for the kernel to deliver FIN and any
/// queued bytes to the peer. No reads or writes are allowed.
#[derive(Debug)]
pub enum Closing {}
impl sealed::Sealed for Closing {}
impl State for Closing {}

/// A TCP connection in lifecycle state `S`, paired with a registered fixed
/// buffer for the duration of its `Active` lifetime.
///
/// On transition to `Closing`, the buffer is handed back to the caller so
/// it can be released (dropped) at a deterministic point rather than
/// surviving the connection itself.
pub struct Connection<S: State> {
    stream: TcpStream,
    buffer: Option<FixedBuf>,
    _state: PhantomData<S>,
}

impl<S: State> fmt::Debug for Connection<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("state", &std::any::type_name::<S>())
            .field("buffer_held", &self.buffer.is_some())
            .finish_non_exhaustive()
    }
}

/// Outcome of a single read.
#[derive(Debug)]
pub enum ReadOutcome {
    /// `n` bytes were read into the connection's buffer; `n > 0`.
    Bytes(usize),
    /// The peer half-closed (`read_fixed` returned 0).
    PeerClosed,
}

impl Connection<Active> {
    /// Wraps an accepted stream with the registered buffer the connection
    /// will use for the duration of its `Active` lifetime.
    #[must_use]
    pub fn new(stream: TcpStream, buffer: FixedBuf) -> Self {
        Self {
            stream,
            buffer: Some(buffer),
            _state: PhantomData,
        }
    }

    /// Reads from the underlying stream into the connection's registered
    /// buffer using `IORING_OP_READ_FIXED`.
    ///
    /// On `Ok`, the buffer's `bytes_init` (length) reflects the bytes read.
    pub async fn read(&mut self) -> io::Result<ReadOutcome> {
        // INVARIANT: in Active state, the connection always holds its buffer
        // between method calls. `read` and `write_all` take it to satisfy
        // the rent-return I/O API and put it back before returning.
        let buf = self.buffer.take().expect("Active connection holds its buffer");
        let (result, returned) = self.stream.read_fixed(buf).await;
        self.buffer = Some(returned);
        match result {
            Ok(0) => Ok(ReadOutcome::PeerClosed),
            Ok(n) => Ok(ReadOutcome::Bytes(n)),
            Err(e) => Err(e),
        }
    }

    /// Writes the first `len` bytes of the connection's buffer to the stream
    /// using `IORING_OP_WRITE_FIXED`.
    pub async fn write_all(&mut self, len: usize) -> io::Result<()> {
        let buf = self.buffer.take().expect("Active connection holds its buffer");
        let (result, slice) = self.stream.write_fixed_all(buf.slice(..len)).await;
        self.buffer = Some(slice.into_inner());
        result
    }

    /// Transitions to [`Closing`], handing the rented buffer back to the
    /// caller so it can be released to the owning pool by dropping it.
    #[must_use]
    pub fn shutdown(self) -> (Connection<Closing>, FixedBuf) {
        let buffer = self.buffer.expect("Active connection holds its buffer");
        let next = Connection {
            stream: self.stream,
            buffer: None,
            _state: PhantomData,
        };
        (next, buffer)
    }
}

// `Connection<Closing>` deliberately exposes no methods. Dropping the value
// closes the underlying stream; that is the entire contract for now. NATS
// `-ERR` drain semantics arrive in C2.
impl Connection<Closing> {}
