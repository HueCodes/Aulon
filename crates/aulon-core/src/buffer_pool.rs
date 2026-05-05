//! Per-core fixed-size buffer pool, registered with `io_uring`.
//!
//! Thin wrapper around [`tokio_uring::buf::fixed::FixedBufPool`] that pins
//! down Aulon's policy: one pool per runtime thread, fixed buffer size,
//! registered against the local ring at startup. The wrapper exists so the
//! rest of the crate has a single named place for sizing / capacity /
//! registration decisions; the heavy lifting (kernel registration,
//! `read_fixed` / `write_fixed_all` integration) is upstream.

use std::io;

use tokio_uring::buf::fixed::{FixedBuf, FixedBufPool};

/// Default buffer size (one page).
pub const DEFAULT_BUFFER_SIZE: usize = 4096;

/// Default number of buffers per pool.
pub const DEFAULT_POOL_CAPACITY: usize = 256;

/// Per-core pool of `io_uring`-registered fixed-size buffers.
///
/// Cloning a `BufferPool` produces a new handle to the same underlying
/// pool; the handle is `!Send`, matching `FixedBufPool`'s thread-local
/// constraint. Each runtime thread should construct exactly one pool and
/// call [`register`](Self::register) before any I/O is issued through it.
#[derive(Clone)]
pub struct BufferPool {
    inner: FixedBufPool<Vec<u8>>,
    buffer_size: usize,
    capacity: usize,
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("capacity", &self.capacity)
            .field("buffer_size", &self.buffer_size)
            .finish_non_exhaustive()
    }
}

impl BufferPool {
    /// Allocates `capacity` buffers of `buffer_size` bytes each. Buffers are
    /// uninitialised vectors with the requested capacity (and length 0); the
    /// kernel writes into them during `read_fixed` operations.
    ///
    /// This does not register the buffers with `io_uring`; call
    /// [`register`](Self::register) inside a `tokio_uring` runtime to do so.
    #[must_use]
    pub fn new(capacity: usize, buffer_size: usize) -> Self {
        let bufs = (0..capacity).map(|_| Vec::with_capacity(buffer_size));
        Self {
            inner: FixedBufPool::new(bufs),
            buffer_size,
            capacity,
        }
    }

    /// Registers the pool's buffers with the current `tokio_uring` runtime
    /// (via `IORING_REGISTER_BUFFERS`).
    ///
    /// Must be called from within `tokio_uring::start` (or equivalent) before
    /// any I/O on the buffers. Registration persists for the runtime's
    /// lifetime.
    ///
    /// # Errors
    ///
    /// Forwards any error from the kernel registration call.
    pub fn register(&self) -> io::Result<()> {
        self.inner.register()
    }

    /// Acquires a buffer of the pool's standard size, or returns `None` if
    /// the pool is exhausted.
    ///
    /// The returned [`FixedBuf`] is registered with the kernel and is
    /// suitable for `read_fixed` / `write_fixed_all` operations. Dropping
    /// the buffer returns it to the pool automatically.
    #[must_use]
    pub fn acquire(&self) -> Option<FixedBuf> {
        self.inner.try_next(self.buffer_size)
    }

    /// Awaits an available buffer, blocking the calling task until one is
    /// returned to the pool.
    pub async fn acquire_async(&self) -> FixedBuf {
        self.inner.next(self.buffer_size).await
    }

    /// Buffer size in bytes.
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Pool capacity (number of buffers managed).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_and_buffer_size_are_recorded() {
        let pool = BufferPool::new(8, 256);
        assert_eq!(pool.capacity(), 8);
        assert_eq!(pool.buffer_size(), 256);
    }

    #[test]
    fn acquire_before_registration_succeeds() {
        // `try_next` only walks the pool's internal free list and does not
        // touch the runtime context; this confirms that constructing the
        // wrapper outside a tokio-uring runtime is sound.
        let pool = BufferPool::new(2, 64);
        let a = pool.acquire().expect("first acquire");
        let b = pool.acquire().expect("second acquire");
        assert!(pool.acquire().is_none(), "pool should be exhausted");
        drop(a);
        let _c = pool.acquire().expect("after drop, slot is free");
        drop(b);
    }
}
