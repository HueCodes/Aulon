//! Per-core fixed-size buffer pool.
//!
//! Each runtime thread owns one [`BufferPool`]. Buffers are acquired by index,
//! handed to `monoio` read/write operations, and released back to the pool on
//! completion. The pool is `!Send`; nothing crosses cores.
//!
//! `IORING_REGISTER_BUFFERS` registration lands later inside C1.

// SAFETY note: this module contains `unsafe impl` blocks for monoio's
// `IoBuf` / `IoBufMut` traits. The unsafe is mechanically required to plug
// our owned buffer type into monoio's io_uring read/write API; the
// implementations only expose pointers into a `Box<[u8]>` that the
// `PooledBuffer` owns for its full lifetime. No pointer dereferences happen
// in this file.
#![allow(unsafe_code)]

use std::fmt;

/// Default buffer size (one page).
pub const DEFAULT_BUFFER_SIZE: usize = 4096;

/// Default number of buffers per pool.
pub const DEFAULT_POOL_CAPACITY: u16 = 256;

/// Identifier for a buffer in a [`BufferPool`].
///
/// Doubles as the `buf_index` argument used with `IORING_REGISTER_BUFFERS`
/// once registration lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BufferId(u16);

impl BufferId {
    /// Returns the underlying index.
    #[must_use]
    pub fn index(self) -> u16 {
        self.0
    }
}

impl fmt::Display for BufferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "buf#{}", self.0)
    }
}

/// A pool of fixed-size buffers, owned by a single runtime thread.
pub struct BufferPool {
    slots: Vec<Option<Box<[u8]>>>,
    free: Vec<u16>,
    buffer_size: usize,
}

impl BufferPool {
    /// Allocates a new pool with `capacity` buffers of `buffer_size` bytes
    /// each.
    #[must_use]
    pub fn new(capacity: u16, buffer_size: usize) -> Self {
        let capacity_usize = usize::from(capacity);
        let mut slots = Vec::with_capacity(capacity_usize);
        let mut free = Vec::with_capacity(capacity_usize);
        for i in 0..capacity {
            slots.push(Some(vec![0u8; buffer_size].into_boxed_slice()));
            free.push(i);
        }
        Self {
            slots,
            free,
            buffer_size,
        }
    }

    /// Pool capacity (total number of buffers managed).
    #[must_use]
    pub fn capacity(&self) -> u16 {
        u16::try_from(self.slots.len()).unwrap_or(u16::MAX)
    }

    /// Buffer size in bytes.
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Number of buffers currently checked out.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    /// Acquires a buffer, or returns `None` if the pool is exhausted.
    pub fn acquire(&mut self) -> Option<PooledBuffer> {
        let idx = self.free.pop()?;
        let bytes = self.slots[usize::from(idx)]
            .take()
            .expect("pool invariant: free index has Some slot");
        Some(PooledBuffer {
            id: BufferId(idx),
            bytes,
            init_len: 0,
        })
    }

    /// Returns a buffer to the pool. The buffer's contents are not zeroed;
    /// callers must not assume previous bytes are cleared.
    pub fn release(&mut self, buf: PooledBuffer) {
        let idx = buf.id.0;
        debug_assert!(self.slots[usize::from(idx)].is_none(), "double release");
        self.slots[usize::from(idx)] = Some(buf.bytes);
        self.free.push(idx);
    }
}

impl fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPool")
            .field("capacity", &self.capacity())
            .field("buffer_size", &self.buffer_size)
            .field("in_flight", &self.in_flight())
            .finish_non_exhaustive()
    }
}

/// A buffer rented from a [`BufferPool`].
///
/// Ownership is intentionally explicit: the buffer is removed from the pool's
/// free list on [`BufferPool::acquire`] and must be handed back via
/// [`BufferPool::release`]. Dropping a `PooledBuffer` without releasing it
/// leaks the slot until pool teardown; callers that may panic should hold the
/// buffer in a guard or release on the unwinding path.
pub struct PooledBuffer {
    id: BufferId,
    bytes: Box<[u8]>,
    init_len: usize,
}

impl PooledBuffer {
    /// Returns the buffer's pool index.
    #[must_use]
    pub fn id(&self) -> BufferId {
        self.id
    }

    /// Returns the byte length of the initialised prefix (set by writers).
    #[must_use]
    pub fn init_len(&self) -> usize {
        self.init_len
    }

    /// Returns the buffer's full capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.bytes.len()
    }

    /// Initialised prefix as a slice.
    #[must_use]
    pub fn filled(&self) -> &[u8] {
        &self.bytes[..self.init_len]
    }
}

impl fmt::Debug for PooledBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledBuffer")
            .field("id", &self.id)
            .field("init_len", &self.init_len)
            .field("capacity", &self.bytes.len())
            .finish()
    }
}

// SAFETY: `PooledBuffer` owns its backing `Box<[u8]>` for its entire lifetime;
// `read_ptr` returns a pointer into that allocation, valid until the buffer is
// dropped. `bytes_init` and `bytes_total` reflect the actual storage.
unsafe impl monoio::buf::IoBuf for PooledBuffer {
    fn read_ptr(&self) -> *const u8 {
        self.bytes.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.init_len
    }
}

// SAFETY: As above; `write_ptr` exposes the same allocation mutably. `set_init`
// records how many leading bytes the kernel wrote and is the only path by
// which `init_len` increases on the read side.
unsafe impl monoio::buf::IoBufMut for PooledBuffer {
    fn write_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    fn bytes_total(&mut self) -> usize {
        self.bytes.len()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        debug_assert!(pos <= self.bytes.len(), "set_init beyond capacity");
        self.init_len = pos;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_round_trip() {
        let mut pool = BufferPool::new(4, 64);
        assert_eq!(pool.capacity(), 4);
        assert_eq!(pool.in_flight(), 0);

        let a = pool.acquire().expect("acquire 0");
        let b = pool.acquire().expect("acquire 1");
        assert_eq!(pool.in_flight(), 2);
        assert_ne!(a.id(), b.id());

        pool.release(a);
        pool.release(b);
        assert_eq!(pool.in_flight(), 0);
    }

    #[test]
    fn exhaustion_returns_none() {
        let mut pool = BufferPool::new(2, 64);
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        assert!(pool.acquire().is_none());
        pool.release(a);
        let c = pool.acquire().expect("after release");
        pool.release(b);
        pool.release(c);
        assert_eq!(pool.in_flight(), 0);
    }

    #[test]
    fn buffer_size_matches() {
        let pool = BufferPool::new(1, 256);
        assert_eq!(pool.buffer_size(), 256);
    }
}
