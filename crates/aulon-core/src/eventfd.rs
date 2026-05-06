//! Cross-thread wake primitive backed by Linux `eventfd`.
//!
//! The reader side is single-owner and converts into a
//! [`tokio_uring::fs::File`]; the waker side is `Arc`-cloned across
//! peer shards' worker threads. The two sides reference the same
//! kernel `eventfd` object via `dup(2)`, so writes from any waker
//! accumulate into the 64-bit counter the reader sees.
//!
//! See `docs/design/topology-sharding.md` for how this pairs with the
//! cross-shard inbox.

#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// The single-owner read side of an eventfd pair.
#[derive(Debug)]
pub struct EventfdReader {
    file: File,
}

impl EventfdReader {
    /// Convert into a `tokio_uring::fs::File`. The eventfd's nonblocking
    /// flag is preserved; reads of 8 bytes return the accumulated
    /// counter or fail with `EAGAIN` if the counter is zero.
    #[must_use]
    pub fn into_uring_file(self) -> tokio_uring::fs::File {
        tokio_uring::fs::File::from_std(self.file)
    }
}

/// The Arc-shareable write side of an eventfd pair.
#[derive(Debug)]
pub struct EventfdWaker {
    fd: OwnedFd,
}

impl EventfdWaker {
    /// Increment the eventfd counter by 1. Idempotent for our purposes:
    /// kicking an already-pending consumer is harmless.
    pub fn wake(&self) -> io::Result<()> {
        let one = 1u64.to_ne_bytes();
        // Safety: write(2) on a valid eventfd with an 8-byte buffer is
        // the documented protocol. `self.fd` owns the descriptor.
        let n = unsafe { libc::write(self.fd.as_raw_fd(), one.as_ptr().cast::<libc::c_void>(), 8) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// Create a paired eventfd reader and waker. Both sides reference the
/// same kernel object via `dup(2)`.
pub fn eventfd_pair() -> io::Result<(EventfdReader, EventfdWaker)> {
    // Safety: eventfd(2) returns a fresh file descriptor on success or
    // -1 on failure. Wrap immediately in owned types.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // Safety: dup(2) of a fresh, owned fd returns another fd or -1.
    let dup = unsafe { libc::dup(fd) };
    if dup < 0 {
        let err = io::Error::last_os_error();
        // Safety: `fd` is owned by us and not yet wrapped; closing it
        // is correct on the dup-failure path.
        unsafe {
            libc::close(fd);
        }
        return Err(err);
    }
    let reader = EventfdReader {
        // Safety: `fd` is a valid file descriptor we own.
        file: unsafe { File::from_raw_fd(fd) },
    };
    let waker = EventfdWaker {
        // Safety: `dup` is a valid file descriptor we own.
        fd: unsafe { OwnedFd::from_raw_fd(dup) },
    };
    Ok((reader, waker))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn pair_round_trips_a_wake() {
        let (reader, waker) = eventfd_pair().expect("eventfd_pair");
        // Bump the counter twice; reading once should yield 2.
        waker.wake().expect("wake 1");
        waker.wake().expect("wake 2");
        // Read directly via the std::fs::File so the test does not pull
        // in the tokio_uring runtime.
        let mut buf = [0u8; 8];
        let mut file = reader.file;
        let n = file.read(&mut buf).expect("read eventfd");
        assert_eq!(n, 8);
        assert_eq!(u64::from_ne_bytes(buf), 2);
    }

    #[test]
    fn empty_eventfd_read_returns_eagain() {
        let (reader, _waker) = eventfd_pair().expect("eventfd_pair");
        let mut buf = [0u8; 8];
        let mut file = reader.file;
        let err = file
            .read(&mut buf)
            .expect_err("non-blocking read on empty eventfd must error");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }
}
