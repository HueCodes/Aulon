//! Core runtime, buffer pool, routing, and topology for Aulon.
//!
//! Per-core fixed-buffer pool registered against `io_uring`; subscription
//! state sharded by L3 cache domain.

pub mod buffer_pool;
pub mod connection;
pub mod subscription;

pub use buffer_pool::{BufferPool, DEFAULT_BUFFER_SIZE, DEFAULT_POOL_CAPACITY};
pub use connection::{Active, Closing, Connection, ReadOutcome, State};
pub use subscription::{ConnectionId, Sub, SubscriptionTable};
