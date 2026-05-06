//! Core runtime, buffer pool, routing, and topology for Aulon.
//!
//! Per-core fixed-buffer pool registered against `io_uring`; subscription
//! state sharded by L3 cache domain.

// The buffer pool, connection, and connection-state modules pull in
// tokio-uring and tokio, which both have their own internal `cfg(loom)`
// gates that interact badly with our top-level `--cfg loom` build.
// They are not under test from loom; gate them out so the loom test
// binary links a minimal aulon-core surface.
#[cfg(not(loom))]
pub mod buffer_pool;
#[cfg(not(loom))]
pub mod connection;
#[cfg(not(loom))]
pub mod connection_state;
#[cfg(not(loom))]
pub mod eventfd;
pub mod shard_inbox;
pub mod subscription;
pub mod topology;

#[cfg(not(loom))]
pub use buffer_pool::{BufferPool, DEFAULT_BUFFER_SIZE, DEFAULT_POOL_CAPACITY};
#[cfg(not(loom))]
pub use connection::{Active, Closing, Connection, ReadOutcome, State};
#[cfg(not(loom))]
pub use connection_state::{
    CloseReason, ConnectionState, EnqueueOutcome, DEFAULT_OUTBOUND_CAPACITY,
};
#[cfg(not(loom))]
pub use eventfd::{eventfd_pair, EventfdReader, EventfdWaker};
pub use shard_inbox::{PublishedFrame, ShardInbox, ShardInboxFull};
pub use subscription::{ConnectionId, Sub, SubjectError, SubscriptionTrie};
pub use topology::{Shard, Topology};
