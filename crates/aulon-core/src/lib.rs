//! Core runtime, buffer pool, routing, and topology for Aulon.
//!
//! Per-core fixed-buffer pool registered against `io_uring`; subscription
//! state sharded by L3 cache domain. The only crate permitted to use
//! `unsafe`, and only inside the `driver` module (introduced in C1).
//!
//! Empty until checkpoint C1.
