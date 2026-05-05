#![no_main]
//! Fuzz target for the wire codec.
//!
//! Feeds arbitrary bytes into [`aulon_proto::parse_frame`]. The parser
//! must not panic, abort, overflow, or deref out of bounds for any
//! input — only `Frame`, `NeedMore`, or `Err` are legal outcomes.
//!
//! Run with:
//!
//! ```
//! cargo +nightly fuzz run parse_frame -- -max_total_time=60
//! ```
//!
//! The C2 gate is >1M iterations with no findings.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = aulon_proto::parse_frame(data);
});
