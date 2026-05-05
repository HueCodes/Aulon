//! NATS-core wire protocol codec for Aulon.
//!
//! Allocation-free, borrowed-slice parser and emitter over the NATS-core
//! verb subset listed in `docs/SCOPE.md`. The crate has no I/O, no async,
//! no runtime dependency: it operates exclusively on `&[u8]` (parse) and
//! `&mut [u8]` (emit).
//!
//! See `docs/design/wire-codec.md` for the design rationale.
//!
//! # Quick example
//!
//! ```
//! use aulon_proto::{parse_frame, Frame, ParseOutcome};
//!
//! let buf = b"PING\r\n";
//! match parse_frame(buf) {
//!     ParseOutcome::Frame { frame: Frame::Ping, consumed } => {
//!         assert_eq!(consumed, buf.len());
//!     }
//!     other => panic!("unexpected outcome: {other:?}"),
//! }
//! ```

#![forbid(unsafe_code)]
#![cfg_attr(not(test), no_std)]

pub mod error;
pub mod frame;
pub mod parse;

pub use error::{EmitError, ParseError};
pub use frame::Frame;
pub use parse::{parse_frame, ParseOutcome};
