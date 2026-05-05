//! Codec error types.

use core::fmt;

/// Errors returned by [`crate::parse::parse_frame`].
///
/// The set is intentionally small. Anything not classified here is a class
/// we have not yet seen via fuzzing; new variants are added as fuzz finds
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// Verb keyword does not match any verb in `docs/SCOPE.md`.
    UnknownVerb,
    /// Header line is malformed: wrong arity, missing field, bad
    /// whitespace, etc.
    BadHeader,
    /// `PUB` / `MSG` byte count does not fit a `u64` or is otherwise
    /// nonsensical (e.g., empty or non-digit).
    PayloadLengthInvalid,
    /// `UNSUB` `max_msgs` field is not a valid `u64`.
    MaxMsgsInvalid,
    /// `PUB` / `MSG` payload is missing its trailing `\r\n` terminator.
    MissingTrailingCrlf,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownVerb => f.write_str("unknown verb"),
            Self::BadHeader => f.write_str("malformed header line"),
            Self::PayloadLengthInvalid => f.write_str("invalid payload length"),
            Self::MaxMsgsInvalid => f.write_str("invalid UNSUB max_msgs"),
            Self::MissingTrailingCrlf => f.write_str("missing trailing CRLF after payload"),
        }
    }
}

/// Errors returned by `emit_*` functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitError {
    /// Output buffer is too small to hold the encoded frame.
    BufferTooSmall {
        /// Bytes required.
        needed: usize,
        /// Bytes available.
        have: usize,
    },
}

impl fmt::Display for EmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall { needed, have } => {
                write!(f, "buffer too small: needed {needed}, have {have}")
            }
        }
    }
}
