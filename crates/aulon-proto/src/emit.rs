//! Allocation-free NATS-core wire emitter.
//!
//! Each `emit_*` function writes the encoded frame into a caller-provided
//! `&mut [u8]` and returns the number of bytes written, or
//! [`EmitError::BufferTooSmall`] if the output is too short.

use crate::error::EmitError;
use crate::frame::Frame;

const CRLF: &[u8] = b"\r\n";
const SP: u8 = b' ';

/// Maximum bytes a `u64` decimal expansion can occupy.
const U64_DIGITS_MAX: usize = 20;

/// Emits any [`Frame`] into `out`, returning bytes written.
///
/// # Errors
/// [`EmitError::BufferTooSmall`] if `out` cannot hold the encoded frame.
pub fn emit_frame(out: &mut [u8], frame: &Frame<'_>) -> Result<usize, EmitError> {
    match *frame {
        Frame::Ping => emit_ping(out),
        Frame::Pong => emit_pong(out),
        Frame::Ok => emit_ok(out),
        Frame::Err { message } => emit_err(out, message),
        Frame::Info { options } => emit_info(out, options),
        Frame::Connect { options } => emit_connect(out, options),
        Frame::Sub {
            subject,
            queue_group,
            sid,
        } => emit_sub(out, subject, queue_group, sid),
        Frame::Unsub { sid, max_msgs } => emit_unsub(out, sid, max_msgs),
        Frame::Pub {
            subject,
            reply_to,
            payload,
        } => emit_pub(out, subject, reply_to, payload),
        Frame::Msg {
            subject,
            sid,
            reply_to,
            payload,
        } => emit_msg(out, subject, sid, reply_to, payload),
    }
}

/// Emits `PING\r\n`.
pub fn emit_ping(out: &mut [u8]) -> Result<usize, EmitError> {
    write_all(out, 0, b"PING\r\n")
}

/// Emits `PONG\r\n`.
pub fn emit_pong(out: &mut [u8]) -> Result<usize, EmitError> {
    write_all(out, 0, b"PONG\r\n")
}

/// Emits `+OK\r\n`.
pub fn emit_ok(out: &mut [u8]) -> Result<usize, EmitError> {
    write_all(out, 0, b"+OK\r\n")
}

/// Emits `-ERR <message>\r\n`.
pub fn emit_err(out: &mut [u8], message: &[u8]) -> Result<usize, EmitError> {
    let mut n = 0;
    n = write_all(out, n, b"-ERR ")?;
    n = write_all(out, n, message)?;
    n = write_all(out, n, CRLF)?;
    Ok(n)
}

/// Emits `INFO <options>\r\n`. The options bytes are written verbatim.
pub fn emit_info(out: &mut [u8], options: &[u8]) -> Result<usize, EmitError> {
    let mut n = 0;
    n = write_all(out, n, b"INFO ")?;
    n = write_all(out, n, options)?;
    n = write_all(out, n, CRLF)?;
    Ok(n)
}

/// Emits `CONNECT <options>\r\n`. The options bytes are written verbatim.
pub fn emit_connect(out: &mut [u8], options: &[u8]) -> Result<usize, EmitError> {
    let mut n = 0;
    n = write_all(out, n, b"CONNECT ")?;
    n = write_all(out, n, options)?;
    n = write_all(out, n, CRLF)?;
    Ok(n)
}

/// Emits `SUB <subject> [queue] <sid>\r\n`.
pub fn emit_sub(
    out: &mut [u8],
    subject: &[u8],
    queue_group: Option<&[u8]>,
    sid: &[u8],
) -> Result<usize, EmitError> {
    let mut n = 0;
    n = write_all(out, n, b"SUB ")?;
    n = write_all(out, n, subject)?;
    n = write_byte(out, n, SP)?;
    if let Some(q) = queue_group {
        n = write_all(out, n, q)?;
        n = write_byte(out, n, SP)?;
    }
    n = write_all(out, n, sid)?;
    n = write_all(out, n, CRLF)?;
    Ok(n)
}

/// Emits `UNSUB <sid> [max_msgs]\r\n`.
pub fn emit_unsub(
    out: &mut [u8],
    sid: &[u8],
    max_msgs: Option<u64>,
) -> Result<usize, EmitError> {
    let mut n = 0;
    n = write_all(out, n, b"UNSUB ")?;
    n = write_all(out, n, sid)?;
    if let Some(m) = max_msgs {
        n = write_byte(out, n, SP)?;
        n = write_decimal_u64(out, n, m)?;
    }
    n = write_all(out, n, CRLF)?;
    Ok(n)
}

/// Emits `PUB <subject> [reply] <#bytes>\r\n<payload>\r\n`.
pub fn emit_pub(
    out: &mut [u8],
    subject: &[u8],
    reply_to: Option<&[u8]>,
    payload: &[u8],
) -> Result<usize, EmitError> {
    let mut n = 0;
    n = write_all(out, n, b"PUB ")?;
    n = write_all(out, n, subject)?;
    n = write_byte(out, n, SP)?;
    if let Some(r) = reply_to {
        n = write_all(out, n, r)?;
        n = write_byte(out, n, SP)?;
    }
    n = write_decimal_u64(out, n, payload.len() as u64)?;
    n = write_all(out, n, CRLF)?;
    n = write_all(out, n, payload)?;
    n = write_all(out, n, CRLF)?;
    Ok(n)
}

/// Emits `MSG <subject> <sid> [reply] <#bytes>\r\n<payload>\r\n`.
pub fn emit_msg(
    out: &mut [u8],
    subject: &[u8],
    sid: &[u8],
    reply_to: Option<&[u8]>,
    payload: &[u8],
) -> Result<usize, EmitError> {
    let mut n = 0;
    n = write_all(out, n, b"MSG ")?;
    n = write_all(out, n, subject)?;
    n = write_byte(out, n, SP)?;
    n = write_all(out, n, sid)?;
    n = write_byte(out, n, SP)?;
    if let Some(r) = reply_to {
        n = write_all(out, n, r)?;
        n = write_byte(out, n, SP)?;
    }
    n = write_decimal_u64(out, n, payload.len() as u64)?;
    n = write_all(out, n, CRLF)?;
    n = write_all(out, n, payload)?;
    n = write_all(out, n, CRLF)?;
    Ok(n)
}

// === byte-level helpers =====================================================

fn write_all(out: &mut [u8], pos: usize, src: &[u8]) -> Result<usize, EmitError> {
    let end = pos
        .checked_add(src.len())
        .ok_or(EmitError::BufferTooSmall {
            needed: usize::MAX,
            have: out.len(),
        })?;
    if end > out.len() {
        return Err(EmitError::BufferTooSmall {
            needed: end,
            have: out.len(),
        });
    }
    out[pos..end].copy_from_slice(src);
    Ok(end)
}

fn write_byte(out: &mut [u8], pos: usize, byte: u8) -> Result<usize, EmitError> {
    if pos >= out.len() {
        return Err(EmitError::BufferTooSmall {
            needed: pos + 1,
            have: out.len(),
        });
    }
    out[pos] = byte;
    Ok(pos + 1)
}

fn write_decimal_u64(out: &mut [u8], pos: usize, mut value: u64) -> Result<usize, EmitError> {
    let mut tmp = [0u8; U64_DIGITS_MAX];
    let mut len = 0;
    if value == 0 {
        tmp[0] = b'0';
        len = 1;
    } else {
        while value > 0 {
            tmp[len] = b'0' + u8::try_from(value % 10).expect("0..=9 fits in u8");
            value /= 10;
            len += 1;
        }
    }
    let end = pos.checked_add(len).ok_or(EmitError::BufferTooSmall {
        needed: usize::MAX,
        have: out.len(),
    })?;
    if end > out.len() {
        return Err(EmitError::BufferTooSmall {
            needed: end,
            have: out.len(),
        });
    }
    for i in 0..len {
        out[pos + i] = tmp[len - 1 - i];
    }
    Ok(end)
}

// === tests ==================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{parse_frame, ParseOutcome};

    fn round_trip(frame: &Frame<'_>) {
        let mut buf = [0u8; 8192];
        let n = emit_frame(&mut buf, frame).expect("emit");
        let parsed = match parse_frame(&buf[..n]) {
            ParseOutcome::Frame { frame, consumed } => {
                assert_eq!(consumed, n, "consumed must match emitted length");
                frame
            }
            other => panic!("re-parse failed: {other:?}"),
        };
        assert_eq!(&parsed, frame);
    }

    #[test]
    fn round_trip_ping_pong_ok() {
        round_trip(&Frame::Ping);
        round_trip(&Frame::Pong);
        round_trip(&Frame::Ok);
    }

    #[test]
    fn round_trip_err() {
        round_trip(&Frame::Err {
            message: b"slow consumer",
        });
    }

    #[test]
    fn round_trip_connect_info() {
        round_trip(&Frame::Connect {
            options: b"{\"verbose\":false}",
        });
        round_trip(&Frame::Info {
            options: b"{\"server_id\":\"x\"}",
        });
    }

    #[test]
    fn round_trip_sub_with_and_without_queue() {
        round_trip(&Frame::Sub {
            subject: b"foo.bar",
            queue_group: None,
            sid: b"7",
        });
        round_trip(&Frame::Sub {
            subject: b"foo.bar",
            queue_group: Some(b"workers"),
            sid: b"7",
        });
    }

    #[test]
    fn round_trip_unsub() {
        round_trip(&Frame::Unsub {
            sid: b"7",
            max_msgs: None,
        });
        round_trip(&Frame::Unsub {
            sid: b"7",
            max_msgs: Some(12),
        });
    }

    #[test]
    fn round_trip_pub() {
        round_trip(&Frame::Pub {
            subject: b"foo",
            reply_to: None,
            payload: b"hello",
        });
        round_trip(&Frame::Pub {
            subject: b"foo",
            reply_to: Some(b"INBOX.42"),
            payload: b"hello",
        });
        round_trip(&Frame::Pub {
            subject: b"foo",
            reply_to: None,
            payload: b"",
        });
    }

    #[test]
    fn round_trip_msg() {
        round_trip(&Frame::Msg {
            subject: b"foo",
            sid: b"7",
            reply_to: None,
            payload: b"hello",
        });
        round_trip(&Frame::Msg {
            subject: b"foo",
            sid: b"7",
            reply_to: Some(b"INBOX.42"),
            payload: b"hello",
        });
    }

    #[test]
    fn buffer_too_small_returns_error() {
        let mut buf = [0u8; 4];
        let res = emit_ping(&mut buf);
        assert!(matches!(res, Err(EmitError::BufferTooSmall { .. })));
    }

    #[test]
    fn decimal_writer_handles_zero_and_max() {
        let mut buf = [0u8; 32];
        let n = write_decimal_u64(&mut buf, 0, 0).unwrap();
        assert_eq!(&buf[..n], b"0");
        let mut buf = [0u8; 32];
        let n = write_decimal_u64(&mut buf, 0, u64::MAX).unwrap();
        assert_eq!(&buf[..n], b"18446744073709551615");
    }
}
