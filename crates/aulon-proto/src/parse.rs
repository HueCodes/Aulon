//! Streaming, allocation-free NATS-core wire parser.

use crate::error::ParseError;
use crate::frame::Frame;

/// Outcome of one [`parse_frame`] call against a borrowed input buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseOutcome<'a> {
    /// A complete frame was parsed; advance the buffer by `consumed`
    /// bytes before the next call.
    Frame {
        /// The parsed frame, with field slices borrowed from `buf`.
        frame: Frame<'a>,
        /// Number of bytes that made up this frame in `buf`.
        consumed: usize,
    },
    /// `buf` does not yet contain a full frame; the caller should append
    /// more bytes from the wire and call `parse_frame` again.
    NeedMore,
    /// `buf` is malformed in a way that does not improve with more data;
    /// the caller should close the connection.
    Err(ParseError),
}

const CRLF: &[u8] = b"\r\n";

/// Parses one frame from the head of `buf`.
#[must_use]
pub fn parse_frame(buf: &[u8]) -> ParseOutcome<'_> {
    let Some(line_end) = find_crlf(buf) else {
        return ParseOutcome::NeedMore;
    };
    let header_total = line_end + CRLF.len();
    let line = &buf[..line_end];

    let (verb, rest) = split_verb(line);
    match verb {
        b"PING" => header_only(Frame::Ping, rest, header_total),
        b"PONG" => header_only(Frame::Pong, rest, header_total),
        b"+OK" => header_only(Frame::Ok, rest, header_total),
        b"-ERR" => parse_err(rest, header_total),
        b"CONNECT" => parse_connect(rest, header_total),
        b"INFO" => parse_info(rest, header_total),
        b"SUB" => parse_sub(rest, header_total),
        b"UNSUB" => parse_unsub(rest, header_total),
        b"PUB" => parse_pub(buf, rest, header_total),
        b"MSG" => parse_msg(buf, rest, header_total),
        _ => ParseOutcome::Err(ParseError::UnknownVerb),
    }
}

// === verb parsers ============================================================

fn header_only<'a>(frame: Frame<'a>, rest: &[u8], header_total: usize) -> ParseOutcome<'a> {
    if !rest.is_empty() {
        return ParseOutcome::Err(ParseError::BadHeader);
    }
    ParseOutcome::Frame {
        frame,
        consumed: header_total,
    }
}

fn parse_err(rest: &[u8], header_total: usize) -> ParseOutcome<'_> {
    // `rest` is the message verbatim; `split_verb` already consumed the
    // single separator that follows `-ERR`. We do NOT trim further
    // leading whitespace because the message itself may legitimately
    // start with a space (the codec must round-trip such messages).
    ParseOutcome::Frame {
        frame: Frame::Err { message: rest },
        consumed: header_total,
    }
}

fn parse_connect(rest: &[u8], header_total: usize) -> ParseOutcome<'_> {
    if rest.is_empty() {
        return ParseOutcome::Err(ParseError::BadHeader);
    }
    ParseOutcome::Frame {
        frame: Frame::Connect { options: rest },
        consumed: header_total,
    }
}

fn parse_info(rest: &[u8], header_total: usize) -> ParseOutcome<'_> {
    if rest.is_empty() {
        return ParseOutcome::Err(ParseError::BadHeader);
    }
    ParseOutcome::Frame {
        frame: Frame::Info { options: rest },
        consumed: header_total,
    }
}

fn parse_sub(rest: &[u8], header_total: usize) -> ParseOutcome<'_> {
    // SUB <subject> [queue] <sid>
    let mut tokens = TokenIter::new(rest);
    let Some(subject) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let Some(second) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let third = tokens.next();
    let extra = tokens.next();
    if extra.is_some() {
        return ParseOutcome::Err(ParseError::BadHeader);
    }
    let (queue_group, sid) = match third {
        Some(sid) => (Some(second), sid),
        None => (None, second),
    };
    ParseOutcome::Frame {
        frame: Frame::Sub {
            subject,
            queue_group,
            sid,
        },
        consumed: header_total,
    }
}

fn parse_unsub(rest: &[u8], header_total: usize) -> ParseOutcome<'_> {
    // UNSUB <sid> [max_msgs]
    let mut tokens = TokenIter::new(rest);
    let Some(sid) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let max_msgs = match tokens.next() {
        Some(token) => match parse_u64(token) {
            Some(n) => Some(n),
            None => return ParseOutcome::Err(ParseError::MaxMsgsInvalid),
        },
        None => None,
    };
    if tokens.next().is_some() {
        return ParseOutcome::Err(ParseError::BadHeader);
    }
    ParseOutcome::Frame {
        frame: Frame::Unsub { sid, max_msgs },
        consumed: header_total,
    }
}

fn parse_pub<'a>(buf: &'a [u8], rest: &'a [u8], header_total: usize) -> ParseOutcome<'a> {
    // PUB <subject> [reply-to] <#bytes>
    let mut tokens = TokenIter::new(rest);
    let Some(subject) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let Some(second) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let third = tokens.next();
    if tokens.next().is_some() {
        return ParseOutcome::Err(ParseError::BadHeader);
    }
    let (reply_to, count_token) = match third {
        Some(count) => (Some(second), count),
        None => (None, second),
    };
    let Some(payload_len) = parse_usize(count_token) else {
        return ParseOutcome::Err(ParseError::PayloadLengthInvalid);
    };
    let Some((payload, total)) = consume_payload(buf, header_total, payload_len) else {
        return need_more_or_missing_crlf(buf, header_total, payload_len);
    };
    ParseOutcome::Frame {
        frame: Frame::Pub {
            subject,
            reply_to,
            payload,
        },
        consumed: total,
    }
}

fn parse_msg<'a>(buf: &'a [u8], rest: &'a [u8], header_total: usize) -> ParseOutcome<'a> {
    // MSG <subject> <sid> [reply-to] <#bytes>
    let mut tokens = TokenIter::new(rest);
    let Some(subject) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let Some(sid) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let Some(third) = tokens.next() else {
        return ParseOutcome::Err(ParseError::BadHeader);
    };
    let fourth = tokens.next();
    if tokens.next().is_some() {
        return ParseOutcome::Err(ParseError::BadHeader);
    }
    let (reply_to, count_token) = match fourth {
        Some(count) => (Some(third), count),
        None => (None, third),
    };
    let Some(payload_len) = parse_usize(count_token) else {
        return ParseOutcome::Err(ParseError::PayloadLengthInvalid);
    };
    let Some((payload, total)) = consume_payload(buf, header_total, payload_len) else {
        return need_more_or_missing_crlf(buf, header_total, payload_len);
    };
    ParseOutcome::Frame {
        frame: Frame::Msg {
            subject,
            sid,
            reply_to,
            payload,
        },
        consumed: total,
    }
}

// === byte-level helpers =====================================================

fn find_crlf(buf: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn split_verb(line: &[u8]) -> (&[u8], &[u8]) {
    // Consume exactly one separator byte after the verb. Verb parsers
    // for `-ERR` / `CONNECT` / `INFO` then take `rest` verbatim as the
    // message / JSON body, preserving any leading whitespace that
    // legitimately belongs to the payload. Multi-token verbs
    // (`SUB`, `PUB`, …) drive `TokenIter`, which strips additional
    // leading whitespace before each token.
    match line.iter().position(|b| *b == b' ' || *b == b'\t') {
        Some(idx) => (&line[..idx], &line[idx + 1..]),
        None => (line, &[]),
    }
}

fn trim_leading_spaces(s: &[u8]) -> &[u8] {
    let mut i = 0;
    while i < s.len() && (s[i] == b' ' || s[i] == b'\t') {
        i += 1;
    }
    &s[i..]
}

/// Iterates whitespace-separated tokens within a single header line.
struct TokenIter<'a> {
    rest: &'a [u8],
}

impl<'a> TokenIter<'a> {
    fn new(rest: &'a [u8]) -> Self {
        Self { rest }
    }
}

impl<'a> Iterator for TokenIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        self.rest = trim_leading_spaces(self.rest);
        if self.rest.is_empty() {
            return None;
        }
        let end = self
            .rest
            .iter()
            .position(|b| *b == b' ' || *b == b'\t')
            .unwrap_or(self.rest.len());
        let token = &self.rest[..end];
        self.rest = &self.rest[end..];
        Some(token)
    }
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut n: usize = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(usize::from(b - b'0'))?;
    }
    Some(n)
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    Some(n)
}

/// On success, returns the payload slice and the total bytes consumed
/// (header + payload + trailing CRLF). Returns `None` if the buffer does
/// not yet contain the payload + trailing CRLF.
fn consume_payload(buf: &[u8], header_total: usize, payload_len: usize) -> Option<(&[u8], usize)> {
    let payload_end = header_total.checked_add(payload_len)?;
    let total = payload_end.checked_add(CRLF.len())?;
    if buf.len() < total {
        return None;
    }
    if &buf[payload_end..total] != CRLF {
        return None;
    }
    Some((&buf[header_total..payload_end], total))
}

fn need_more_or_missing_crlf(buf: &[u8], header_total: usize, payload_len: usize) -> ParseOutcome<'_> {
    let Some(payload_end) = header_total.checked_add(payload_len) else {
        return ParseOutcome::Err(ParseError::PayloadLengthInvalid);
    };
    let Some(total) = payload_end.checked_add(CRLF.len()) else {
        return ParseOutcome::Err(ParseError::PayloadLengthInvalid);
    };
    if buf.len() < total {
        ParseOutcome::NeedMore
    } else {
        // Length is satisfied but trailing CRLF is wrong.
        ParseOutcome::Err(ParseError::MissingTrailingCrlf)
    }
}

// === tests ==================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_complete(input: &[u8]) -> Frame<'_> {
        match parse_frame(input) {
            ParseOutcome::Frame { frame, consumed } => {
                assert_eq!(consumed, input.len(), "consumed should match input length");
                frame
            }
            other => panic!("expected Frame, got {other:?}"),
        }
    }

    #[test]
    fn ping_pong_ok() {
        assert_eq!(parse_complete(b"PING\r\n"), Frame::Ping);
        assert_eq!(parse_complete(b"PONG\r\n"), Frame::Pong);
        assert_eq!(parse_complete(b"+OK\r\n"), Frame::Ok);
    }

    #[test]
    fn err_carries_message() {
        assert_eq!(
            parse_complete(b"-ERR slow consumer\r\n"),
            Frame::Err {
                message: b"slow consumer"
            }
        );
    }

    #[test]
    fn connect_carries_options_bytes() {
        let f = parse_complete(b"CONNECT {\"verbose\":false}\r\n");
        let Frame::Connect { options } = f else {
            panic!("expected Connect, got {f:?}");
        };
        assert_eq!(options, b"{\"verbose\":false}");
    }

    #[test]
    fn info_carries_options_bytes() {
        let f = parse_complete(b"INFO {\"server_id\":\"x\"}\r\n");
        let Frame::Info { options } = f else {
            panic!("expected Info, got {f:?}");
        };
        assert_eq!(options, b"{\"server_id\":\"x\"}");
    }

    #[test]
    fn sub_without_queue() {
        assert_eq!(
            parse_complete(b"SUB foo.bar 7\r\n"),
            Frame::Sub {
                subject: b"foo.bar",
                queue_group: None,
                sid: b"7",
            }
        );
    }

    #[test]
    fn sub_with_queue() {
        assert_eq!(
            parse_complete(b"SUB foo.bar workers 7\r\n"),
            Frame::Sub {
                subject: b"foo.bar",
                queue_group: Some(b"workers"),
                sid: b"7",
            }
        );
    }

    #[test]
    fn unsub_without_max_msgs() {
        assert_eq!(
            parse_complete(b"UNSUB 7\r\n"),
            Frame::Unsub {
                sid: b"7",
                max_msgs: None,
            }
        );
    }

    #[test]
    fn unsub_with_max_msgs() {
        assert_eq!(
            parse_complete(b"UNSUB 7 12\r\n"),
            Frame::Unsub {
                sid: b"7",
                max_msgs: Some(12),
            }
        );
    }

    #[test]
    fn pub_without_reply() {
        assert_eq!(
            parse_complete(b"PUB foo 5\r\nhello\r\n"),
            Frame::Pub {
                subject: b"foo",
                reply_to: None,
                payload: b"hello",
            }
        );
    }

    #[test]
    fn pub_with_reply() {
        assert_eq!(
            parse_complete(b"PUB foo INBOX.42 5\r\nhello\r\n"),
            Frame::Pub {
                subject: b"foo",
                reply_to: Some(b"INBOX.42"),
                payload: b"hello",
            }
        );
    }

    #[test]
    fn msg_without_reply() {
        assert_eq!(
            parse_complete(b"MSG foo 7 5\r\nhello\r\n"),
            Frame::Msg {
                subject: b"foo",
                sid: b"7",
                reply_to: None,
                payload: b"hello",
            }
        );
    }

    #[test]
    fn msg_with_reply() {
        assert_eq!(
            parse_complete(b"MSG foo 7 INBOX.42 5\r\nhello\r\n"),
            Frame::Msg {
                subject: b"foo",
                sid: b"7",
                reply_to: Some(b"INBOX.42"),
                payload: b"hello",
            }
        );
    }

    #[test]
    fn empty_payload_is_legal() {
        assert_eq!(
            parse_complete(b"PUB foo 0\r\n\r\n"),
            Frame::Pub {
                subject: b"foo",
                reply_to: None,
                payload: b"",
            }
        );
    }

    #[test]
    fn need_more_when_header_truncated() {
        assert_eq!(parse_frame(b"PIN"), ParseOutcome::NeedMore);
        assert_eq!(parse_frame(b"PUB foo 5\r\nhel"), ParseOutcome::NeedMore);
        assert_eq!(
            parse_frame(b"PUB foo 5\r\nhello"),
            ParseOutcome::NeedMore,
            "trailing CRLF not yet present"
        );
    }

    #[test]
    fn unknown_verb_fails() {
        assert!(matches!(
            parse_frame(b"WAT\r\n"),
            ParseOutcome::Err(ParseError::UnknownVerb)
        ));
    }

    #[test]
    fn missing_trailing_crlf_after_full_payload() {
        // Payload length satisfied but the two bytes after the payload
        // are not "\r\n".
        let buf = b"PUB foo 5\r\nhelloXY";
        assert!(matches!(
            parse_frame(buf),
            ParseOutcome::Err(ParseError::MissingTrailingCrlf)
        ));
    }

    #[test]
    fn ping_with_extra_bytes_is_bad_header() {
        assert!(matches!(
            parse_frame(b"PING junk\r\n"),
            ParseOutcome::Err(ParseError::BadHeader)
        ));
    }

    #[test]
    fn parses_back_to_back_frames() {
        let buf = b"PING\r\nPONG\r\n";
        let ParseOutcome::Frame { frame, consumed } = parse_frame(buf) else {
            panic!("first parse failed");
        };
        assert_eq!(frame, Frame::Ping);
        assert_eq!(consumed, 6);
        let rest = &buf[consumed..];
        let ParseOutcome::Frame { frame, consumed } = parse_frame(rest) else {
            panic!("second parse failed");
        };
        assert_eq!(frame, Frame::Pong);
        assert_eq!(consumed, 6);
    }

    #[test]
    fn invalid_payload_length_rejected() {
        assert!(matches!(
            parse_frame(b"PUB foo abc\r\n"),
            ParseOutcome::Err(ParseError::PayloadLengthInvalid)
        ));
    }

    #[test]
    fn invalid_max_msgs_rejected() {
        assert!(matches!(
            parse_frame(b"UNSUB 7 abc\r\n"),
            ParseOutcome::Err(ParseError::MaxMsgsInvalid)
        ));
    }
}
