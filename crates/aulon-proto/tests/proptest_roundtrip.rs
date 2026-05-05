//! Property tests for the wire codec.
//!
//! For any value-shape that maps onto a [`Frame`] variant, generate
//! random instances, emit them, parse the result, and assert the
//! parsed frame equals the original. The codec's correctness contract
//! is `parse(emit(frame)) == frame` for every legal frame.
//!
//! See `docs/design/wire-codec.md`.

#![allow(missing_docs)]

use aulon_proto::{
    emit_connect, emit_err, emit_frame, emit_info, emit_msg, emit_pub, emit_sub, emit_unsub,
    parse_frame, Frame, ParseOutcome,
};
use proptest::prelude::*;

// === generators ============================================================

fn token_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::string::string_regex(r"[A-Za-z0-9._\-*>]{1,64}")
        .unwrap()
        .prop_map(String::into_bytes)
}

fn opt_token_bytes() -> impl Strategy<Value = Option<Vec<u8>>> {
    proptest::option::of(token_bytes())
}

fn payload_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..=512)
}

fn json_body() -> impl Strategy<Value = Vec<u8>> {
    // Arbitrary printable ASCII excluding CR / LF, sized 1..=128.
    proptest::string::string_regex(r"[\x20-\x7E&&[^\r\n]]{1,128}")
        .unwrap()
        .prop_map(String::into_bytes)
}

fn err_message() -> impl Strategy<Value = Vec<u8>> {
    proptest::string::string_regex(r"[\x20-\x7E&&[^\r\n]]{1,128}")
        .unwrap()
        .prop_map(String::into_bytes)
}

// === one round-trip helper =================================================

const SCRATCH: usize = 8192;

fn roundtrip_eq(frame: &Frame<'_>) {
    let mut buf = vec![0u8; SCRATCH];
    let n = emit_frame(&mut buf, frame).expect("emit fits in scratch");
    match parse_frame(&buf[..n]) {
        ParseOutcome::Frame {
            frame: parsed,
            consumed,
        } => {
            assert_eq!(consumed, n, "consumed must equal emitted length");
            assert_eq!(&parsed, frame);
        }
        other => panic!("re-parse failed: {other:?} for frame {frame:?}"),
    }
}

// === per-variant round-trip properties =====================================

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn ping_roundtrips(_: ()) {
        roundtrip_eq(&Frame::Ping);
    }

    #[test]
    fn pong_roundtrips(_: ()) {
        roundtrip_eq(&Frame::Pong);
    }

    #[test]
    fn ok_roundtrips(_: ()) {
        roundtrip_eq(&Frame::Ok);
    }

    #[test]
    fn err_roundtrips(message in err_message()) {
        roundtrip_eq(&Frame::Err { message: &message });
    }

    #[test]
    fn connect_roundtrips(options in json_body()) {
        roundtrip_eq(&Frame::Connect { options: &options });
    }

    #[test]
    fn info_roundtrips(options in json_body()) {
        roundtrip_eq(&Frame::Info { options: &options });
    }

    #[test]
    fn sub_roundtrips(
        subject in token_bytes(),
        queue in opt_token_bytes(),
        sid in token_bytes(),
    ) {
        roundtrip_eq(&Frame::Sub {
            subject: &subject,
            queue_group: queue.as_deref(),
            sid: &sid,
        });
    }

    #[test]
    fn unsub_roundtrips(
        sid in token_bytes(),
        max in proptest::option::of(any::<u64>()),
    ) {
        roundtrip_eq(&Frame::Unsub {
            sid: &sid,
            max_msgs: max,
        });
    }

    #[test]
    fn pub_roundtrips(
        subject in token_bytes(),
        reply in opt_token_bytes(),
        payload in payload_bytes(),
    ) {
        roundtrip_eq(&Frame::Pub {
            subject: &subject,
            reply_to: reply.as_deref(),
            payload: &payload,
        });
    }

    #[test]
    fn msg_roundtrips(
        subject in token_bytes(),
        sid in token_bytes(),
        reply in opt_token_bytes(),
        payload in payload_bytes(),
    ) {
        roundtrip_eq(&Frame::Msg {
            subject: &subject,
            sid: &sid,
            reply_to: reply.as_deref(),
            payload: &payload,
        });
    }
}

// === parser robustness =====================================================
//
// Even on bytes that are *not* a legal frame, the parser must not
// panic. Returning `Err`, `NeedMore`, or `Frame` are all acceptable
// outcomes; abort / overflow / divide-by-zero are not.

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1024,
        ..ProptestConfig::default()
    })]

    #[test]
    fn parser_does_not_panic_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..=2048)) {
        let _ = parse_frame(&bytes);
    }
}

// === per-emitter buffer-too-small property =================================

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    #[test]
    fn emit_short_buffer_returns_err_not_panic(message in err_message()) {
        let mut tiny = [0u8; 2];
        let _ = emit_err(&mut tiny, &message);
        let _ = emit_info(&mut tiny, &message);
        let _ = emit_connect(&mut tiny, &message);
        let _ = emit_sub(&mut tiny, &message, None, b"7");
        let _ = emit_unsub(&mut tiny, b"7", Some(12));
        let _ = emit_pub(&mut tiny, b"foo", None, &message);
        let _ = emit_msg(&mut tiny, b"foo", b"7", None, &message);
    }
}
