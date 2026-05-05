//! Wire-frame data type.

/// A NATS-core wire frame, with all variable-length fields borrowed from
/// the input buffer.
///
/// `Frame<'a>` holds slices into the buffer the frame was parsed from;
/// the frame is valid only as long as that buffer is alive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame<'a> {
    /// `CONNECT { ... }\r\n` — client → server greeting.
    ///
    /// The codec does not parse the JSON body.
    Connect {
        /// JSON body bytes, without the leading `CONNECT ` and without
        /// the trailing `\r\n`.
        options: &'a [u8],
    },

    /// `PUB <subject> [reply-to] <#bytes>\r\n<payload>\r\n` —
    /// client-published message.
    Pub {
        /// Subject the message is published to.
        subject: &'a [u8],
        /// Optional reply subject.
        reply_to: Option<&'a [u8]>,
        /// Message payload bytes; length matches the header byte count.
        payload: &'a [u8],
    },

    /// `SUB <subject> [queue-group] <sid>\r\n` — client subscription.
    Sub {
        /// Subject to subscribe to (may include wildcards in C3+).
        subject: &'a [u8],
        /// Optional queue group; load-balanced delivery within the group.
        queue_group: Option<&'a [u8]>,
        /// Client-chosen subscription identifier.
        sid: &'a [u8],
    },

    /// `UNSUB <sid> [max_msgs]\r\n` — client unsubscribe.
    Unsub {
        /// Subscription identifier from the matching `SUB`.
        sid: &'a [u8],
        /// Optional auto-unsubscribe-after-N-messages count.
        max_msgs: Option<u64>,
    },

    /// `MSG <subject> <sid> [reply-to] <#bytes>\r\n<payload>\r\n` —
    /// server-delivered message.
    Msg {
        /// Subject the message was published to.
        subject: &'a [u8],
        /// Subscription identifier matched by the routing layer.
        sid: &'a [u8],
        /// Optional reply subject set by the publisher.
        reply_to: Option<&'a [u8]>,
        /// Message payload bytes; length matches the header byte count.
        payload: &'a [u8],
    },

    /// `PING\r\n` — keepalive request.
    Ping,

    /// `PONG\r\n` — keepalive response.
    Pong,

    /// `INFO { ... }\r\n` — server → client greeting / cluster update.
    Info {
        /// JSON body bytes; the codec does not parse it.
        options: &'a [u8],
    },

    /// `+OK\r\n` — acknowledgement (sent only when the client requested
    /// `verbose` mode in `CONNECT`).
    Ok,

    /// `-ERR <message>\r\n` — protocol-level error from the server.
    Err {
        /// Human-readable error description.
        message: &'a [u8],
    },
}
