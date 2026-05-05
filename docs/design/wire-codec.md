# Wire codec

## Decision

`aulon-proto` parses and emits the NATS-core protocol verbs listed in
`docs/SCOPE.md` over **borrowed byte slices**, with zero allocations on
the hot path. Frames are exposed as `Frame<'a>` enums whose fields are
`&'a [u8]` slices into the caller's input buffer. The parser is a
streaming state machine that returns `ParseOutcome::NeedMore` when the
input does not yet contain a complete frame; the caller is responsible
for appending more bytes and re-trying.

The crate is `#![no_std]`-clean (uses `core` and `alloc` only — and the
public surface uses no `alloc`). `aulon-proto` has no I/O dependencies,
no async, no runtime tie-in. It can be fuzzed and proptested in
isolation.

## Frame shape

```text
pub enum Frame<'a> {
    Connect { options: &'a [u8] },           // raw JSON bytes
    Pub { subject: &'a [u8], reply_to: Option<&'a [u8]>, payload: &'a [u8] },
    Sub { subject: &'a [u8], queue_group: Option<&'a [u8]>, sid: &'a [u8] },
    Unsub { sid: &'a [u8], max_msgs: Option<u64> },
    Msg { subject: &'a [u8], sid: &'a [u8], reply_to: Option<&'a [u8]>, payload: &'a [u8] },
    Ping,
    Pong,
    Info { options: &'a [u8] },
    Ok,
    Err { message: &'a [u8] },
}
```

- `Connect` and `Info` carry their JSON body as raw bytes; v1 does **not**
  parse the JSON. The server reads only the fields it needs (e.g.
  `verbose`) via a small targeted scan, deferred until those fields
  matter (likely C3). This avoids pulling a JSON dependency into the
  zero-copy crate.
- `Pub` / `Msg` payload bytes are sliced directly out of the input. No
  copying. No allocation. Validation is structural only — we do not
  inspect payload bytes.
- Subjects, sids, reply-tos, and queue groups are byte slices, not
  strings. Subject validity (legal characters, dot-separated tokens,
  wildcard placement) is enforced **at routing layer** (`aulon-core`),
  not in the codec. The codec just returns the bytes as written.

## Parse outcome

```text
pub enum ParseOutcome<'a> {
    Frame { frame: Frame<'a>, consumed: usize },
    NeedMore,
    Err(ParseError),
}
```

The caller drives the parse by repeatedly calling `parse_frame(buf)`. If
`Frame` is returned, advance the buffer by `consumed` bytes and call
again for the next frame. If `NeedMore`, read more bytes from the wire
and re-call. If `Err`, the connection is in an unrecoverable state and
should be closed with `-ERR`.

`consumed` is precise — it is the count of bytes that made up the parsed
frame's *header line and CRLF, plus any payload and trailing CRLF*. This
keeps the caller's buffer management simple.

## Parsing strategy

Two recognisable shapes:

1. **Single-line verbs** end at the first CRLF (`\r\n`).
   `CONNECT`, `SUB`, `UNSUB`, `PING`, `PONG`, `INFO`, `+OK`, `-ERR`.
2. **Header + body verbs** have a CRLF-terminated header that ends with
   a byte count, followed by exactly that many payload bytes, followed
   by a trailing CRLF.
   `PUB`, `MSG`.

The parser:

1. Find the first `\r\n` in the input. If absent, return `NeedMore`.
2. Parse the verb keyword (case-insensitive in the NATS spec; we accept
   uppercase canonical only and document it as a v1 simplification — the
   `nats` CLI sends uppercase). Match against the known verb set.
3. For single-line verbs: parse the verb's arguments from the remainder
   of the line. Return `Frame` with `consumed = end-of-CRLF`.
4. For header+body verbs: parse header arguments (last is the byte
   count). Check that the buffer holds at least
   `header_len + payload_len + 2` bytes (the trailing CRLF). If not,
   return `NeedMore`. Otherwise slice the payload, validate the trailing
   CRLF, return `Frame` with `consumed = total`.

No backtracking. No state across calls — each call is independent over
the current buffer. The caller maintains the buffer itself.

## Emit strategy

```text
pub fn emit_msg(
    out: &mut [u8],
    subject: &[u8],
    sid: &[u8],
    reply_to: Option<&[u8]>,
    payload: &[u8],
) -> Result<usize, EmitError>;
```

Each verb has a corresponding `emit_*` function that writes into a caller-
provided `&mut [u8]` and returns the number of bytes written, or
`EmitError::BufferTooSmall { needed, have }` if the buffer is short.

No allocation. No `Vec`. The server side allocates one fixed buffer per
outbound write (drawn from the same pool used for reads — see
`docs/design/buffer-pool.md`).

`itoa` (or a small inline `write!` to a `core::fmt::Write` adapter) is
used for integer formatting; no full-fat formatting machinery on the
hot path.

## What the codec does NOT do

- **It does not validate subjects.** Routing layer's job. Codec accepts
  any non-whitespace byte sequence as a subject token.
- **It does not interpret `Connect` / `Info` JSON.** Body is opaque
  bytes.
- **It does not enforce protocol state.** Parsing `MSG` from a client
  is structurally fine even though it makes no sense; the routing layer
  rejects it.
- **It does not buffer.** That is the caller's concern. The codec works
  exclusively over the slice it is handed.
- **It does not implement the `HMSG` / `HPUB` headers verbs.** Headers
  are out of scope per `docs/SCOPE.md`.

## Error surface

```text
pub enum ParseError {
    UnknownVerb,
    BadHeader,
    PayloadLengthOverflow,    // header byte count is absurd
    InvalidUtf8InCount,       // byte count is not ASCII digits
    MissingTrailingCrlf,      // payload terminator missing
}

pub enum EmitError {
    BufferTooSmall { needed: usize, have: usize },
}
```

The set is intentionally small. Anything not classified above is a
parse-error class we will discover via fuzz; new variants are added as
fuzz finds them.

## Test surface

Unit tests in `aulon-proto/src/parse.rs` cover one "happy path" per
verb plus the obvious failure cases (truncation, bad byte count,
unknown verb).

Property tests via `proptest`: round-trip — generate a `Frame`, emit
into a buffer, parse the buffer, assert equality.

Fuzz target via `cargo-fuzz` under `fuzz/`: feed arbitrary bytes into
`parse_frame`. The acceptance criterion for the C2 gate is >1M
iterations clean.

## Performance plan

A `criterion` micro-bench under `crates/aulon-proto/benches/parse.rs`
reports per-frame parse latency for each verb at representative subject
and payload sizes (e.g. `PUB foo.bar 256B`). Numbers land in
`PERFORMANCE.md` once criterion is added (early in C2).
