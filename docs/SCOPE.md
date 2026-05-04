# Aulon — Scope

## In scope (v1)

### Wire protocol verbs

- `CONNECT`
- `PUB`
- `SUB`
- `UNSUB`
- `MSG`
- `PING` / `PONG`
- `INFO`
- `+OK` / `-ERR`

### Subject features

- Exact-match subjects
- Single-token wildcard `*`
- Multi-token wildcard `>` (must be terminal)
- Queue groups (load-balanced delivery within a group)

### Transport

- TCP only
- Plain text framing per the NATS-core protocol

## Out of scope (v1)

- TLS termination
- Authentication: token, NKey, JWT, user/pass
- JetStream (persistent streams, consumers, KV, object store)
- Clustering, gateways, leafnodes
- WebSocket transport
- MQTT bridging
- Request-reply convenience features beyond what the `MSG` reply-to field already provides

## Compatibility goal

`nats bench` and the official `nats` CLI run unmodified against an Aulon instance for the verbs listed above, including wildcards and queue groups. Anything outside the in-scope list is allowed to return `-ERR` with a descriptive message.

## Non-goals

- Beating `nats-server` on every workload. The headline benchmark is single-core p99.99 publish-to-deliver latency at small payloads. Other axes are not tuned in v1.
- Full operational completeness (no metrics endpoint, no admin API, no graceful drain) until C5.
