# Connection lifecycle

## Decision

Encode connection state in the type system via the typestate pattern. A `Connection<S>` is parameterised over a state marker type; methods are defined on impls of specific states. State transitions consume `self` and return a new typed value. Calling `write` on a connection that has not yet reached the right state is a compile error, not a runtime check.

## States

```text
Unconnected --> Negotiating --> Active --> Closing --> Closed
                    ^                |
                    |                |
                    +-- (C2+ only) --+
```

- **Unconnected** — never used as a runtime type; included for completeness. The accept loop produces `Connection<Active>` directly for C1 since the echo protocol has no handshake.
- **Negotiating** — added in C2 when the NATS `CONNECT` exchange is implemented. Holds a partial state; cannot send `MSG`.
- **Active** — connection is fully usable. Reads, writes, and shutdown are all callable.
- **Closing** — graceful shutdown is in flight (FIN sent, draining inbound). No new writes accepted.
- **Closed** — terminal. Resources released. The value is dropped; there is no method on `Connection<Closed>`.

Only **Active** and **Closing** are real types in C1. The others land as their corresponding checkpoints introduce them.

## Type sketch

```text
pub struct Connection<S: State> {
    fd: RawFd,
    rx_buf: BufferId,
    state: PhantomData<S>,
}

impl Connection<Active> {
    pub async fn read(&mut self, pool: &mut BufferPool) -> io::Result<usize> { ... }
    pub async fn write(&mut self, pool: &mut BufferPool, len: usize) -> io::Result<()> { ... }
    pub async fn shutdown(self) -> io::Result<Connection<Closing>> { ... }
}

impl Connection<Closing> {
    pub async fn drain(self) -> io::Result<()> { ... } // drops to Closed
}
```

`State` is a sealed marker trait so external code cannot invent its own states.

## What this prevents at compile time

- Calling `write` on a connection that has not finished negotiation (once C2 lands).
- Calling `write` after `shutdown` (no `write` method on `Connection<Closing>`).
- Double-`shutdown` (consumes `self`).
- Re-using a `Closed` connection (no methods exist; the value has been dropped).

These are the four bug classes that appear most often in hand-written async network code. Encoding them in the type system means they never appear in code review.

## Trade-offs accepted

- **Generic types in storage.** A worker that owns N connections cannot put `Vec<Connection<Active>>` and `Vec<Connection<Closing>>` in the same container without an enum. The accept loop will use a small typed slab per state, not one polymorphic collection. This keeps dispatch monomorphic and avoids `Box<dyn>`.
- **State transitions consume `self`.** This precludes "in place" mutation of state, but it matches how io_uring completions actually work: the future driving the transition resolves, and the next state of the connection is what we operate on next. The cost is rhetorical, not runtime.
- **Closing requires explicit state.** A user who wants "fire and forget" close has to pick: drop the `Connection<Active>` (sends RST via `Drop`) or call `shutdown` (graceful). Either is fine; the type forces the decision.

## Out of scope for C1

- TLS state. Out of scope per `docs/SCOPE.md`.
- Half-close handling for asymmetric direction shutdown. Not needed for the broker model; either side closing is the same as both.
- Reconnection. Aulon is a server; clients reconnect with their own logic.

## Measurement plan

There is nothing to measure for the typestate itself; correctness is enforced at compile time. The relevant measurements live in `runtime.md` (syscalls per round-trip) and `buffer-pool.md` (acquire/release latency). The connection layer's contribution to RTT shows up as the difference between raw `read_fixed`/`write_fixed` latency and end-to-end echo latency.
