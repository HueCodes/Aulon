# `--cfg loom` versus `tokio-uring`

## What I was trying to do

C4 introduced a cross-shard wake primitive: a bounded lock-free MPMC ring
(`crates/aulon-core/src/shard_inbox.rs`) restricted to single-consumer
use, plus an `eventfd` poke. The locked decision in `docs/PROMPT.md`
forbids `Arc<Mutex<T>>` in `aulon-core`, so a custom Vyukov-style ring
was the only honest option. Custom unsafe concurrency demands a
machine-checked argument that the memory ordering is correct, so
`shard_inbox` had to be `loom`-tested.

The plan was the standard one. Author the ring with `loom::sync::atomic`
under `cfg(loom)` and `core::sync::atomic` otherwise; write two
interleaving models (producer-producer slot-claim, producer-consumer
visibility); run them under `RUSTFLAGS="--cfg loom" cargo test -p
aulon-core --test loom_inbox --release`.

That last command fails to build.

## What actually happens

Setting `RUSTFLAGS="--cfg loom"` does not just activate Aulon's own
`cfg(loom)` branches. Cargo passes `RUSTFLAGS` to every crate in the
dependency graph, so it activates `cfg(loom)` branches **inside `tokio`
itself**. `tokio` ships with its own internal loom integration for its
own concurrency tests. One of the things its `cfg(loom)` branch disables
is the `current_thread` runtime's `on_thread_park` builder hook.

`tokio-uring` requires `on_thread_park`. That is the hook it uses to
check the io_uring completion queue every time the executor parks. With
the hook gone, `tokio_uring::start` fails to compile. The error surface
is several layers deep into private types in `tokio_uring::runtime`, so
the connection back to "you set `RUSTFLAGS=--cfg loom` an hour ago" is
not obvious.

Adding `cfg(not(loom))` gates around the modules in `aulon-core` that
use `tokio-uring` is necessary but not sufficient: the `tokio-uring`
crate itself is still in the dependency graph and its build script
still runs against the cfg-poisoned `tokio`.

## The fix

Two-part. First, drop `tokio-uring` and `tokio` from the dependency
graph entirely under `cfg(loom)`. Second, gate the modules that use
them out of `aulon-core`'s public surface under loom too.

```toml
# crates/aulon-core/Cargo.toml
[target.'cfg(not(loom))'.dependencies]
tokio-uring = { workspace = true }
tokio = { workspace = true }
libc = "0.2"

[target.'cfg(loom)'.dependencies]
loom = "0.7"
```

```rust
// crates/aulon-core/src/lib.rs
#[cfg(not(loom))]
pub mod buffer_pool;
#[cfg(not(loom))]
pub mod connection;
#[cfg(not(loom))]
pub mod connection_state;
#[cfg(not(loom))]
pub mod eventfd;
pub mod shard_inbox;
pub mod subscription;
pub mod topology;
```

A second non-obvious detail: `loom` itself has to be a regular
dependency (under `cfg(loom)`), not a `dev-dependency`. The atomic
types in `shard_inbox` are referenced from non-test code via
`#[cfg(loom)] use loom::sync::atomic::AtomicUsize;`, so the symbol
needs to be reachable from the library crate and not just the test
binary. `dev-dependencies` are not visible to the library, so the
import fails to resolve.

With those changes, `RUSTFLAGS="--cfg loom" cargo test -p aulon-core
--test loom_inbox --release` exhaustively explores both interleaving
models in well under a second.

## What I'd tell someone hitting this for the first time

1. `RUSTFLAGS="--cfg X"` is global. Every crate sees it. If `X` is a
   name a dependency cares about, you have inherited that dependency's
   `cfg(X)` behaviour whether you wanted to or not.
2. Loom-test the smallest possible crate. The narrower the crate's
   dependency graph under `cfg(loom)`, the smaller the cfg-leak blast
   radius. If `aulon-core` had been split (e.g. a separate
   `aulon-sync` crate that holds nothing but the inbox), the loom
   build would not have touched `tokio-uring` at all. I would split
   it now if `aulon-core` grew further; for the current size the
   `cfg(not(loom))` gates are cheaper than a crate split.
3. `loom` must be a regular dep when its types are referenced from
   non-test source files, even if the only call sites are
   `#[cfg(loom)]`-gated.
4. The Cargo.toml comment explaining all of this is the second-most
   valuable artifact from this exercise; the loom test is the first.
   Future-me does not need to re-derive any of it.

## Where this lives in the tree

- The fix: `crates/aulon-core/Cargo.toml` (target-cfg dependency
  split), `crates/aulon-core/src/lib.rs` (module gates).
- The test: `crates/aulon-core/tests/loom_inbox.rs`.
- The ring it tests: `crates/aulon-core/src/shard_inbox.rs`.
- The C4 review entry that called this out the first time:
  `docs/reviews/checkpoint-4.md` ("What did we get wrong?").
