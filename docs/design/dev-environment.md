# Development environment

## Decision

Aulon is built and tested on Linux. Daily development happens inside an OrbStack-managed Ubuntu VM on the maintainer's macOS host; the macOS host is the editor and source of truth for the working tree. CI uses GitHub Actions Linux runners. Headline benchmarks (C4) move to a dedicated bare-metal or cloud Linux machine where macOS-hosted virtualisation jitter is eliminated.

## Why

`tokio-uring` (and `io_uring` itself) is Linux-only. It depends on syscalls that have no equivalent on macOS or Windows. There is no cross-platform `cfg`-gated fallback that would preserve the runtime's identity. So the project is Linux-first by construction.

## Local layout

- Working tree on macOS at `/Users/hugh/Dev/projects/Aulon`.
- OrbStack auto-mounts the macOS user home into the VM at the same path. No `rsync` or `Mutagen` needed.
- VM-side build artifacts live in `/tmp/aulon-target` (set via `CARGO_TARGET_DIR`) so the macOS host's `target/` is not polluted by Linux objects and vice versa.
- macOS host can also run `cargo build` for any crate that does not depend on `tokio-uring`. `aulon-proto` is no-std-clean and pure Rust, so it builds and tests on macOS. `aulon-core`, `aulon-server`, and `aulon-bench` build only inside the VM.

## Standard commands

From the macOS host:

```bash
# build / lint / test inside the VM, using a separate target dir
orb -m my-ubuntu-vm bash -lc 'CARGO_TARGET_DIR=/tmp/aulon-target cargo build'
orb -m my-ubuntu-vm bash -lc 'CARGO_TARGET_DIR=/tmp/aulon-target cargo clippy --all-targets --all-features -- -D warnings'
orb -m my-ubuntu-vm bash -lc 'CARGO_TARGET_DIR=/tmp/aulon-target cargo test'
```

`docs/` and design files are edited and committed from the macOS host as normal.

## Why not other paths

- **`cfg`-gated cross-platform shim.** Would mean writing a parallel macOS implementation of the broker's data path against `kqueue`. Doubles the surface, halves the rigour, and contradicts the project's identity as an io_uring showcase. Rejected.
- **Lima or UTM.** Both are real Linux VMs and would work. OrbStack is already installed on this machine, has dynamic memory allocation that suits an 8 GB host, and integrates cleanly with the macOS filesystem. No reason to switch.
- **Pure Docker on macOS.** Docker Desktop and OrbStack-Docker both run a Linux VM under the hood. We need shell access into the VM, not a container per build. The VM model is a better fit.
- **Cloud dev box for everything.** Higher latency, ongoing cost, and irrelevant for daily development. Reserved for the C4 headline benchmark, where measurement quality justifies the infrastructure.

## Verified state

- VM kernel: 6.19 (well past the 5.1 floor for `io_uring`).
- `kernel.io_uring_disabled = 0`. No seccomp restrictions on the syscall set.
- VM Rust toolchain: stable 1.91.1.
- `cargo build`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`, `cargo doc --no-deps --all-features` all green inside the VM on the C0 scaffold.
- macOS host build of the same scaffold remains green (no Monoio dep yet).

## Notes

- The VM's `/etc/resolv.conf` came up pointing at Tailscale's MagicDNS resolver, which is not running inside the VM. This breaks `rustup update` and any other DNS-dependent tooling. If a toolchain bump is needed, fix DNS first (`echo "nameserver 1.1.1.1" | sudo tee /etc/resolv.conf` for a one-shot, or configure systemd-resolved properly for a permanent fix). Not blocking for current work.
- MSRV in `Cargo.toml` is set to `1.85` — the floor for `[workspace.lints]` and the other features used. We bump it freely when we need a newer feature; we do not promise a stable MSRV to consumers.
