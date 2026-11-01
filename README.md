# VolsGames

Open-core real-time gaming network engine in Rust. Customers plug game clients in and get
connections, rooms, broadcasting, matchmaking and sandboxed server-side logic.

Status: design phase, no code yet.

## Architecture in one paragraph

Clients connect over QUIC (WebTransport in browsers, raw QUIC natively, WebSocket as a
fallback) to an edge gateway that validates a signed session token and forwards frames to
the room host that owns the room. A room is a single-threaded actor pinned to a core with a
fixed tick, reliable and unreliable channels on top of datagrams, interest management, and
delta-compressed snapshots. Customer logic runs inside the room as a WebAssembly module
(wasmtime) with fuel metering and a host-function-only sandbox. Everything that is not
latency sensitive goes through REST and webhooks.

Docs:
- [docs/DESIGN.md](docs/DESIGN.md) — components, crate layout, security, benchmarks
- [docs/ARCHITECTURE-DECISIONS.md](docs/ARCHITECTURE-DECISIONS.md) — technical decisions
- PROTOCOL.md, HOST-ABI.md and BENCHMARKS.md arrive with their milestones

Repos: `volsgames` (this, core), `volsgames-sdk-js`, `volsgames-sdk-unity`,
`volsgames-examples`. The hosted control plane and billing are not part of the open core.

## Licence

Apache-2.0, see [LICENSE](LICENSE). Everything here is meant to be self-hostable.
