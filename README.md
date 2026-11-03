# VolsGames

Open-core real-time gaming network engine in Rust: QUIC/WebTransport/WebSocket gateway,
rooms, relay, reconnect, and a REST control API. Apache-2.0.

```
cargo run --release -- serve --ws 127.0.0.1:8081
cargo run --release -- fleet --node http://127.0.0.1:8080 --node-key $(cat .volsgames/api.key)
cargo run --release --bin volsgames-loadgen -- --api-key $(cat .volsgames/api.key) --bots 50 --insecure
```

Crates: `proto` (wire format, tokens), `room` (room actors), `gateway` (transports),
`control` (REST API), `server` (the `volsgames` binary), `loadgen` (bots).

Documentation and API reference: https://volsgames.com/docs
