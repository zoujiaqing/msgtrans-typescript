# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-beta.0] — 2026-05-01

Initial public beta. Wire-format compatible with the `msgtrans` Rust crate
(v1.0.6+) per [`WIRE_FORMAT.md`](https://github.com/zoujiaqing/msgtrans/blob/main/docs/WIRE_FORMAT.md) v1.

### Added

- `Packet` wire-format encoder / decoder (16-byte fixed header, big-endian)
- `WebSocketTransport` — browser-first, Node-compatible via injected
  `webSocketFactory`; binary frames only, configurable text-frame policy
- `RequestManager` — request/response correlation, per-request timeout,
  shared u32 messageId counter with wraparound semantics
- `TransportClient` — high-level facade with surface aligned to the Rust
  `msgtrans::transport::TransportClient`:
  - `connect()` / `close()` / `disconnect()` (alias) / `isConnected()`
  - `send(payload, options)` / `request(payload, options)` returning bytes
  - `sendPacket(packet)` / `requestPacket(packet)` for raw `Packet` access
  - `on('message' | 'messageSent' | 'close' | 'error', cb)` returning an
    unsubscribe function
- `SendOptions` / `RequestOptions` accepting `bizType`, `extHeader`,
  `reserved`, `compression`, `timeoutMs` (mirrors Rust `TransportOptions`)
- `TransportContext.respond()` for handling server-initiated `Request` packets
- Cross-language fixture-based wire-format conformance against the Rust crate
  (8 fixtures, byte-for-byte verification in both directions)
- E2E suite against the dedicated WS-only `e2e_echo_server` (fast smoke)
- E2E suite against the official multi-protocol `echo_server` in passive mode
  (TCP + WebSocket + QUIC bound simultaneously, real-gateway compatibility)

### Notes

- ESM-only package. Requires Node 18+.
- WebSocket is the only currently supported transport. WebTransport is on the
  roadmap as a separate adapter; bare TCP / QUIC will not be supported in the
  browser.
- `compression` is part of the `SendOptions` / `RequestOptions` surface but
  only `CompressionType.None` is implemented in this release. Passing any other
  value rejects with `UnsupportedCompressionError`. Codec injection (zlib via
  native `DecompressionStream`, zstd as peer dependency) is planned for v0.3.
