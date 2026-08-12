# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## 0.2.0-beta.0 — 2026-08-12

The msgtrans **2.0 semantic alignment**. Wire format is unchanged (protocol
v1, byte-identical fixtures); everything below is contract, not bytes.

### Breaking

- **Ids belong to the transport.** `sendPacket()`, `requestPacket()` and
  `TransportClientOptions.initialMessageId` are removed; `RequestManager` is
  no longer exported. A caller-numbered Request could complete a DIFFERENT
  pending waiter that happened to draw the same id.
- **Ids never wrap.** The counter refuses to wrap at `0xffffffff`
  (`IdExhaustedError`); a reconnect starts a fresh generation with a fresh
  counter — matching the Rust 2.0 per-session registry.
- **`TransportContext` is split** into `ClientMessage` (OneWay — no
  `respond` at all) and `ClientRequest` (arrives via the new
  `on('request')`; `respond()` answers at most once and refuses to write
  onto a later connection generation).
- **`messageSent` → `messageEnqueued`** (`Transport.onMessageSent` →
  `onMessageEnqueued`, `SentInfo` → `EnqueuedInfo`). A browser WebSocket
  offers no write completion; the tier is named for what it proves.
- **Strict decode.** `Packet.decode` → `Packet.decodeExact`: unknown
  packet-type/compression bytes, trailing bytes, and frames over the
  configured `DecodeLimits` are protocol errors — and inbound protocol
  errors CLOSE the connection (Rust strict framing), instead of being
  logged while the stream keeps running desynchronized.
- **Frame caps default to the Rust defaults** (16 MiB payload / 65535 ext
  header, configurable via `TransportClientOptions.limits`), enforced in
  BOTH directions — the old encode ceiling was the raw wire max (4 GiB),
  a frame the server was guaranteed to kill the connection over.
- **Pending requests reset on close only.** A non-fatal transport error no
  longer rejects every in-flight request while the connection stays open.
- `SendOptions.reserved` is removed. `ReservedFlag` stays exported as wire
  metadata; `Fragmented` is refused by the WebSocket transport (nothing
  implements reassembly).

### Added

- **Compression that actually works.** `ZlibCodec` via native
  `CompressionStream/DecompressionStream('deflate')` (RFC 1950), registered
  by default; `CompressionCodec` SPI for injecting Zstd. One registry serves
  both directions. No codec ⇒ the SEND rejects (a raw payload is never
  shipped under a compressed header) and an inbound algorithm without a
  codec is FATAL (the app never receives compressed bytes it would mistake
  for plaintext). Decompression is streaming with a hard output cap —
  cancelled the moment it exceeds the limit, never inflate-then-check.
- **Serialized inbound normalization** with an idle synchronous fast path:
  delivery order is arrival order even when compressed and plain packets
  interleave, and plain traffic on an idle chain is delivered synchronously
  exactly like 0.1. The outbound path mirrors it, so
  `send(a, {compression}); send(b)` cannot put `b` on the wire first.
- **WebSocket generation isolation**: a previous socket's late-arriving
  `onclose`/`onerror`/`onmessage` can no longer close the replacement
  connection, reject its pending requests, or deliver stale traffic into it.
- **Projected backpressure**: `send()` rejects when `bufferedAmount` PLUS
  this frame would cross the high-water mark (checked via `totalLength()`,
  before any encode).
- **Request timeout starts at enqueue**: the timer arms only after the
  transport accepted the packet, so a slow enqueue cannot eat the peer's
  response budget — and a request that failed to enqueue can no longer be
  "timed out" and then sent anyway, minting an orphan response.
- `api-report.txt` public API snapshot + CI (typecheck, tests, build,
  fixtures, API drift).
- E2E zlib interop against the Rust 2.0 server: compressed request,
  compressed response (`respond_with_options`), compressed push.

### Falsification

Every headline fix was verified against its reintroduced defect: skipping
inbound decompression fails 5 tests; removing socket generation isolation
fails 3; arming the timer before enqueue, dropping the backpressure
projection, unchecking respond generations, and letting plain sends bypass
a busy outbound chain each fail their test.


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
