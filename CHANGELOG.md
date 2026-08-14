# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## 0.6.2-beta.0 — 2026-08-14

**Final supervisor closure + API freeze prep** (tenth review; the
eleventh-review adapter-close fixes below shipped under the same version by
reviewer decision — nothing was published in between).

### Fixed (twelfth review)

- A rollback close that itself fails is now NON-retryable
  (`InstallationCleanupError`, internal): the failed attempt's physical
  link may still be open, and a successful redial would leave two live
  connections — `connect()` terminates to Idle instead of retrying.
- The missing-WebSocket test stubs `globalThis.WebSocket` (Node ≥ 22 ships
  one): the publish gate no longer really dials `test.invalid` and depend
  on DNS timing.

### Fixed (eleventh review)

- The built-in WebSocket/WebTransport sessions no longer swallow a platform
  `close()` that throws: local terminal state converges and the close event
  fires FIRST, then the platform failure is propagated — so
  `TransportClient.shutdown()`/`disconnect()` reject end-to-end instead of
  reporting success over a still-open socket.
- `WebSocketSession.close()` fixes the local close intent BEFORE calling
  the platform close: a WebSocket whose `onclose` fires synchronously can
  no longer relabel an intentional close as `intentional: false` (the field
  reconnect managers key on). Caller `code`/`reason` now map onto
  `ws.close(code, reason)` and WebTransport's `{ closeCode, reason }`.
- The failed-installation rollback OWNS its close: `connect()` settles (and
  retry dials the next attempt) only after the failed attempt's physical
  link is released; if that close also fails, both errors stay visible in
  the rejection.

### Fixed

- A `connect()` issued right after `disconnect()` cancelled an in-flight
  dial no longer receives the doomed old flight: a cancelled single-flight
  is never shared — the new intent queues behind it and dials fresh.
- Session installation is an exception-safe transaction: a throwing
  `subscribe()` rolls back current, retires the session AND closes the
  physical link; a close replayed synchronously during `subscribe()` no
  longer leaks the observer registration (`setUnsubscribe` runs the handle
  immediately when the session already retired).
- `disconnect()`/`shutdown()` PROPAGATE an underlying
  `TransportSession.close()` failure after converging state in `finally` —
  a broken close is no longer reported as success over a possibly-open
  session (the error also still surfaces on `'error'`).

### Breaking (pre-freeze cleanup; nothing published on npm yet)

- Removed the deprecated `transport` option — the public name is
  `connector`.
- Typed close everywhere: new `TransportCloseEvent`
  (`{ intentional, reason?, code?, cause? }`).
  `TransportSession.close(event?: Partial<TransportCloseEvent>)`,
  `TransportObserver.onClose(event: TransportCloseEvent)`, and the client's
  `'close'` event all use it; sessions normalize platform close shapes.

### Infra

- `prepublishOnly` now runs `test:e2e:all` (13 Rust-interop tests), not the
  8-test subset.
- Downstream `@privchat/sdk` peer range moved to `^0.6.2-beta.0` and the
  pairing was verified via packed tarballs (`npm pack` + real install), not
  only `file:` links.

## 0.6.1-beta.0 — 2026-08-14

**Serialized lifecycle supervisor** (ninth review). `connect()`,
`disconnect()` and `shutdown()` are serialized — never interleaved — and
every post-await state write is owned:

- A dial queues BEHIND an in-progress close (`closeFlight`), so a finishing
  `disconnect()` can no longer write `Idle` over a session a concurrent
  `connect()` just installed.
- `shutdown()` takes over an in-flight close and resolves only once the
  physical session is actually down (previously it returned early because
  the session reference was already null).
- Session install is atomic: the session becomes current BEFORE
  `subscribe()`, and a close replayed synchronously during subscribe fails
  the connect (`connection closed during establishment`) instead of
  installing a dead session and reporting Open. A session that never became
  Open emits no close event — the `connect()` rejection is the report.
- Close-state convergence and the exactly-once close notification moved
  into `finally`: a `TransportSession.close()` that rejects cannot strand
  the client in `Closing` (the error surfaces via `on('error')`).
- The connect-retry backoff sleep is abort-interruptible:
  `disconnect()`/`shutdown()` no longer wait a backoff out.
- WebTransport dial races the AbortSignal against `ready` AND
  `createBidirectionalStream` — abort wins even when the platform never
  settles either.

### Changed

- The connector option is now officially `connector?: TransportConnector`;
  `transport?` remains as a deprecated alias. README migrated off the
  pre-0.6 `Transport`/`client.close()`/`WebTransportTransport` names.

## 0.6.0-beta.0 — 2026-08-14

**The Connector/Session SPI** (eighth review). The shared reconnectable
`Transport` object is gone from the public boundary: a `TransportConnector`
holds configuration only, and every `connect()` returns a brand-new
`TransportSession` that OWNS its physical connection, writer and event
source. The reviewer's four counterexamples — an old connection's late
same-id Response completing the new connection's request, an async send
writing into the replacement socket, a dial installing a session after
disconnect(), a late onopen reviving a closed state machine — are now
unrepresentable rather than guarded.

### Breaking

- SPI: `Transport` → `TransportConnector` + `TransportSession` +
  `TransportObserver`. `connect({signal})` is abortable and MUST release a
  connection it wins after cancellation. Sessions enqueue/subscribe/close;
  close is reported exactly once.
- `WebSocketTransport` → `WebSocketConnector` (`WebSocketConnectorOptions`);
  experimental `WebTransportTransport` → `WebTransportConnector`.
- `TransportClient.state` is a real machine: Idle / Connecting / Open /
  Closing / Stopped. `disconnect()` cancels an in-flight dial, awaits it,
  retires the session, returns to Idle and emits ONE close event
  (`intentional: true`); `shutdown()` is terminal and refuses later
  `connect()`.
- Requests/sends/responds bind to the session current at CALL time and
  enqueue on that session's link — which rejects once closed.
- `observeActivity()` is the liveness tap; `HeartbeatMonitor` uses it and
  no longer collides with the single request consumer.
- `CompressionCodec` takes an optional `AbortSignal`; retiring a session
  aborts its in-flight codec work (a hung compression releases its memory).
- SDK note: `PrivchatClient.dispose()` now calls `transport.shutdown()`
  (terminal), and `connect()` after dispose throws.

## 0.5.0-beta.0 — 2026-08-14

Session-ownership rework (seventh review). The prior claim that per-promise
generation checks matched the Connector/Session semantics was WRONG — six
reviewer-reproduced counterexamples proved it — so ownership is now
structural: an internal `Session` object owns the id allocator, request
manager, inbound normalizer and outbound queue, and retiring it voids every
piece of in-flight async work at once, with no dependence on adapters
firing events.

### Fixed

- **P0** Async sends cannot cross a reconnect: every send/request/respond is
  bound to the session it was CALLED on; if the connection turns over during
  async compression, the operation is refused (no silent replay of
  non-idempotent requests).
- **P0** `connect()` is single-flight: concurrent calls share one attempt,
  so two completion paths can never each install a session (the id-ABA).
- **P0** Ownership no longer depends on the adapter emitting `close`:
  `disconnect()` retires the session directly, and a (re)connect installs a
  fresh one — a custom Transport that never fires `onClose` cannot leak old
  async work into the replacement connection.
- **P0** WebSocket state cannot be revived: `close()` detaches the socket
  (a late `onopen` fails `isCurrent()`), and a handshake `onerror` without
  `onclose` lands in Closed instead of wedging Connecting and replaying the
  same rejected promise to every later `connect()`.
- **P1** Poisoning covers the packet already inside its decompression when
  the verdict lands (post-await recheck).
- **P1** Helper epochs are complete: a stale probe's SUCCESS is as void as
  its failure, a hung probe cannot pin the restarted monitor's `probing`
  flag, and a reconnect cycle abandoned by `stop();start()` cannot
  zombie-connect under the new lifecycle's flag.
- **P1** Validation covers `connectRetry` (integer maxRetries — `-1` used to
  make `connect()` throw `undefined`), `multiplier` (finite), `maxAttempts`
  (integer or Infinity), `defaultTimeoutMs`, per-request `timeoutMs`, and
  `maxBufferedBytes`.
- **P2** Queue caps count full frame bytes on inbound; outbound is capped in
  ops AND bytes; overflow raises the new `QueueOverflowError` (queue
  occupancy) instead of misusing `BackpressureError` (socket bytes).

### Changed (breaking)

- `ClientMessage` / `ClientRequest` are exported as INTERFACES with a `kind`
  discriminant; the implementations are not exported (compile-level seal,
  not just a runtime throw). Discriminate with `x.kind === 'request'`.
- Requests have exactly ONE consumer: a second `on('request')` registration
  throws (a request carries a single response obligation).
- `close()` is replaced by `disconnect()` (reconnectable) and `shutdown()`
  (terminal; later `connect()` is refused).
- `RequestManager` no longer self-subscribes to transport close (one
  listener leaked per reconnect); the owning session resets it.
- `prepublishOnly` now also enforces the API snapshot and runs the Rust
  interop E2E.

## 0.4.0-beta.0 — 2026-08-14

Lifecycle hardening: six reviewer-reproduced generation/lifecycle faults,
each captured as a failing test first, then fixed and re-falsified.

### Fixed

- **P0** `connect()` is idempotent: a redundant call on a live connection no
  longer restarts the id space (which stranded a pending request's promise
  by handing its id to the next request).
- **P0** Inbound normalization is generation-bound: a reconnect installs a
  fresh normalizer, so a previous connection's in-flight decompression has
  nowhere to deliver. A fatal normalization failure POISONS the stream —
  packets queued behind the violation are dropped, not delivered.
- **P0** Cancel-safe lifecycle: WS `close()` during Connecting settles the
  pending `connect()` (it used to hang forever on a hung handshake); a WT
  factory that throws synchronously no longer wedges the state machine in
  Connecting; WT `close()` during `connect()` fails that connect instead of
  letting it report success on a dead connection.
- **P1** Stale async results are void: `HeartbeatMonitor.stop()` bumps an
  epoch so an in-flight probe's late verdict cannot close the next
  connection; a close landing while a reconnect cycle unwinds is re-examined
  when the cycle ends instead of being swallowed.
- **P1** Bounded queues: inbound normalization (frames + bytes, overflow
  closes) and the outbound compression chain (overflow rejects with
  `BackpressureError`).
- **P1** Strict config validation: `NaN`/negative/non-finite values in
  `DecodeLimits`, retry, heartbeat and reconnect options THROW instead of
  silently disarming comparisons (a `NaN` payload cap disabled the
  decompression-bomb defense entirely).

### Changed (breaking)

- `ClientMessage` / `ClientRequest` are sealed: constructed by the transport
  only (a consumer-constructible ClientRequest could respond with an
  arbitrary messageId). `TransportClient.currentGeneration` and `respondTo`
  are no longer public.
- **WebTransport moved to `@msgtrans/client/experimental`** until a real
  Rust WebTransport server exists for interop E2E.
- `ReconnectOptions.jitterRatio` (default 0.2) spreads fleet reconnects;
  `ReconnectManager` documents itself as THE retry policy (leave
  `connectRetry` at its zero default underneath it).
- `api-report.txt` now includes only modules reachable from the package
  exports — the old all-of-dist concatenation froze internals.

## 0.3.0-beta.0 — 2026-08-13

### Added

- **`WebTransportTransport`** — msgtrans over WebTransport (HTTP/3).
  Framing mirrors the Rust QUIC adapter (one bidirectional stream, 4-byte
  big-endian length prefix per frame) so the two interoperate the day a
  server exposes a WebTransport endpoint. Same contract as the WebSocket
  adapter: strict inbound (over-limit declared length is refused BEFORE
  buffering; undecodable frames close the connection), `Fragmented` refused,
  session generation isolation, enqueue-confirmed sends whose await is the
  stream's own backpressure. Injectable `webTransportFactory` for tests and
  non-browser hosts.
- **`HeartbeatMonitor`** — application-layer heartbeat. msgtrans has no
  protocol-level ping, so the caller supplies the probe (any
  request/response the peer must answer); the monitor owns idle detection
  (traffic in either direction suppresses probing), the probe deadline, and
  the verdict (default: close, so reconnect logic takes over). Detection and
  recovery are deliberately separate jobs.
- **`ReconnectManager`** — exponential-backoff reconnection with a blunt
  contract: every `close` reconnects while active, so an intentional
  disconnect must `stop()` first; one cycle at a time; `stop()` during a
  backoff sleep abandons the cycle; pending requests are NOT replayed
  (replay policy belongs to the application, which knows what is
  idempotent).

### Falsification

WT generation isolation, heartbeat idle suppression, and the mid-sleep
`stop()` check were each verified to fail their test with the guard removed.

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
