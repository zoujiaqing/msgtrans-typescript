# @msgtrans/client

TypeScript client runtime for the [msgtrans](https://github.com/zoujiaqing/msgtrans) wire protocol.

`@msgtrans/client` is the language mapping of the `msgtrans` Rust client surface — same Packet semantics, same Request/Response correlation rules, same wire bytes. The wire format is defined by [`WIRE_FORMAT.md`](https://github.com/zoujiaqing/msgtrans/blob/main/docs/WIRE_FORMAT.md) and conformance is verified by a shared cross-language fixture suite.

> **Status: 0.2.0-beta.0** — the msgtrans **2.0 semantic alignment**: strict decode, transport-owned message ids, connection generations, serialized inbound normalization, and working zlib compression (native streams), all fixture- and E2E-verified against the Rust 2.0 server.
>
> **Compatibility matrix**
>
> | @msgtrans/client | msgtrans (Rust) | Wire protocol |
> |---|---|---|
> | 0.2.x | 2.0.x | v1 |
> | 0.1.x | 1.0.x / 2.0.x | v1 |
>
> The version numbers deliberately do NOT lock step with the Rust crate: this is a browser, single-connection client, not a full mapping of the Rust server architecture. What IS aligned: the wire format, the request lifecycle, strict decoding, and the error semantics.

## Install

```bash
npm install @msgtrans/client
# or
pnpm add @msgtrans/client
```

ESM only. Requires Node 18+ for tooling; browsers need native `WebSocket` (any modern browser).

## Quick start (browser)

```ts
import { TransportClient } from '@msgtrans/client';

const client = new TransportClient({
  url: 'wss://gateway.example.com/ws',
  defaultTimeoutMs: 10_000,
});

await client.connect();
console.log(client.isConnected()); // true

// Fire-and-forget OneWay
await client.send(new TextEncoder().encode('hello'), { bizType: 1 });

// Request / Response
const response = await client.request(
  new TextEncoder().encode(JSON.stringify({ route: 'user.find', id: '42' })),
  { bizType: 100, timeoutMs: 10_000 },
);
console.log(new TextDecoder().decode(response));

await client.disconnect();
```

## Receiving server-pushed messages

```ts
client.on('message', (msg) => {
  // Server-pushed OneWay event — always plaintext (inbound compression is
  // undone before delivery).
  handleEvent(msg.bizType, msg.data);
});

client.on('request', async (req) => {
  // Server is asking us something; respond with the same messageId.
  // Answers at most once, and only onto the connection generation it
  // arrived on.
  const answer = await handle(req.bizType, req.data);
  await req.respond(answer);
});

client.on('messageEnqueued', ({ messageId }) => {
  // Fired when a packet was ACCEPTED by the socket's outbound buffer.
  // Deliberately NOT called "sent": a browser WebSocket cannot confirm a
  // write reached the network, and this API does not pretend it can.
});

client.on('close', (event) => {
  console.log('disconnected', event);
});

client.on('error', (e) => {
  console.error('transport error', e);
});

// All four return an unsubscribe function:
const off = client.on('message', handler);
off(); // detach later
```

## Node usage

Browsers expose `WebSocket` globally; in Node you inject the constructor through `webSocketFactory`:

```ts
import { TransportClient, type WebSocketLike } from '@msgtrans/client';
import WS from 'ws';

const client = new TransportClient({
  url: 'wss://gateway.example.com/ws',
  webSocketFactory: (url, protocols) =>
    new WS(url, protocols) as unknown as WebSocketLike,
});
```

The same hook lets you swap in a custom WebSocket (TLS-pinned, telemetry-wrapped, mocked for tests, etc.) without touching the rest of the client.

## API surface

```ts
new TransportClient({
  url: string;                       // required when using the default WebSocket transport
  transport?: 'websocket' | Transport;
  protocols?: string | string[];
  webSocketFactory?: WebSocketFactory;
  onTextFrame?: 'error' | 'ignore';  // default: 'error'
  defaultTimeoutMs?: number;         // default: 10_000
});

// Lifecycle
client.connect(): Promise<void>;
client.close(): Promise<void>;
client.disconnect(): Promise<void>;  // alias for close()
client.isConnected(): boolean;

// Sending
client.send(payload: Uint8Array, options?: SendOptions): Promise<void>;
client.request(payload: Uint8Array, options?: RequestOptions): Promise<Uint8Array>;

// There is deliberately NO lower-level packet send: message ids belong to
// the transport (a caller-numbered Request could complete a different
// pending waiter that drew the same id — the ABA the Rust 2.0 registry
// closed). `Packet` stays exported for codecs/fixtures/custom Transports.

// Events (each returns an unsubscribe function)
client.on('message',         (msg: ClientMessage) => void): () => void;
client.on('request',         (req: ClientRequest) => void): () => void;
client.on('messageEnqueued', (info: { messageId: number }) => void): () => void;
client.on('close',           (event?: unknown) => void): () => void;
client.on('error',           (error: unknown) => void): () => void;
```

```ts
interface SendOptions {
  bizType?: number;
  extHeader?: Uint8Array;
  compression?: CompressionType;     // Zlib works out of the box (native
                                     // streams); Zstd via an injected codec.
                                     // No codec => the send REJECTS — a raw
                                     // payload is never shipped under a
                                     // compressed header.
}

interface RequestOptions extends SendOptions {
  timeoutMs?: number;                // 0 disables timeout
}
```

`ClientMessage` (OneWay) exposes `payload`/`data`, `bizType`, `messageId`, `extHeader` — and no `respond` at all: the split is typed, not a runtime flag. `ClientRequest` adds `respond(payload, options?)`, which answers at most once, reuses the original `messageId`, and refuses to write onto a later connection generation (a stale response after a reconnect could complete a different server-side request that drew the same id).

## Rust ↔ TypeScript mapping

`@msgtrans/client` is the language mapping of `msgtrans::transport::TransportClient`. Concept-level alignment is exact; the API surface uses TypeScript-idiomatic shapes (options objects + callback events) but each method has a clear Rust counterpart.

| Rust | TypeScript |
|---|---|
| `TransportClientBuilder::new().with_protocol(WebSocketClientConfig::new(url)?).build().await?` | `new TransportClient({ url })` |
| `client.connect().await?` | `await client.connect()` |
| `client.disconnect().await?` | `await client.disconnect()` (or `client.close()`) |
| `client.is_connected().await` | `client.isConnected()` (sync) |
| `client.send_with_options(bytes, SendOptions::new().biz_type(5))` | `client.send(bytes, { bizType: 5 })` |
| `client.request_with_options(bytes, RequestOptions::new().timeout(d))` | `client.request(bytes, { timeoutMs: 10_000 })` |
| `ClientEvent::Message(msg)` | `client.on('message', msg => ...)` |
| `ClientEvent::Request(req)` | `client.on('request', req => ...)` |
| `ClientEvent::MessageSent { message_id }` (write-confirmed) | `client.on('messageEnqueued', ...)` — **enqueue-confirmed**: a browser WebSocket offers no write completion, so the honest tier is named for what it proves |
| `ClientEvent::Disconnected { reason }` | `client.on('close', ev => ...)` |
| `ClientEvent::Error { error }` | `client.on('error', e => ...)` |
| `Responder::respond(bytes)` / `respond_with_options` | `await req.respond(bytes, options?)` |

**Things deliberately not mirrored** (TS-idiomatic instead):

- No `TransportClientBuilder` — options object is more natural in TS
- Event delivery is callback-based, not a `subscribe_events()` stream — same information, JS-idiomatic shape
- Browser `WebSocket` cannot send raw ping/pong frames, so Rust's `with_ping_interval` / `with_pong_timeout` is not exposed; application-layer heartbeat will land in v0.2 with an explicit `heartbeatPacketFactory` hook

## Wire format

`@msgtrans/client` implements the v1 protocol defined in [`msgtrans/docs/WIRE_FORMAT.md`](https://github.com/zoujiaqing/msgtrans/blob/main/docs/WIRE_FORMAT.md):

- 16-byte fixed header, big-endian
- Three packet types: `OneWay` / `Request` / `Response`
- Compression byte is part of the header but compression itself is currently treated by the client as opaque (the bytes are passed through; an upper layer can apply zlib/zstd)
- Each WebSocket binary frame carries exactly one Packet — no length-prefix framing is needed at this layer

Cross-language conformance is enforced: every release runs the same fixture set on both the Rust crate and this package, and asserts byte-for-byte equality in both directions.

## Transport scope

| Transport | Browser | Node | Status |
|---|---|---|---|
| WebSocket | ✅ native | ✅ via `webSocketFactory` (e.g. `ws`) | **supported** |
| WebTransport (HTTP/3) | ✅ in modern Chromium / Firefox / Safari | — | roadmap (separate adapter) |
| TCP | — | — | not in scope (browser cannot do raw TCP) |
| QUIC (raw) | — | — | not in scope (no browser API) |

WebTransport will land as a separate `Transport` implementation; the existing `TransportClient` API stays stable.

## Roadmap

- [x] **0.1.0-beta** — wire format, WebSocket transport, request/response, server-push handling
- [x] **0.2.0-beta** — 2.0 semantic alignment: strict decode + `DecodeLimits`, transport-owned ids (no wrap), socket & respond generations, serialized inbound normalization, zlib via native streams, codec SPI (zstd injectable), honest enqueue tier, projected backpressure, API surface shrink
- [x] **0.3.0-beta** — `HeartbeatMonitor` (caller-supplied probe, idle-based), `ReconnectManager` (backoff, blunt stop-first contract), `WebTransportTransport` (HTTP/3, Rust-QUIC-mirror framing, generation-isolated)
- [ ] later — Zstd codec package, WebTransport server interop E2E (needs a server endpoint)

## License

[Apache-2.0](./LICENSE)
