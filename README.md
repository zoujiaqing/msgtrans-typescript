# @msgtrans/client

TypeScript client runtime for the [msgtrans](https://github.com/zoujiaqing/msgtrans) wire protocol.

`@msgtrans/client` is the language mapping of the `msgtrans` Rust client surface — same Packet semantics, same Request/Response correlation rules, same wire bytes. The wire format is defined by [`WIRE_FORMAT.md`](https://github.com/zoujiaqing/msgtrans/blob/main/docs/WIRE_FORMAT.md) and conformance is verified by a shared cross-language fixture suite.

> **Status: 0.1.0-beta.0.** Protocol layer + WebSocket transport + request/response are stable and fixture-verified end-to-end against the Rust server. Heartbeat, automatic reconnection, and compression are on the roadmap.

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
client.on('message', async (ctx) => {
  if (ctx.isRequest) {
    // Server is asking us something; respond with the same messageId.
    const answer = await handle(ctx.bizType, ctx.data);
    await ctx.respond(answer);
  } else {
    // Server-pushed OneWay event.
    handleEvent(ctx.bizType, ctx.data);
  }
});

client.on('messageSent', ({ messageId }) => {
  // Fired after every successful transport-level send (OneWay, Request, Response).
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

// Lower level — full Packet control:
client.sendPacket(packet: Packet): Promise<void>;
client.requestPacket(packet: Packet, options?: RequestOptions): Promise<Packet>;

// Events (each returns an unsubscribe function)
client.on('message',     (ctx: TransportContext) => void): () => void;
client.on('messageSent', (info: { messageId: number }) => void): () => void;
client.on('close',       (event?: unknown) => void): () => void;
client.on('error',       (error: unknown) => void): () => void;
```

```ts
interface SendOptions {
  bizType?: number;
  extHeader?: Uint8Array;
  reserved?: number;
  compression?: CompressionType;     // only CompressionType.None in v0.1
}

interface RequestOptions extends SendOptions {
  timeoutMs?: number;                // 0 disables timeout
}
```

`TransportContext` exposes `data: Uint8Array`, `bizType: number`, `messageId: number`, `packetType`, `isRequest: boolean`, `hasResponded: boolean`, and `respond(payload, options?: SendOptions)` — the latter is only valid on Request packets and reuses the original `messageId`.

## Rust ↔ TypeScript mapping

`@msgtrans/client` is the language mapping of `msgtrans::transport::TransportClient`. Concept-level alignment is exact; the API surface uses TypeScript-idiomatic shapes (options objects + callback events) but each method has a clear Rust counterpart.

| Rust | TypeScript |
|---|---|
| `TransportClientBuilder::new().with_protocol(WebSocketClientConfig::new(url)?).build().await?` | `new TransportClient({ url })` |
| `client.connect().await?` | `await client.connect()` |
| `client.disconnect().await?` | `await client.disconnect()` (or `client.close()`) |
| `client.is_connected().await` | `client.isConnected()` (sync) |
| `client.send_with_options(bytes, TransportOptions::new().with_biz_type(5).with_ext_header(...))` | `client.send(bytes, { bizType: 5, extHeader: ... })` |
| `client.request_with_options(bytes, TransportOptions::new().with_biz_type(100).with_timeout(d))` | `client.request(bytes, { bizType: 100, timeoutMs: 10_000 })` |
| `subscribe_events()` → `ClientEvent::MessageReceived(ctx)` | `client.on('message', ctx => ...)` |
| `ClientEvent::MessageSent { message_id }` | `client.on('messageSent', ({ messageId }) => ...)` |
| `ClientEvent::Disconnected { reason }` | `client.on('close', ev => ...)` |
| `ClientEvent::Error { error }` | `client.on('error', e => ...)` |
| `context.respond(bytes)` | `await ctx.respond(bytes)` |

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
- [ ] **0.2.0** — application-layer heartbeat hook, reconnect with backoff
- [ ] **0.3.0** — compression codec injection (zlib via native `DecompressionStream`, zstd as peer dependency)
- [ ] **0.4.0+** — WebTransport adapter

## License

[Apache-2.0](./LICENSE)
