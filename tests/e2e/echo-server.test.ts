// E2E against the dedicated WS-only `e2e_echo_server` (fast smoke).
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import WS from 'ws';
import {
  CompressionType,
  TransportClient,
  type WebSocketLike,
} from '../../src/index.js';
import {
  DEFAULT_MAX_EXT_HEADER_LEN,
  DEFAULT_MAX_PAYLOAD_LEN,
} from '../../src/constants.js';
import { spawnDedicatedEchoServer, type SpawnedServer } from './setup.js';
import { defineEchoSuite } from './echo-suite.js';

const PORT = 18080;
/** Server-side trigger: echo with a Zlib-compressed body. */
const BIZ_COMPRESS = 251;
let server: SpawnedServer;

beforeAll(async () => {
  server = await spawnDedicatedEchoServer(PORT);
});

afterAll(async () => {
  if (server) await server.stop();
});

defineEchoSuite('E2E (dedicated) — Rust e2e_echo_server ↔ TS TransportClient', {
  url: () => `ws://127.0.0.1:${PORT}`,
});

it('negotiates msgtrans.v1 with the Rust WebSocket endpoint', async () => {
  let socket: WS | undefined;
  const client = new TransportClient({
    url: server.url,
    protocols: ['msgtrans.v1'],
    webSocketFactory: (url, protocols) => {
      socket = new WS(url, protocols);
      return socket as unknown as WebSocketLike;
    },
  });
  try {
    await client.connect();
    expect(socket?.protocol).toBe('msgtrans.v1');
    const body = new Uint8Array([0, 1, 255]);
    expect(await client.request(body)).toEqual(body);
  } finally {
    await client.shutdown();
  }
});

it('round-trips a maximum-size default frame with both headers', async () => {
  const client = new TransportClient({
    url: server.url,
    webSocketFactory: (url, protocols) =>
      new WS(url, protocols) as unknown as WebSocketLike,
  });
  try {
    await client.connect();
    const body = Buffer.alloc(DEFAULT_MAX_PAYLOAD_LEN, 0xa5);
    const reply = await client.request(body, {
      extHeader: new Uint8Array(DEFAULT_MAX_EXT_HEADER_LEN),
    });
    expect(Buffer.from(reply).equals(body)).toBe(true);
  } finally {
    await client.shutdown();
  }
});

describe('E2E zlib interop — real Rust 2.0 compression on both sides', () => {
  function createClient(): TransportClient {
    return new TransportClient({
      url: `ws://127.0.0.1:${PORT}`,
      webSocketFactory: (u, p) => new WS(u, p) as unknown as WebSocketLike,
    });
  }
  const text = (s: string) => new TextEncoder().encode(s);
  const decode = (b: Uint8Array) => new TextDecoder().decode(b);

  it('TS compressed REQUEST → Rust normalizes → echoed back intact', async () => {
    const client = createClient();
    await client.connect();
    const body = 'compressed request payload '.repeat(64);
    // The server sees PLAINTEXT (its inbound normalization undid the zlib),
    // so an identical echo proves the TS compressor produced real zlib.
    const response = await client.request(text(body), {
      compression: CompressionType.Zlib,
      timeoutMs: 10_000,
    });
    expect(decode(response)).toBe(body);
    await client.disconnect();
  });

  it('Rust compressed RESPONSE (respond_with_options) → TS delivers plaintext', async () => {
    const client = createClient();
    await client.connect();
    const body = 'compressed response payload '.repeat(64);
    const response = await client.request(text(body), {
      bizType: BIZ_COMPRESS,
      timeoutMs: 10_000,
    });
    expect(decode(response)).toBe(body);
    await client.disconnect();
  });

  it('Rust compressed PUSH (send_data_with_options) → TS message is plaintext', async () => {
    const client = createClient();
    await client.connect();
    const body = 'compressed push payload '.repeat(64);
    const received: string[] = [];
    client.on('message', (m) => received.push(decode(m.payload)));

    await client.send(text(body), { bizType: BIZ_COMPRESS });
    const deadline = Date.now() + 10_000;
    while (received.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 20));
    }
    expect(received).toEqual([body]);
    await client.disconnect();
  });
});
