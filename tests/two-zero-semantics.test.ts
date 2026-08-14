/**
 * Regression suite for the 2.0 semantic alignment (0.2.0), one test per
 * closed defect. Each meaningful assertion was verified to FAIL against the
 * pre-alignment implementation (see the CHANGELOG's falsification notes).
 */
import { describe, expect, it } from 'vitest';
import {
  BackpressureError,
  ClientMessage,
  ClientRequest,
  CompressionType,
  DecodeLimitError,
  Packet,
  PacketType,
  TransportClient,
  UnknownWireValueError,
  UnsupportedCompressionError,
  WebSocketConnector,
  type WebSocketLike,
  WireFormatError,
  ZlibCodec,
} from '../src/index.js';
import { FakeTransport, flush, until } from './fake-transport.js';

const text = (s: string) => new TextEncoder().encode(s);
const decode = (b: Uint8Array) => new TextDecoder().decode(b);

// ---------------------------------------------------------------------------
// Strict decode
// ---------------------------------------------------------------------------

describe('strict decode', () => {
  const validFrame = (mutate?: (bytes: Uint8Array) => void): Uint8Array => {
    const bytes = new Packet({
      packetType: PacketType.OneWay,
      messageId: 1,
      payload: text('body'),
    }).encode();
    mutate?.(bytes);
    return bytes;
  };

  it('an unknown packet-type byte is a protocol error, not a bogus enum member', () => {
    const bytes = validFrame((b) => {
      b[2] = 7;
    });
    expect(() => Packet.decodeExact(bytes)).toThrow(UnknownWireValueError);
  });

  it('an unknown compression byte is a protocol error', () => {
    const bytes = validFrame((b) => {
      b[1] = 9;
    });
    expect(() => Packet.decodeExact(bytes)).toThrow(UnknownWireValueError);
  });

  it('trailing bytes after the frame are a protocol error (one WS message = one packet)', () => {
    const frame = validFrame();
    const padded = new Uint8Array(frame.length + 3);
    padded.set(frame);
    expect(() => Packet.decodeExact(padded)).toThrow(WireFormatError);
  });

  it('a declared payload over the configured limit is rejected before allocation', () => {
    const bytes = validFrame();
    expect(() =>
      Packet.decodeExact(bytes, { maxPayloadLen: 2, maxExtHeaderLen: 0xffff }),
    ).toThrow(DecodeLimitError);
  });

  it('encode enforces the SAME caps, so this client cannot build a frame the server must kill', () => {
    const over = new Packet({
      packetType: PacketType.OneWay,
      messageId: 1,
      payload: new Uint8Array(64),
    });
    expect(() =>
      over.encode({ maxPayloadLen: 16, maxExtHeaderLen: 0xffff }),
    ).toThrow(DecodeLimitError);
  });
});

// ---------------------------------------------------------------------------
// Zlib codec + inbound normalization
// ---------------------------------------------------------------------------

describe('zlib codec', () => {
  it('roundtrips through the native streams', async () => {
    const codec = new ZlibCodec();
    const input = text('compressible '.repeat(500));
    const packed = await codec.compress(input, 16 * 1024 * 1024);
    expect(packed.byteLength).toBeLessThan(input.byteLength);
    const restored = await codec.decompress(packed, 16 * 1024 * 1024);
    expect(decode(restored)).toBe(decode(input));
  });

  it('decompression cancels the moment output exceeds the limit (bomb defense)', async () => {
    const codec = new ZlibCodec();
    // 4 MiB of zeros compresses to ~4 KiB: the classic bomb shape.
    const bomb = await codec.compress(new Uint8Array(4 * 1024 * 1024), 16 * 1024 * 1024);
    expect(bomb.byteLength).toBeLessThan(64 * 1024);
    await expect(codec.decompress(bomb, 1024 * 1024)).rejects.toBeInstanceOf(
      DecodeLimitError,
    );
  });
});

describe('inbound normalization', () => {
  async function compressedInbound(body: string, messageId = 5): Promise<Packet> {
    const codec = new ZlibCodec();
    return new Packet({
      packetType: PacketType.OneWay,
      messageId,
      bizType: 9,
      compression: CompressionType.Zlib,
      payload: await codec.compress(text(body), 16 * 1024 * 1024),
    });
  }

  it('a compressed inbound packet reaches the app as PLAINTEXT with the flag cleared', async () => {
    // The defect this closes: compressed packets were handed to the app
    // as-is — compressed bytes under a still-set compression flag, silently.
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const received: ClientMessage[] = [];
    c.on('message', (m) => received.push(m));

    t.fireMessage(await compressedInbound('the real content'));
    await until(() => received.length === 1);

    expect(decode(received[0]!.payload)).toBe('the real content');
  });

  it('an algorithm with no codec (Zstd) is FATAL: error + connection closed, nothing delivered', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const received: ClientMessage[] = [];
    const errors: unknown[] = [];
    c.on('message', (m) => received.push(m));
    c.on('error', (e) => errors.push(e));

    t.fireMessage(
      new Packet({
        packetType: PacketType.OneWay,
        messageId: 1,
        compression: CompressionType.Zstd,
        payload: new Uint8Array([1, 2, 3]),
      }),
    );
    await until(
      () =>
        errors.some((e) => e instanceof UnsupportedCompressionError) &&
        t.closeCalls.length > 0,
    );
    expect(received).toHaveLength(0);
  });

  it('a corrupt compressed body is FATAL: error + connection closed', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const errors: unknown[] = [];
    c.on('error', (e) => errors.push(e));

    t.fireMessage(
      new Packet({
        packetType: PacketType.OneWay,
        messageId: 1,
        compression: CompressionType.Zlib,
        payload: text('this is not zlib data'),
      }),
    );
    await until(() => errors.length > 0 && t.closeCalls.length > 0);
  });

  it('delivery order is ARRIVAL order even when a compressed packet precedes a plain one', async () => {
    // Async decompression without serialization would let the later plain
    // packet overtake the earlier compressed one.
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const order: string[] = [];
    c.on('message', (m) => order.push(decode(m.payload)));

    t.fireMessage(await compressedInbound('first (compressed)', 1));
    t.fireMessage(
      new Packet({
        packetType: PacketType.OneWay,
        messageId: 2,
        payload: text('second (plain)'),
      }),
    );
    await until(() => order.length === 2);

    expect(order).toEqual(['first (compressed)', 'second (plain)']);
  });

  it('a compressed RESPONSE resolves the pending request with plaintext', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const promise = c.request(text('ask'));
    await flush();
    const reqId = t.sent[0]!.messageId;

    const codec = new ZlibCodec();
    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: reqId,
        compression: CompressionType.Zlib,
        payload: await codec.compress(text('the answer'), 16 * 1024 * 1024),
      }),
    );

    expect(decode(await promise)).toBe('the answer');
  });
});

// ---------------------------------------------------------------------------
// Outbound compression
// ---------------------------------------------------------------------------

describe('outbound compression', () => {
  it('send() with Zlib puts a genuinely compressed body on the wire', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const body = text('repetitive '.repeat(200));

    await c.send(body, { compression: CompressionType.Zlib });

    expect(t.sent).toHaveLength(1);
    const p = t.sent[0]!;
    expect(p.compression).toBe(CompressionType.Zlib);
    expect(p.payload.byteLength).toBeLessThan(body.byteLength);
    const restored = await new ZlibCodec().decompress(
      p.payload,
      16 * 1024 * 1024,
    );
    expect(decode(restored)).toBe(decode(body));
  });

  it('program order survives a compressed send followed by a plain one', async () => {
    // The outbound mirror of the inbound ordering rule: compression is
    // async, so without the outbound chain the plain packet would hit the
    // wire first.
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();

    void c.send(text('a '.repeat(500)), { compression: CompressionType.Zlib });
    void c.send(text('plain'));
    await until(() => t.sent.length === 2);

    expect(t.sent[0]!.compression).toBe(CompressionType.Zlib);
    expect(decode(t.sent[1]!.payload)).toBe('plain');
  });

  it('respond() with Zlib compresses the response body', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    let captured: ClientRequest | null = null;
    c.on('request', (r) => {
      captured = r;
    });
    t.fireMessage(
      new Packet({
        packetType: PacketType.Request,
        messageId: 77,
        bizType: 4,
        payload: text('?'),
      }),
    );
    await flush();

    await captured!.respond(text('answer '.repeat(200)), {
      compression: CompressionType.Zlib,
    });

    expect(t.sent).toHaveLength(1);
    expect(t.sent[0]!.compression).toBe(CompressionType.Zlib);
    expect(t.sent[0]!.messageId).toBe(77);
  });
});

// ---------------------------------------------------------------------------
// Timeout starts at enqueue
// ---------------------------------------------------------------------------

describe('request timeout starts at enqueue', () => {
  it('a slow enqueue does not eat the response budget', async () => {
    // Old order started the clock before send(); a 60 ms enqueue with a
    // 100 ms timeout left the peer only 40 ms. Now the peer gets its full
    // 100 ms measured from enqueue completion.
    const t = new FakeTransport();
    t.sendDelayMs = 60;
    const c = new TransportClient({ transport: t });
    await c.connect();

    const promise = c.request(text('ask'), { timeoutMs: 100 });
    await new Promise((r) => setTimeout(r, 90)); // enqueue at 60, +30 into budget
    expect(t.sent).toHaveLength(1);

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: t.sent[0]!.messageId,
        payload: text('made it'),
      }),
    );
    expect(decode(await promise)).toBe('made it');
  });

  it('an enqueue failure rejects immediately with the send error, not a timeout', async () => {
    const t = new FakeTransport();
    t.sendShouldFail = new Error('enqueue exploded');
    const c = new TransportClient({ transport: t });
    await c.connect();
    await expect(c.request(text('ask'), { timeoutMs: 50 })).rejects.toThrow(
      'enqueue exploded',
    );
  });
});

// ---------------------------------------------------------------------------
// WebSocket generation isolation + projected backpressure
// ---------------------------------------------------------------------------

class GhostWS implements WebSocketLike {
  binaryType = 'blob';
  readyState = 0;
  bufferedAmount?: number;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  readonly sent: Uint8Array[] = [];
  send(data: ArrayBufferLike | ArrayBufferView | string): void {
    if (ArrayBuffer.isView(data)) {
      const v = data as ArrayBufferView;
      this.sent.push(new Uint8Array(v.buffer, v.byteOffset, v.byteLength).slice());
    }
  }
  close(): void {
    this.readyState = 3;
    queueMicrotask(() => this.onclose?.({ code: 1000 }));
  }
  open(): void {
    this.readyState = 1;
    this.onopen?.({});
  }
}

describe('WebSocket session isolation (connector model)', () => {
  async function reconnected() {
    const sockets: GhostWS[] = [];
    const connector = new WebSocketConnector({
      url: 'ws://test.invalid',
      webSocketFactory: () => {
        const ws = new GhostWS();
        sockets.push(ws);
        return ws;
      },
    });
    const dial = () =>
      connector.connect({ signal: new AbortController().signal });
    const d1 = dial();
    sockets[0]!.open();
    const oldLink = await d1;
    // The connection drops and a NEW session is dialed.
    sockets[0]!.readyState = 3;
    sockets[0]!.onclose?.({ code: 1006 });
    const d2 = dial();
    sockets[1]!.open();
    const freshLink = await d2;
    const fresh = { received: [] as Packet[], errors: [] as unknown[], closes: [] as unknown[] };
    freshLink.subscribe({
      onMessage: (p) => fresh.received.push(p),
      onError: (e) => fresh.errors.push(e),
      onClose: (ev) => fresh.closes.push(ev),
    });
    return { oldLink, freshLink, fresh, old: sockets[0]! };
  }

  it("an OLD socket's late onclose cannot close the replacement session", async () => {
    const { freshLink, fresh, old } = await reconnected();
    old.onclose?.({ code: 1006, reason: 'late ghost' });
    expect(freshLink.isOpen()).toBe(true);
    expect(fresh.closes).toHaveLength(0);
  });

  it("an OLD socket's late onmessage cannot deliver into the new session", async () => {
    const { fresh, old } = await reconnected();
    const ghost = new Packet({
      packetType: PacketType.OneWay,
      messageId: 1,
      payload: text('ghost'),
    }).encode();
    old.onmessage?.({ data: ghost.buffer.slice(0) });
    expect(fresh.received).toHaveLength(0);
  });

  it("an OLD socket's late onerror cannot poison the new session", async () => {
    const { freshLink, fresh, old } = await reconnected();
    old.onerror?.(new Error('ghost error'));
    expect(fresh.errors).toHaveLength(0);
    expect(freshLink.isOpen()).toBe(true);
  });
});

describe('projected backpressure', () => {
  it('rejects when buffered + THIS frame would cross the mark', async () => {
    // The old check compared only the current level, so a large frame at
    // just-under-the-mark sailed through and blew the budget.
    const ws = new GhostWS();
    const connector = new WebSocketConnector({
      url: 'ws://test.invalid',
      webSocketFactory: () => ws,
      maxBufferedBytes: 1024,
    });
    const dialing = connector.connect({ signal: new AbortController().signal });
    ws.open();
    const link = await dialing;

    ws.bufferedAmount = 1000; // under the mark on its own
    const big = new Packet({
      packetType: PacketType.OneWay,
      messageId: 1,
      payload: new Uint8Array(512), // projection: 1000 + 528 > 1024
    });
    await expect(link.enqueue(big)).rejects.toBeInstanceOf(BackpressureError);
    expect(ws.sent).toHaveLength(0);

    ws.bufferedAmount = 100; // now the projection fits
    await link.enqueue(big);
    expect(ws.sent).toHaveLength(1);
  });
});

describe('strict inbound framing closes the session', () => {
  it('an undecodable frame is fatal, not a logged shrug', async () => {
    const ws = new GhostWS();
    const connector = new WebSocketConnector({
      url: 'ws://test.invalid',
      webSocketFactory: () => ws,
    });
    const dialing = connector.connect({ signal: new AbortController().signal });
    ws.open();
    const link = await dialing;
    const errors: unknown[] = [];
    link.subscribe({ onError: (e) => errors.push(e) });

    const garbage = new Uint8Array([2, 9, 9, 9, 0, 0, 0, 0]); // bad version, short
    ws.onmessage?.({ data: garbage.buffer });

    expect(errors).toHaveLength(1);
    expect(ws.readyState).toBe(3); // session closed the socket
    expect(link.isOpen()).toBe(false);
  });
});
