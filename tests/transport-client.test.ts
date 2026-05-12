import { describe, expect, it } from 'vitest';
import {
  CompressionType,
  Packet,
  PacketType,
  TransportClient,
  TransportContext,
  TransportError,
  UnsupportedCompressionError,
  type WebSocketLike,
} from '../src/index.js';
import { FakeTransport } from './fake-transport.js';

function newClient(transport: FakeTransport): TransportClient {
  return new TransportClient({ transport });
}

function makeOneWayInbound(payload = 'hi', bizType = 0): Packet {
  return new Packet({
    packetType: PacketType.OneWay,
    messageId: 555,
    bizType,
    payload: new TextEncoder().encode(payload),
  });
}

function makeRequestInbound(messageId: number, bizType = 7): Packet {
  return new Packet({
    packetType: PacketType.Request,
    messageId,
    bizType,
    payload: new TextEncoder().encode('please respond'),
  });
}

describe('TransportClient — lifecycle', () => {
  it('connect/close pass through to transport', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();
    expect(t.connectCalls).toHaveLength(1);
    await c.close();
    expect(t.closeCalls).toHaveLength(1);
  });

  it('throws when neither url nor transport is provided', () => {
    expect(() => new TransportClient({})).toThrow(TransportError);
  });

  it('uses an injected Transport instance verbatim', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    expect(t.connectCalls).toHaveLength(1);
  });
});

describe('TransportClient — send / request convenience methods', () => {
  it('send() builds a OneWay packet with payload + bizType', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.send(new TextEncoder().encode('hello'), { bizType: 5 });

    expect(t.sent).toHaveLength(1);
    const p = t.sent[0]!;
    expect(p.packetType).toBe(PacketType.OneWay);
    expect(p.bizType).toBe(5);
    expect(p.messageId).toBe(1);
    expect(new TextDecoder().decode(p.payload)).toBe('hello');
  });

  it('request() returns the response payload bytes', async () => {
    const t = new FakeTransport();
    const c = newClient(t);

    const promise = c.request(new TextEncoder().encode('ping'), {
      bizType: 100,
    });

    expect(t.sent).toHaveLength(1);
    const sent = t.sent[0]!;
    expect(sent.packetType).toBe(PacketType.Request);
    expect(sent.bizType).toBe(100);
    const reqId = sent.messageId;

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: reqId,
        bizType: 100,
        payload: new TextEncoder().encode('pong'),
      }),
    );

    const respBytes = await promise;
    expect(new TextDecoder().decode(respBytes)).toBe('pong');
  });

  it('sendPacket() / requestPacket() expose the raw Packet', async () => {
    const t = new FakeTransport();
    const c = newClient(t);

    const oneway = new Packet({
      packetType: PacketType.OneWay,
      messageId: 0,
      bizType: 9,
      payload: new TextEncoder().encode('data'),
    });
    await c.sendPacket(oneway);
    expect(oneway.messageId).toBe(1);

    const reqPacket = new Packet({
      packetType: PacketType.OneWay,
      messageId: 0,
      bizType: 11,
      payload: new TextEncoder().encode('q'),
    });
    const promise = c.requestPacket(reqPacket);
    const reqId = reqPacket.messageId;

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: reqId,
        bizType: 11,
        payload: new TextEncoder().encode('a'),
      }),
    );
    const fullResponse = await promise;
    expect(fullResponse).toBeInstanceOf(Packet);
    expect(fullResponse.bizType).toBe(11);
    expect(new TextDecoder().decode(fullResponse.payload)).toBe('a');
  });
});

describe('TransportClient — message routing', () => {
  it('OneWay packets surface via on("message") with TransportContext', () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: TransportContext[] = [];
    c.on('message', (ctx) => received.push(ctx));

    t.fireMessage(makeOneWayInbound('hi', 3));

    expect(received).toHaveLength(1);
    const ctx = received[0]!;
    expect(ctx.packetType).toBe(PacketType.OneWay);
    expect(ctx.bizType).toBe(3);
    expect(ctx.messageId).toBe(555);
    expect(new TextDecoder().decode(ctx.data)).toBe('hi');
    expect(ctx.isRequest).toBe(false);
  });

  it('Response packets matching pending request do NOT trigger message event', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: TransportContext[] = [];
    c.on('message', (ctx) => received.push(ctx));

    const promise = c.request(new TextEncoder().encode('ping'));
    const reqId = t.sent[0]!.messageId;

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: reqId,
        payload: new TextEncoder().encode('pong'),
      }),
    );

    await promise;
    expect(received).toHaveLength(0);
  });

  it('Response with unknown messageId DOES surface as message (orphan response)', () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: TransportContext[] = [];
    c.on('message', (ctx) => received.push(ctx));

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: 9999,
        payload: new Uint8Array(),
      }),
    );

    expect(received).toHaveLength(1);
    expect(received[0]!.packetType).toBe(PacketType.Response);
  });

  it('Request packets surface via on("message") and ctx.respond() sends a Response', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: TransportContext[] = [];
    c.on('message', (ctx) => received.push(ctx));

    const inboundReq = makeRequestInbound(12345, 50);
    t.fireMessage(inboundReq);

    expect(received).toHaveLength(1);
    const ctx = received[0]!;
    expect(ctx.isRequest).toBe(true);
    expect(ctx.messageId).toBe(12345);

    await ctx.respond(new TextEncoder().encode('here you go'));

    expect(t.sent).toHaveLength(1);
    const resp = t.sent[0]!;
    expect(resp.packetType).toBe(PacketType.Response);
    expect(resp.messageId).toBe(12345); // reused
    expect(resp.bizType).toBe(50); // inherited
    expect(new TextDecoder().decode(resp.payload)).toBe('here you go');
    expect(ctx.hasResponded).toBe(true);
  });

  it('respond() can override bizType', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: TransportContext | null = null;
    c.on('message', (ctx) => {
      captured = ctx;
    });
    t.fireMessage(makeRequestInbound(1, 50));
    await captured!.respond(new Uint8Array(), { bizType: 99 });
    expect(t.sent[0]!.bizType).toBe(99);
  });

  it('respond() rejects on non-Request packets', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: TransportContext | null = null;
    c.on('message', (ctx) => {
      captured = ctx;
    });
    t.fireMessage(makeOneWayInbound('hi'));

    await expect(
      captured!.respond(new TextEncoder().encode('nope')),
    ).rejects.toBeInstanceOf(TransportError);
  });

  it('respond() rejects when called twice', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: TransportContext | null = null;
    c.on('message', (ctx) => {
      captured = ctx;
    });
    t.fireMessage(makeRequestInbound(7));

    await captured!.respond(new Uint8Array());
    await expect(
      captured!.respond(new Uint8Array()),
    ).rejects.toBeInstanceOf(TransportError);
  });
});

describe('TransportClient — close / error events', () => {
  it('close event propagates to listeners', () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const closes: unknown[] = [];
    c.on('close', (ev) => closes.push(ev));
    t.fireClose({ code: 1001 });
    expect(closes).toEqual([{ code: 1001 }]);
  });

  it('error event propagates to listeners', () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const errors: unknown[] = [];
    c.on('error', (e) => errors.push(e));
    const boom = new Error('boom');
    t.fireError(boom);
    expect(errors).toEqual([boom]);
  });

  it('on() returns an unsubscribe function', () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: TransportContext[] = [];
    const off = c.on('message', (ctx) => received.push(ctx));

    t.fireMessage(makeOneWayInbound('first'));
    off();
    t.fireMessage(makeOneWayInbound('second'));

    expect(received).toHaveLength(1);
    expect(new TextDecoder().decode(received[0]!.data)).toBe('first');
  });
});

describe('TransportClient — Rust-compat surface (B-mini)', () => {
  it('disconnect() is an alias for close()', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();
    await c.disconnect();
    expect(t.closeCalls).toHaveLength(1);
  });

  it('isConnected() reflects underlying transport state', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    expect(c.isConnected()).toBe(false);

    await c.connect();
    expect(c.isConnected()).toBe(true);

    t.fireClose();
    expect(c.isConnected()).toBe(false);
  });

  it('messageSent event fires after each successful send (OneWay)', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    const sent: number[] = [];
    c.on('messageSent', (info) => sent.push(info.messageId));

    await c.send(new TextEncoder().encode('a'));
    await c.send(new TextEncoder().encode('b'));

    expect(sent).toEqual([1, 2]);
  });

  it('messageSent event fires for the Request portion of request()', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    const sent: number[] = [];
    c.on('messageSent', (info) => sent.push(info.messageId));

    const promise = c.request(new TextEncoder().encode('q'));
    expect(sent).toEqual([1]); // Request was sent (synchronously through FakeTransport)

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: 1,
        payload: new TextEncoder().encode('a'),
      }),
    );
    await promise;
  });

  it('on("messageSent") returns an unsubscribe function', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    const sent: number[] = [];
    const off = c.on('messageSent', (info) => sent.push(info.messageId));

    await c.send(new TextEncoder().encode('first'));
    off();
    await c.send(new TextEncoder().encode('second'));

    expect(sent).toEqual([1]);
  });

  it('SendOptions.extHeader is propagated onto the wire packet', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    const ext = new TextEncoder().encode('route:msg');
    await c.send(new TextEncoder().encode('payload'), {
      bizType: 7,
      extHeader: ext,
    });

    expect(t.sent).toHaveLength(1);
    const p = t.sent[0]!;
    expect(p.bizType).toBe(7);
    expect(Array.from(p.extHeader)).toEqual(Array.from(ext));
  });

  it('SendOptions.reserved is propagated onto the wire packet', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    await c.send(new TextEncoder().encode('p'), { reserved: 0x0006 });
    expect(t.sent[0]!.reserved).toBe(0x0006);
  });

  it('SendOptions.compression=None is allowed and reaches the packet', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    await c.send(new TextEncoder().encode('p'), {
      compression: CompressionType.None,
    });
    expect(t.sent[0]!.compression).toBe(CompressionType.None);
  });

  it('send() rejects with UnsupportedCompressionError when compression != None', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    await expect(
      c.send(new TextEncoder().encode('p'), {
        compression: CompressionType.Zlib,
      }),
    ).rejects.toBeInstanceOf(UnsupportedCompressionError);

    expect(t.sent).toHaveLength(0); // nothing left the client
  });

  it('request() rejects with UnsupportedCompressionError when compression != None', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    await expect(
      c.request(new TextEncoder().encode('p'), {
        compression: CompressionType.Zstd,
      }),
    ).rejects.toBeInstanceOf(UnsupportedCompressionError);
  });

  it('ctx.respond() rejects with UnsupportedCompressionError when compression != None', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: TransportContext | null = null;
    c.on('message', (ctx) => {
      captured = ctx;
    });
    t.fireMessage(
      new Packet({
        packetType: PacketType.Request,
        messageId: 99,
        bizType: 5,
        payload: new TextEncoder().encode('?'),
      }),
    );

    await expect(
      captured!.respond(new TextEncoder().encode('!'), {
        compression: CompressionType.Zlib,
      }),
    ).rejects.toBeInstanceOf(UnsupportedCompressionError);
    expect(captured!.hasResponded).toBe(false);
  });

  it('ctx.respond() propagates extHeader and reserved', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: TransportContext | null = null;
    c.on('message', (ctx) => {
      captured = ctx;
    });
    t.fireMessage(
      new Packet({
        packetType: PacketType.Request,
        messageId: 7,
        bizType: 5,
        payload: new TextEncoder().encode('?'),
      }),
    );

    const ext = new TextEncoder().encode('route:reply');
    await captured!.respond(new TextEncoder().encode('!'), {
      extHeader: ext,
      reserved: 0x0002,
    });

    expect(t.sent).toHaveLength(1);
    const r = t.sent[0]!;
    expect(r.messageId).toBe(7); // reused
    expect(Array.from(r.extHeader)).toEqual(Array.from(ext));
    expect(r.reserved).toBe(0x0002);
  });
});

describe('TransportClient — default WebSocket transport', () => {
  // Mock matching WebSocketLike contract for default-path coverage.
  class MockWS implements WebSocketLike {
    binaryType = 'blob';
    readyState = 0;
    onopen: ((ev: unknown) => void) | null = null;
    onmessage: ((ev: { data: unknown }) => void) | null = null;
    onerror: ((ev: unknown) => void) | null = null;
    onclose: ((ev: unknown) => void) | null = null;
    sent: Uint8Array[] = [];
    send(data: ArrayBufferLike | ArrayBufferView | string): void {
      if (data instanceof Uint8Array) {
        this.sent.push(new Uint8Array(data));
      } else if (data instanceof ArrayBuffer) {
        this.sent.push(new Uint8Array(data));
      }
    }
    close(): void {
      this.readyState = 3;
      this.onclose?.({ code: 1000 });
    }
  }

  it('constructs a WebSocketTransport when given url + factory', async () => {
    const ws = new MockWS();
    const c = new TransportClient({
      url: 'ws://test.invalid',
      webSocketFactory: () => ws,
    });

    const connectPromise = c.connect();
    ws.readyState = 1;
    ws.onopen?.({});
    await connectPromise;

    await c.send(new TextEncoder().encode('payload'), { bizType: 1 });
    expect(ws.sent).toHaveLength(1);

    // Roundtrip: parse what was sent and confirm semantics.
    const parsed = Packet.decode(ws.sent[0]!);
    expect(parsed.packetType).toBe(PacketType.OneWay);
    expect(parsed.bizType).toBe(1);
    expect(new TextDecoder().decode(parsed.payload)).toBe('payload');
  });
});
