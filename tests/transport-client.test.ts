import { describe, expect, it } from 'vitest';
import {
  type ClientMessage,
  type ClientRequest,
  CompressionType,
  Packet,
  PacketType,
  TransportClient,
  TransportError,
  UnsupportedCompressionError,
  type WebSocketLike,
} from '../src/index.js';
import { FakeTransport, flush } from './fake-transport.js';

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
    await c.disconnect();
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
    await flush(); // enqueue rides the outbound chain

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

  it('ids belong to the transport: no public path accepts a caller-numbered packet', () => {
    // `sendPacket()` / `requestPacket()` / `initialMessageId` are gone: a
    // caller-numbered Request could complete a DIFFERENT pending waiter that
    // happened to draw the same id (the ABA the Rust 2.0 registry closed).
    const t = new FakeTransport();
    const c = newClient(t) as unknown as Record<string, unknown>;
    expect(c['sendPacket']).toBeUndefined();
    expect(c['requestPacket']).toBeUndefined();
  });
});

describe('TransportClient — message routing', () => {
  it('OneWay packets surface via on("message") as ClientMessage', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: ClientMessage[] = [];
    c.on('message', (msg) => received.push(msg));

    t.fireMessage(makeOneWayInbound('hi', 3));
    await flush();

    expect(received).toHaveLength(1);
    const msg = received[0]!;
    expect(msg.bizType).toBe(3);
    expect(msg.messageId).toBe(555);
    expect(new TextDecoder().decode(msg.data)).toBe('hi');
    expect(msg.kind).toBe('message');
  });

  it('Response packets matching pending request do NOT trigger message event', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: ClientMessage[] = [];
    c.on('message', (msg) => received.push(msg));

    const promise = c.request(new TextEncoder().encode('ping'));
    await flush();
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

  it('Response with unknown messageId is DROPPED (reported via error channel, never as message)', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: ClientMessage[] = [];
    const errors: unknown[] = [];
    c.on('message', (msg) => received.push(msg));
    c.on('error', (e) => errors.push(e));

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: 9999,
        payload: new Uint8Array(),
      }),
    );
    await flush();

    expect(received).toHaveLength(0);
    expect(errors).toHaveLength(1);
  });

  it('Request packets surface via on("request") as ClientRequest, NOT via on("message")', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const messages: ClientMessage[] = [];
    const requests: ClientRequest[] = [];
    c.on('message', (m) => messages.push(m));
    c.on('request', (r) => requests.push(r));

    t.fireMessage(makeRequestInbound(12345, 50));
    await flush();

    expect(messages).toHaveLength(0);
    expect(requests).toHaveLength(1);
    const req = requests[0]!;
    expect(req.messageId).toBe(12345);

    await req.respond(new TextEncoder().encode('here you go'));

    expect(t.sent).toHaveLength(1);
    const resp = t.sent[0]!;
    expect(resp.packetType).toBe(PacketType.Response);
    expect(resp.messageId).toBe(12345); // reused
    expect(resp.bizType).toBe(50); // inherited
    expect(new TextDecoder().decode(resp.payload)).toBe('here you go');
    expect(req.hasResponded).toBe(true);
  });

  it('respond() can override bizType', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: ClientRequest | null = null;
    c.on('request', (r) => {
      captured = r;
    });
    t.fireMessage(makeRequestInbound(1, 50));
    await flush();
    await captured!.respond(new Uint8Array(), { bizType: 99 });
    expect(t.sent[0]!.bizType).toBe(99);
  });

  it('a OneWay has no respond() at all (typed split, not a runtime check)', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: ClientMessage | null = null;
    c.on('message', (m) => {
      captured = m;
    });
    t.fireMessage(makeOneWayInbound('hi'));
    await flush();

    expect(
      (captured as unknown as Record<string, unknown>)['respond'],
    ).toBeUndefined();
  });

  it('respond() rejects when called twice', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: ClientRequest | null = null;
    c.on('request', (r) => {
      captured = r;
    });
    t.fireMessage(makeRequestInbound(7));
    await flush();

    await captured!.respond(new Uint8Array());
    await expect(captured!.respond(new Uint8Array())).rejects.toBeInstanceOf(
      TransportError,
    );
  });

  it('respond() refuses to write onto a LATER connection generation', async () => {
    // A stale response written after a reconnect could complete a DIFFERENT
    // server-side request that drew the same id — the cross-generation leak
    // Rust 2.0 closed with generation-bound claims.
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: ClientRequest | null = null;
    c.on('request', (r) => {
      captured = r;
    });
    t.fireMessage(makeRequestInbound(42));
    await flush();

    t.fireClose(); // connection turns over
    await c.connect(); // new generation

    await expect(captured!.respond(new Uint8Array())).rejects.toThrow(
      /connection changed/,
    );
    expect(t.sent).toHaveLength(0); // nothing reached the wire
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

  it('on() returns an unsubscribe function', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    const received: ClientMessage[] = [];
    const off = c.on('message', (m) => received.push(m));

    t.fireMessage(makeOneWayInbound('first'));
    await flush();
    off();
    t.fireMessage(makeOneWayInbound('second'));
    await flush();

    expect(received).toHaveLength(1);
    expect(new TextDecoder().decode(received[0]!.data)).toBe('first');
  });
});

describe('TransportClient — Rust-compat surface', () => {
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

  it('messageEnqueued fires after each accepted send (honest tier: enqueue, not write)', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    const enqueued: number[] = [];
    c.on('messageEnqueued', (info) => enqueued.push(info.messageId));

    await c.send(new TextEncoder().encode('a'));
    await c.send(new TextEncoder().encode('b'));

    expect(enqueued).toEqual([1, 2]);
  });

  it('messageEnqueued fires for the Request portion of request()', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    const enqueued: number[] = [];
    c.on('messageEnqueued', (info) => enqueued.push(info.messageId));

    const promise = c.request(new TextEncoder().encode('q'));
    await flush();
    expect(enqueued).toEqual([1]);

    t.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: 1,
        payload: new TextEncoder().encode('a'),
      }),
    );
    await promise;
  });

  it('on("messageEnqueued") returns an unsubscribe function', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    const enqueued: number[] = [];
    const off = c.on('messageEnqueued', (info) => enqueued.push(info.messageId));

    await c.send(new TextEncoder().encode('first'));
    off();
    await c.send(new TextEncoder().encode('second'));

    expect(enqueued).toEqual([1]);
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

  it('respond() propagates extHeader', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    let captured: ClientRequest | null = null;
    c.on('request', (r) => {
      captured = r;
    });
    t.fireMessage(makeRequestInbound(7, 5));
    await flush();

    const ext = new TextEncoder().encode('route:reply');
    await captured!.respond(new TextEncoder().encode('!'), { extHeader: ext });

    expect(t.sent).toHaveLength(1);
    const r = t.sent[0]!;
    expect(r.messageId).toBe(7); // reused
    expect(Array.from(r.extHeader)).toEqual(Array.from(ext));
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

  it('send() rejects with UnsupportedCompressionError for an unregistered codec (Zstd)', async () => {
    const t = new FakeTransport();
    const c = newClient(t);
    await c.connect();

    await expect(
      c.send(new TextEncoder().encode('p'), {
        compression: CompressionType.Zstd,
      }),
    ).rejects.toBeInstanceOf(UnsupportedCompressionError);

    expect(t.sent).toHaveLength(0); // nothing left the client
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
    const parsed = Packet.decodeExact(ws.sent[0]!);
    expect(parsed.packetType).toBe(PacketType.OneWay);
    expect(parsed.bizType).toBe(1);
    expect(new TextDecoder().decode(parsed.payload)).toBe('payload');
  });
});
