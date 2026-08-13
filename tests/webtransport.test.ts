// WebTransport adapter (0.3.0): length-prefixed byte-stream framing over an
// in-memory fake session, exercised both standalone and under a full
// TransportClient (request/response + compression untouched).
import { describe, expect, it } from 'vitest';
import {
  DecodeLimitError,
  Packet,
  PacketType,
  ReservedFlag,
  TransportClient,
  WireFormatError,
} from '../src/index.js';
import {
  WebTransportTransport,
  type WebTransportLike,
} from '../src/experimental.js';
import { until } from './fake-transport.js';

const text = (s: string) => new TextEncoder().encode(s);
const decode = (b: Uint8Array) => new TextDecoder().decode(b);

/** In-memory WebTransport session: the test holds the server ends. */
class FakeSession implements WebTransportLike {
  readonly ready: Promise<void> = Promise.resolve();
  readonly closed: Promise<unknown>;
  private resolveClosed!: (v: unknown) => void;
  /** Server → client pipe (test writes here). */
  readonly toClient: WritableStreamDefaultWriter<Uint8Array>;
  /** Client → server pipe (test reads here). */
  readonly fromClient: ReadableStreamDefaultReader<Uint8Array>;
  private readonly clientReadable: ReadableStream<Uint8Array>;
  private readonly clientWritable: WritableStream<Uint8Array>;
  closeCalls = 0;

  constructor() {
    this.closed = new Promise((r) => {
      this.resolveClosed = r;
    });
    const s2c = new TransformStream<Uint8Array, Uint8Array>();
    const c2s = new TransformStream<Uint8Array, Uint8Array>();
    this.toClient = s2c.writable.getWriter();
    this.fromClient = c2s.readable.getReader();
    this.clientReadable = s2c.readable;
    this.clientWritable = c2s.writable;
  }

  close(): void {
    this.closeCalls += 1;
    this.resolveClosed({ closeCode: 0 });
  }

  async createBidirectionalStream() {
    return { readable: this.clientReadable, writable: this.clientWritable };
  }

  /** Push raw bytes at the client (already length-prefixed by the caller). */
  async push(bytes: Uint8Array): Promise<void> {
    await this.toClient.write(bytes);
  }
}

function frame(packet: Packet): Uint8Array {
  const body = packet.encode();
  const out = new Uint8Array(4 + body.length);
  new DataView(out.buffer).setUint32(0, body.length, false);
  out.set(body, 4);
  return out;
}

async function openTransport(session: FakeSession, limits?: { maxPayloadLen?: number }) {
  const transport = new WebTransportTransport({
    url: 'https://test.invalid/wt',
    webTransportFactory: () => session,
    limits,
  });
  await transport.connect();
  return transport;
}

describe('WebTransportTransport — framing', () => {
  it('roundtrips a packet and length-prefixes outbound frames', async () => {
    const session = new FakeSession();
    const transport = await openTransport(session);
    const received: Packet[] = [];
    transport.onMessage((p: Packet) => received.push(p));

    // Do NOT await the send before reading: the writer promise is REAL
    // backpressure (TransformStream readable HWM=0 pulls on read), so
    // awaiting first would deadlock — which is itself the proof that
    // enqueue-confirmed here means "accepted by the stream", not "buffered
    // into the void".
    const sending = transport.send(Packet.oneWay(7, text('up')));
    const { value } = await session.fromClient.read();
    await sending;
    const view = new DataView(value!.buffer, value!.byteOffset);
    expect(view.getUint32(0, false)).toBe(value!.length - 4);
    const echoed = Packet.decodeExact(value!.slice(4));
    expect(decode(echoed.payload)).toBe('up');

    await session.push(frame(Packet.oneWay(8, text('down'))));
    await until(() => received.length === 1);
    expect(decode(received[0]!.payload)).toBe('down');
  });

  it('reassembles a frame split across arbitrary chunk boundaries', async () => {
    const session = new FakeSession();
    const transport = await openTransport(session);
    const received: Packet[] = [];
    transport.onMessage((p: Packet) => received.push(p));

    const whole = frame(Packet.oneWay(1, text('split into pieces')));
    // Split INSIDE the length prefix and inside the body.
    await session.push(whole.slice(0, 2));
    await session.push(whole.slice(2, 9));
    await session.push(whole.slice(9));
    await until(() => received.length === 1);
    expect(decode(received[0]!.payload)).toBe('split into pieces');
  });

  it('delivers multiple frames arriving in one chunk, in order', async () => {
    const session = new FakeSession();
    const transport = await openTransport(session);
    const order: string[] = [];
    transport.onMessage((p: Packet) => order.push(decode(p.payload)));

    const a = frame(Packet.oneWay(1, text('a')));
    const b = frame(Packet.oneWay(2, text('b')));
    const both = new Uint8Array(a.length + b.length);
    both.set(a);
    both.set(b, a.length);
    await session.push(both);
    await until(() => order.length === 2);
    expect(order).toEqual(['a', 'b']);
  });

  it('a declared length over the configured cap is fatal BEFORE buffering', async () => {
    const session = new FakeSession();
    const transport = await openTransport(session, { maxPayloadLen: 1024 });
    const errors: unknown[] = [];
    transport.onError((e: unknown) => errors.push(e));

    const evil = new Uint8Array(4);
    new DataView(evil.buffer).setUint32(0, 512 * 1024 * 1024, false);
    await session.push(evil);
    await until(() => errors.length > 0);
    expect(errors[0]).toBeInstanceOf(DecodeLimitError);
    expect(transport.isConnected()).toBe(false);
  });

  it('an undecodable frame closes the connection (strict)', async () => {
    const session = new FakeSession();
    const transport = await openTransport(session);
    const errors: unknown[] = [];
    transport.onError((e: unknown) => errors.push(e));

    const garbage = new Uint8Array(4 + 16);
    new DataView(garbage.buffer).setUint32(0, 16, false);
    garbage[4] = 9; // bad version
    await session.push(garbage);
    await until(() => errors.length > 0);
    expect(transport.isConnected()).toBe(false);
  });

  it('refuses to send ReservedFlag.Fragmented', async () => {
    const session = new FakeSession();
    const transport = await openTransport(session);
    const p = Packet.oneWay(1, text('x'));
    p.setFlag(ReservedFlag.Fragmented);
    await expect(transport.send(p)).rejects.toBeInstanceOf(WireFormatError);
  });

  it("a STALE session's late traffic and close cannot touch the replacement", async () => {
    const old = new FakeSession();
    const transport = await openTransport(old);
    // Connection turns over.
    old.close();
    await until(() => !transport.isConnected());
    const fresh = new FakeSession();
    (transport as unknown as { opts: { webTransportFactory: () => FakeSession } })
      .opts.webTransportFactory = () => fresh;
    await transport.connect();

    const received: Packet[] = [];
    const closes: unknown[] = [];
    transport.onMessage((p: Packet) => received.push(p));
    transport.onClose((ev?: unknown) => closes.push(ev));

    await old.push(frame(Packet.oneWay(1, text('ghost')))); // stale traffic
    old.close(); // stale close (already-resolved promise; re-resolve is a no-op)
    await new Promise((r) => setTimeout(r, 50));

    expect(received).toHaveLength(0);
    expect(closes).toHaveLength(0);
    expect(transport.isConnected()).toBe(true);
  });
});

describe('WebTransportTransport — under a full TransportClient', () => {
  it('request/response works end to end over the byte stream', async () => {
    const session = new FakeSession();
    const client = new TransportClient({
      transport: new WebTransportTransport({
        url: 'https://test.invalid/wt',
        webTransportFactory: () => session,
      }),
    });
    await client.connect();

    // Echo server side: read the request frame, answer with a Response.
    const serve = (async () => {
      let buf = new Uint8Array(0);
      for (;;) {
        const { done, value } = await session.fromClient.read();
        if (done) return;
        const merged = new Uint8Array(buf.length + value.length);
        merged.set(buf);
        merged.set(value, buf.length);
        buf = merged;
        if (buf.length < 4) continue;
        const len = new DataView(buf.buffer).getUint32(0, false);
        if (buf.length < 4 + len) continue;
        const req = Packet.decodeExact(buf.slice(4, 4 + len));
        const resp = new Packet({
          packetType: PacketType.Response,
          messageId: req.messageId,
          bizType: req.bizType,
          payload: text('answered over WT'),
        });
        await session.push(frame(resp));
        return;
      }
    })();

    const response = await client.request(text('ask'), { timeoutMs: 5_000 });
    expect(decode(response)).toBe('answered over WT');
    await serve;
    await client.disconnect();
  });
});
