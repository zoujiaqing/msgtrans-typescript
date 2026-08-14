// WebTransport connector (experimental): length-prefixed byte-stream framing
// over an in-memory fake session, standalone and under a full TransportClient.
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
  WebTransportConnector,
  type WebTransportLike,
} from '../src/experimental.js';
import { until } from './fake-transport.js';

const text = (s: string) => new TextEncoder().encode(s);
const decode = (b: Uint8Array) => new TextDecoder().decode(b);

/** In-memory WebTransport session: the test holds the server ends. */
class FakeWt implements WebTransportLike {
  readonly ready: Promise<void> = Promise.resolve();
  readonly closed: Promise<unknown>;
  private resolveClosed!: (v: unknown) => void;
  readonly toClient: WritableStreamDefaultWriter<Uint8Array>;
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

async function openSession(
  wt: FakeWt,
  limits?: { maxPayloadLen?: number },
) {
  const connector = new WebTransportConnector({
    url: 'https://test.invalid/wt',
    webTransportFactory: () => wt,
    limits,
  });
  const link = await connector.connect({
    signal: new AbortController().signal,
  });
  const received: Packet[] = [];
  const errors: unknown[] = [];
  const closes: unknown[] = [];
  link.subscribe({
    onMessage: (p) => received.push(p),
    onError: (e) => errors.push(e),
    onClose: (ev) => closes.push(ev),
  });
  return { link, received, errors, closes };
}

describe('WebTransportConnector — framing', () => {
  it('roundtrips a packet and length-prefixes outbound frames', async () => {
    const wt = new FakeWt();
    const { link, received } = await openSession(wt);

    // Do NOT await the enqueue before reading: the writer promise is REAL
    // backpressure (TransformStream readable HWM=0 pulls on read); awaiting
    // first would deadlock.
    const sending = link.enqueue(Packet.oneWay(7, text('up')));
    const { value } = await wt.fromClient.read();
    await sending;
    const view = new DataView(value!.buffer, value!.byteOffset);
    expect(view.getUint32(0, false)).toBe(value!.length - 4);
    expect(decode(Packet.decodeExact(value!.slice(4)).payload)).toBe('up');

    await wt.push(frame(Packet.oneWay(8, text('down'))));
    await until(() => received.length === 1);
    expect(decode(received[0]!.payload)).toBe('down');
  });

  it('reassembles a frame split across arbitrary chunk boundaries', async () => {
    const wt = new FakeWt();
    const { received } = await openSession(wt);

    const whole = frame(Packet.oneWay(1, text('split into pieces')));
    await wt.push(whole.slice(0, 2)); // inside the length prefix
    await wt.push(whole.slice(2, 9)); // inside the body
    await wt.push(whole.slice(9));
    await until(() => received.length === 1);
    expect(decode(received[0]!.payload)).toBe('split into pieces');
  });

  it('delivers multiple frames arriving in one chunk, in order', async () => {
    const wt = new FakeWt();
    const { received } = await openSession(wt);

    const a = frame(Packet.oneWay(1, text('a')));
    const b = frame(Packet.oneWay(2, text('b')));
    const both = new Uint8Array(a.length + b.length);
    both.set(a);
    both.set(b, a.length);
    await wt.push(both);
    await until(() => received.length === 2);
    expect(received.map((p) => decode(p.payload))).toEqual(['a', 'b']);
  });

  it('a declared length over the configured cap is fatal BEFORE buffering', async () => {
    const wt = new FakeWt();
    const { link, errors } = await openSession(wt, { maxPayloadLen: 1024 });

    const evil = new Uint8Array(4);
    new DataView(evil.buffer).setUint32(0, 512 * 1024 * 1024, false);
    await wt.push(evil);
    await until(() => errors.length > 0);
    expect(errors[0]).toBeInstanceOf(DecodeLimitError);
    expect(link.isOpen()).toBe(false);
  });

  it('an undecodable frame closes the session (strict)', async () => {
    const wt = new FakeWt();
    const { link, errors } = await openSession(wt);

    const garbage = new Uint8Array(4 + 16);
    new DataView(garbage.buffer).setUint32(0, 16, false);
    garbage[4] = 9; // bad version
    await wt.push(garbage);
    await until(() => errors.length > 0);
    expect(link.isOpen()).toBe(false);
  });

  it('refuses to enqueue ReservedFlag.Fragmented', async () => {
    const wt = new FakeWt();
    const { link } = await openSession(wt);
    const p = Packet.oneWay(1, text('x'));
    p.setFlag(ReservedFlag.Fragmented);
    await expect(link.enqueue(p)).rejects.toBeInstanceOf(WireFormatError);
  });

  it('sessions are independent objects: closing one cannot touch another', async () => {
    // The connector-model version of the old "stale session" test: each
    // connect returns a NEW session owning its own connection, so cross-
    // session interference is unrepresentable rather than guarded.
    const oldWt = new FakeWt();
    const old = await openSession(oldWt);
    const freshWt = new FakeWt();
    const fresh = await openSession(freshWt);

    await old.link.close();
    await oldWt.push(frame(Packet.oneWay(1, text('ghost')))).catch(() => {});
    await new Promise((r) => setTimeout(r, 30));

    expect(old.received).toHaveLength(0); // closed session delivers nothing
    expect(fresh.received).toHaveLength(0); // and nothing leaked across
    expect(fresh.link.isOpen()).toBe(true);
    expect(fresh.closes).toHaveLength(0);
  });

  it('an aborted dial closes the connection it would have won', async () => {
    let releaseReady!: () => void;
    const gate = new Promise<void>((r) => {
      releaseReady = r;
    });
    class GatedWt extends FakeWt {
      override readonly ready: Promise<void> = gate;
    }
    const wt = new GatedWt();
    const connector = new WebTransportConnector({
      url: 'https://test.invalid/wt',
      webTransportFactory: () => wt,
    });
    const abort = new AbortController();
    const dialing = connector.connect({ signal: abort.signal });
    abort.abort();
    releaseReady();
    await expect(dialing).rejects.toThrow();
    expect(wt.closeCalls).toBeGreaterThan(0); // released, not leaked
  });
});

describe('WebTransportConnector — under a full TransportClient', () => {
  it('request/response works end to end over the byte stream', async () => {
    const wt = new FakeWt();
    const client = new TransportClient({
      transport: new WebTransportConnector({
        url: 'https://test.invalid/wt',
        webTransportFactory: () => wt,
      }),
    });
    await client.connect();

    const serve = (async () => {
      let buf = new Uint8Array(0);
      for (;;) {
        const { done, value } = await wt.fromClient.read();
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
        await wt.push(frame(resp));
        return;
      }
    })();

    const response = await client.request(text('ask'), { timeoutMs: 5_000 });
    expect(decode(response)).toBe('answered over WT');
    await serve;
    await client.disconnect();
  });
});
