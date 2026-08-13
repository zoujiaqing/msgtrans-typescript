// GPT seventh-review counterexamples: session-ownership faults that survived
// the per-promise generation checks. Each was reproduced against the pre-fix
// implementation, then closed by the internal Session refactor (one object
// owns the id allocator, request manager, normalizer and outbound queue;
// closing it voids every piece of in-flight async work at once).
import { describe, expect, it } from 'vitest';
import {
  CompressionType,
  HeartbeatMonitor,
  Packet,
  PacketType,
  QueueOverflowError,
  ReconnectManager,
  TransportClient,
  TransportState,
  WebSocketTransport,
  type CompressionCodec,
  type EnqueuedInfo,
  type Transport,
  type WebSocketLike,
} from '../src/index.js';
import * as rootApi from '../src/index.js';
import { FakeTransport, flush, until } from './fake-transport.js';

const text = (s: string) => new TextEncoder().encode(s);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A codec whose compress/decompress block until the test releases them. */
function gatedCodec(): {
  codec: CompressionCodec;
  releaseCompress: () => void;
  releaseDecompress: () => void;
} {
  let releaseCompress!: () => void;
  let releaseDecompress!: () => void;
  const compressGate = new Promise<void>((r) => {
    releaseCompress = r;
  });
  const decompressGate = new Promise<void>((r) => {
    releaseDecompress = r;
  });
  const codec: CompressionCodec = {
    type: CompressionType.Zstd,
    async compress(input) {
      await compressGate;
      return input;
    },
    async decompress(input) {
      await decompressGate;
      return input;
    },
  };
  return { codec, releaseCompress, releaseDecompress };
}

describe('P0-1: async sends cannot cross a reconnect', () => {
  it('a compressed send started on the old connection is refused after reconnect', async () => {
    const { codec, releaseCompress } = gatedCodec();
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t, codecs: [codec] });
    await c.connect();

    // The send enters its (gated) compression on generation 1…
    const sending = c.send(text('non-idempotent side effect'), {
      compression: CompressionType.Zstd,
    });
    const observed = sending.then(
      () => 'sent',
      () => 'refused',
    );

    // …the connection turns over…
    t.fireClose();
    await c.connect();

    // …and the compression finally completes. Pre-fix this hit the NEW
    // connection's wire and resolved successfully — an automatic replay of
    // a request the application never chose to retry.
    releaseCompress();
    expect(await observed).toBe('refused');
    expect(t.sent).toHaveLength(0);
  });
});

describe('P0-2: concurrent connect() is single-flight', () => {
  class SlowConnectTransport extends FakeTransport {
    connectStarts = 0;
    private release: Array<() => void> = [];
    override async connect(): Promise<void> {
      this.connectStarts += 1;
      await new Promise<void>((r) => this.release.push(r));
      await super.connect();
    }
    finishOne(): void {
      this.release.shift()?.();
    }
  }

  it('two racing connect() calls share one attempt and one generation', async () => {
    const t = new SlowConnectTransport();
    const c = new TransportClient({ transport: t });

    const first = c.connect();
    const second = c.connect();
    expect(t.connectStarts).toBe(1); // single-flight: no second dial
    t.finishOne();
    await Promise.all([first, second]);

    const r1 = c.request(text('a'), { timeoutMs: 0 });
    await flush();
    const r2 = c.request(text('b'), { timeoutMs: 0 });
    await flush();
    // Pre-fix: each completion path restarted the id space; ids came out
    // [1, 1] and the second pending overwrote the first.
    expect(t.sent.map((p) => p.messageId)).toEqual([1, 2]);
    void r1.catch(() => {});
    void r2.catch(() => {});
  });
});

describe('P0-3: ownership does not depend on the adapter firing close', () => {
  /** A transport whose close() NEVER emits an onClose event. */
  class SilentCloseTransport extends FakeTransport {
    override async close(): Promise<void> {
      // state flips, but no close event reaches the client — the SPI abuse
      // the generation counter silently depended on.
      (this as unknown as { _isConnected: boolean })._isConnected = false;
      this.closeCalls.push(Date.now());
    }
  }

  it("an old session's in-flight decompression is void even without a close event", async () => {
    const { codec, releaseDecompress } = gatedCodec();
    const t = new SilentCloseTransport();
    const c = new TransportClient({ transport: t, codecs: [codec] });
    await c.connect();
    const received: string[] = [];
    c.on('message', (m) => received.push(new TextDecoder().decode(m.payload)));

    // Old session starts a (gated) decompression…
    t.fireMessage(
      new Packet({
        packetType: PacketType.OneWay,
        messageId: 1,
        compression: CompressionType.Zstd,
        payload: text('ghost'),
      }),
    );
    // …the app disconnects. The adapter emits NO close event, and the app
    // does NOT reconnect — disconnect() alone must void the session.
    await c.disconnect();

    releaseDecompress();
    await sleep(50);
    // Pre-fix: nothing retired the session (no close event, no reconnect),
    // so the ghost delivered after the app had hung up.
    expect(received).toEqual([]);
  });
});

describe('P0-4: WebSocket state cannot be revived by late events', () => {
  class ManualWS implements WebSocketLike {
    binaryType = 'blob';
    readyState = 0;
    onopen: ((ev: unknown) => void) | null = null;
    onmessage: ((ev: { data: unknown }) => void) | null = null;
    onerror: ((ev: unknown) => void) | null = null;
    onclose: ((ev: unknown) => void) | null = null;
    send(): void {}
    close(): void {
      this.readyState = 3;
      /* never fires onclose — hung platform */
    }
  }

  it('a late onopen after close-during-Connecting cannot reopen the transport', async () => {
    const ws = new ManualWS();
    const transport = new WebSocketTransport({
      url: 'ws://test.invalid',
      webSocketFactory: () => ws,
    });
    const connecting = transport.connect().catch(() => {});
    await transport.close();
    // The platform's late onopen arrives AFTER the deterministic close.
    ws.readyState = 1;
    ws.onopen?.({});
    await connecting;
    expect(transport.state).toBe(TransportState.Closed);
    expect(transport.isConnected()).toBe(false);
  });

  it('onerror without onclose does not wedge Connecting (a fresh connect gets a fresh dial)', async () => {
    const sockets: ManualWS[] = [];
    const transport = new WebSocketTransport({
      url: 'ws://test.invalid',
      webSocketFactory: () => {
        const ws = new ManualWS();
        sockets.push(ws);
        return ws;
      },
    });
    const first = transport.connect();
    sockets[0]!.onerror?.(new Error('handshake refused'));
    await expect(first).rejects.toThrow();
    // Pre-fix: state stayed Connecting and every later connect() was handed
    // back the SAME rejected promise.
    const second = transport.connect();
    expect(sockets).toHaveLength(2);
    sockets[1]!.readyState = 1;
    sockets[1]!.onopen?.({});
    await second;
    expect(transport.isConnected()).toBe(true);
  });
});

describe('P1: poisoning covers the packet already in flight', () => {
  it('overflow-poisoning also voids the head packet still being decompressed', async () => {
    const { codec, releaseDecompress } = gatedCodec();
    const t = new FakeTransport();
    const c = new TransportClient({
      transport: t,
      codecs: [codec],
      maxInboundQueueFrames: 2,
    });
    await c.connect();
    const received: string[] = [];
    const errors: unknown[] = [];
    c.on('message', (m) => received.push(new TextDecoder().decode(m.payload)));
    c.on('error', (e) => errors.push(e));

    // Head packet ENTERS its gated decompression (flush so the chain item
    // is really inside the await — poisoning must land AFTER that to
    // exercise the post-await recheck, not the queue-entry check)…
    t.fireMessage(
      new Packet({
        packetType: PacketType.OneWay,
        messageId: 1,
        compression: CompressionType.Zstd,
        payload: text('head'),
      }),
    );
    await flush();
    // …two more overflow the 2-frame cap → fatal + poison…
    t.fireMessage(Packet.oneWay(2, text('q1')));
    t.fireMessage(Packet.oneWay(3, text('q2')));
    t.fireMessage(Packet.oneWay(4, text('overflow')));
    await until(() => errors.length > 0);

    // …and the head finally completes. Pre-fix it was still delivered.
    releaseDecompress();
    await sleep(50);
    expect(received).toEqual([]);
  });
});

describe('P1: helper epochs cover success paths and restarts', () => {
  it('after stop();start(), a hung old probe does not block the new cycle', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    let probes = 0;
    let firstProbe = true;
    const hb = new HeartbeatMonitor(c, {
      intervalMs: 80,
      timeoutMs: 10_000,
      probe: () => {
        probes += 1;
        if (firstProbe) {
          firstProbe = false;
          return new Promise(() => {}); // hangs forever
        }
        return Promise.resolve();
      },
      onDead: () => {},
    });
    hb.start();
    await until(() => probes === 1, 3000);
    hb.stop();
    hb.start();
    // Pre-fix: the shared `probing` flag stayed true for the hung probe and
    // the restarted monitor never probed again.
    await until(() => probes >= 2, 3000);
    hb.stop();
  });

  it('stop();start() during a backoff sleep does not let the OLD cycle zombie-connect', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const rm = new ReconnectManager(c, { initialDelayMs: 60, jitterRatio: 0 });
    rm.start();

    t.fireClose(); // old cycle enters its 60ms backoff
    await sleep(15);
    rm.stop();
    rm.start(); // new lifecycle, but NO close has happened in it
    const before = t.connectCalls.length;
    await sleep(150); // old cycle's sleep expires
    // Pre-fix: the old cycle woke, saw active=true (the NEW lifecycle's
    // flag) and dialed a zombie reconnect.
    expect(t.connectCalls.length).toBe(before);
    rm.stop();
  });
});

describe('P1: strict config validation is complete', () => {
  it('connectRetry rejects negative/fractional/non-finite values', () => {
    const mk = (connectRetry: Record<string, number>) => () =>
      new TransportClient({ transport: new FakeTransport(), connectRetry });
    expect(mk({ maxRetries: -1 })).toThrow();
    expect(mk({ maxRetries: 1.5 })).toThrow();
    expect(mk({ initialDelayMs: Number.NaN })).toThrow();
    expect(mk({ multiplier: 0.5 })).toThrow();
  });

  it('reconnect rejects Infinity multiplier and fractional maxAttempts; Infinity maxAttempts is legal', () => {
    const c = new TransportClient({ transport: new FakeTransport() });
    expect(() => new ReconnectManager(c, { multiplier: Infinity })).toThrow();
    expect(() => new ReconnectManager(c, { maxAttempts: 1.5 })).toThrow();
    expect(
      () => new ReconnectManager(c, { maxAttempts: Infinity }),
    ).not.toThrow();
  });

  it('timeouts and buffer marks are validated', () => {
    expect(
      () =>
        new TransportClient({
          transport: new FakeTransport(),
          defaultTimeoutMs: Number.NaN,
        }),
    ).toThrow();
    expect(
      () =>
        new WebSocketTransport({ url: 'ws://x', maxBufferedBytes: -1 }),
    ).toThrow();
    const c = new TransportClient({ transport: new FakeTransport() });
    void expect(c.request(text('x'), { timeoutMs: -5 })).rejects.toThrow();
  });
});

describe('P1: handles are interfaces; requests have ONE consumer', () => {
  it('ClientMessage/ClientRequest are not exported as constructible values', () => {
    expect((rootApi as Record<string, unknown>)['ClientRequest']).toBeUndefined();
    expect((rootApi as Record<string, unknown>)['ClientMessage']).toBeUndefined();
  });

  it('handles carry a kind discriminant', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    const kinds: string[] = [];
    c.on('message', (m) => kinds.push(m.kind));
    c.on('request', (r) => kinds.push(r.kind));
    t.fireMessage(Packet.oneWay(1, text('m')));
    t.fireMessage(Packet.request(2, text('r')));
    await until(() => kinds.length === 2);
    expect(kinds.sort()).toEqual(['message', 'request']);
  });

  it('a second request consumer is refused (single response obligation)', () => {
    const c = new TransportClient({ transport: new FakeTransport() });
    const off = c.on('request', () => {});
    expect(() => c.on('request', () => {})).toThrow();
    off();
    expect(() => c.on('request', () => {})).not.toThrow();
  });
});

describe('P2: queue caps are byte-aware on both directions', () => {
  it('outbound overflow rejects with QueueOverflowError carrying frames+bytes', async () => {
    const { codec } = gatedCodec(); // never released: compression hangs
    const t = new FakeTransport();
    const c = new TransportClient({
      transport: t,
      codecs: [codec],
      maxOutboundQueue: 2,
    });
    await c.connect();

    const inflight = [
      c.send(text('a'), { compression: CompressionType.Zstd }),
      c.send(text('b'), { compression: CompressionType.Zstd }),
    ];
    for (const p of inflight) void p.catch(() => {});
    await expect(
      c.send(text('c'), { compression: CompressionType.Zstd }),
    ).rejects.toBeInstanceOf(QueueOverflowError);
  });

  it('outbound byte cap trips before the frame cap for large payloads', async () => {
    const { codec } = gatedCodec();
    const t = new FakeTransport();
    const c = new TransportClient({
      transport: t,
      codecs: [codec],
      maxOutboundQueueBytes: 1024,
    });
    await c.connect();
    const big = new Uint8Array(900);
    void c
      .send(big, { compression: CompressionType.Zstd })
      .catch(() => {});
    await expect(
      c.send(big, { compression: CompressionType.Zstd }),
    ).rejects.toBeInstanceOf(QueueOverflowError);
  });
});

// Referenced to keep the imports honest.
void ((): Transport | EnqueuedInfo | null => null);
