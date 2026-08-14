// GPT sixth-review counterexamples: lifecycle / generation faults the 125
// green tests did not cover. Written FIRST (each reproduced the fault against
// the pre-fix implementation), then fixed.
import { describe, expect, it } from 'vitest';
import {
  CompressionType,
  Packet,
  PacketType,
  TransportClient,
  WebSocketConnector,
  ZlibCodec,
  type WebSocketLike,
} from '../src/index.js';
import { FakeTransport, flush, until } from './fake-transport.js';

const text = (s: string) => new TextEncoder().encode(s);
const decode = (b: Uint8Array) => new TextDecoder().decode(b);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('P0-1: connect() on a live connection must not restart the id space', () => {
  it('a redundant connect() while a request is pending does not reuse its id', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ connector: t });
    await c.connect();

    const first = c.request(text('one'), { timeoutMs: 0 });
    await flush();
    expect(t.sent[0]!.messageId).toBe(1);

    // The connection has NOT turned over — this is the same live transport.
    await c.connect();

    const second = c.request(text('two'), { timeoutMs: 0 });
    await flush();
    // Pre-fix: startGeneration() ran unconditionally, both requests got id 1,
    // the second overwrote the first's pending entry and stranded its promise.
    expect(t.sent[1]!.messageId).toBe(2);

    t.fireMessage(
      new Packet({ packetType: PacketType.Response, messageId: 1, payload: text('r1') }),
    );
    t.fireMessage(
      new Packet({ packetType: PacketType.Response, messageId: 2, payload: text('r2') }),
    );
    expect(decode(await first)).toBe('r1');
    expect(decode(await second)).toBe('r2');
  });
});

describe('P0-2: inbound normalization is generation-bound and poisonable', () => {
  async function zlibPacket(body: string): Promise<Packet> {
    return new Packet({
      packetType: PacketType.OneWay,
      messageId: 1,
      compression: CompressionType.Zlib,
      payload: await new ZlibCodec().compress(text(body), 16 * 1024 * 1024),
    });
  }

  it("a previous generation's in-flight decompression cannot deliver into the new one", async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ connector: t });
    await c.connect();
    const received: string[] = [];
    c.on('message', (m) => received.push(decode(m.payload)));

    // Old generation submits a compressed packet (async decompress)…
    t.fireMessage(await zlibPacket('ghost from generation 0'));
    // …and the connection turns over before it completes.
    t.fireClose();
    await c.connect();

    await sleep(80); // let the stale decompression finish
    expect(received).toEqual([]); // nothing from the dead generation

    // The NEW generation still works.
    t.fireMessage(
      new Packet({ packetType: PacketType.OneWay, messageId: 2, payload: text('live') }),
    );
    await until(() => received.length === 1);
    expect(received).toEqual(['live']);
  });

  it('after a fatal normalization failure, already-queued packets are NOT delivered', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ connector: t });
    await c.connect();
    const received: string[] = [];
    const errors: unknown[] = [];
    c.on('message', (m) => received.push(decode(m.payload)));
    c.on('error', (e) => errors.push(e));

    // A corrupt compressed packet followed IMMEDIATELY by a plain one: the
    // plain packet is queued behind the failing decompression. A poisoned
    // stream delivers nothing further — the peer and this client no longer
    // agree on the protocol, so everything after the violation is suspect.
    t.fireMessage(
      new Packet({
        packetType: PacketType.OneWay,
        messageId: 1,
        compression: CompressionType.Zlib,
        payload: text('not zlib at all'),
      }),
    );
    t.fireMessage(
      new Packet({ packetType: PacketType.OneWay, messageId: 2, payload: text('tail') }),
    );

    await until(() => errors.length > 0 && t.closeCalls.length > 0);
    await sleep(50);
    expect(received).toEqual([]);
  });
});

describe('P0-3: connect/close lifecycle is cancel-safe (client gate)', () => {
  class HangingWS implements WebSocketLike {
    binaryType = 'blob';
    readyState = 0;
    onopen: ((ev: unknown) => void) | null = null;
    onmessage: ((ev: { data: unknown }) => void) | null = null;
    onerror: ((ev: unknown) => void) | null = null;
    onclose: ((ev: unknown) => void) | null = null;
    send(): void {}
    close(): void {
      this.readyState = 3;
      // A hung handshake: the platform may never fire onclose.
    }
  }

  it('disconnect() during connect() rejects the pending connect (hung handshake included)', async () => {
    const c = new TransportClient({
      url: 'ws://test.invalid',
      webSocketFactory: () => new HangingWS(),
    });
    const connecting = c.connect();
    const observed = connecting.then(
      () => 'resolved',
      () => 'rejected',
    );
    await c.disconnect();
    const outcome = await Promise.race([
      observed,
      sleep(200).then(() => 'pending'),
    ]);
    expect(outcome).toBe('rejected');
    expect(c.isConnected()).toBe(false);
  });

  it('a dial that wins AFTER disconnect() is closed, not installed', async () => {
    // The eighth review's exact counterexample: the in-flight
    // transport.connect used to complete after disconnect() and still
    // install a session — isConnected() came back true.
    const sockets: HangingWS[] = [];
    const c = new TransportClient({
      url: 'ws://test.invalid',
      webSocketFactory: () => {
        const ws = new HangingWS();
        sockets.push(ws);
        return ws;
      },
    });
    const connecting = c.connect().catch(() => {});
    const disconnecting = c.disconnect();
    // The platform completes the handshake AFTER the abort.
    sockets[0]!.readyState = 1;
    sockets[0]!.onopen?.({});
    await connecting;
    await disconnecting;
    expect(c.isConnected()).toBe(false);
  });

  it('shutdown() during connect() leaves the client Stopped and connect() refused forever', async () => {
    const c = new TransportClient({
      url: 'ws://test.invalid',
      webSocketFactory: () => new HangingWS(),
    });
    const connecting = c.connect().catch(() => {});
    await c.shutdown();
    await connecting;
    expect(c.isConnected()).toBe(false);
    await expect(c.connect()).rejects.toThrow(/shut down/);
  });

  it('a connector factory that throws synchronously does not wedge later connects', async () => {
    let attempts = 0;
    const good = new HangingWS();
    const c = new TransportClient({
      url: 'ws://test.invalid',
      webSocketFactory: () => {
        attempts += 1;
        if (attempts === 1) throw new Error('no socket here');
        return good;
      },
    });
    await expect(c.connect()).rejects.toThrow('no socket here');
    const second = c.connect();
    good.readyState = 1;
    good.onopen?.({});
    await second;
    expect(c.isConnected()).toBe(true);
  });
});


describe('P1: stale async results cannot fire', () => {
  it('a probe still in flight when HeartbeatMonitor.stop() runs cannot declare death', async () => {
    const { HeartbeatMonitor } = await import('../src/index.js');
    const t = new FakeTransport();
    const c = new TransportClient({ connector: t });
    await c.connect();
    let failProbe!: (e: Error) => void;
    const dead: unknown[] = [];
    const hb = new HeartbeatMonitor(c, {
      intervalMs: 60,
      timeoutMs: 5_000,
      probe: () =>
        new Promise((_, reject) => {
          failProbe = reject;
        }),
      onDead: (e) => dead.push(e),
    });
    hb.start();
    await until(() => failProbe !== undefined, 3000);
    hb.stop(); // e.g. the app reconnected and re-armed a fresh monitor
    failProbe(new Error('stale probe failure'));
    await sleep(50);
    expect(dead).toEqual([]); // the stale verdict must be void
  });

  it('a close landing while a reconnect cycle is finishing is not swallowed', async () => {
    const { ReconnectManager } = await import('../src/index.js');
    const t = new FakeTransport();
    const c = new TransportClient({ connector: t });
    await c.connect();
    // DETERMINISTIC race: drop the connection again from INSIDE the
    // 'connected' callback — at that instant the cycle is still running
    // (cycleRunning=true), which is exactly the window where the old code
    // swallowed the close, reported connected, and left the client down.
    let droppedAgain = false;
    const rm = new ReconnectManager(c, {
      initialDelayMs: 10,
      jitterRatio: 0,
      onStateChange: (p) => {
        if (p === 'connected' && !droppedAgain) {
          droppedAgain = true;
          t.fireClose();
        }
      },
    });
    rm.start();

    t.fireClose(); // cycle 1 starts; its success immediately drops again
    await until(() => droppedAgain && c.isConnected(), 3000);
    rm.stop();
    expect(c.isConnected()).toBe(true);
  });
});

describe('P1: sealed handles and validated config', () => {
  it('ClientRequest is not even a VALUE any more (interface-only export)', async () => {
    const api = await import('../src/index.js');
    expect((api as Record<string, unknown>)['ClientRequest']).toBeUndefined();
  });

  it('NaN limits are rejected instead of disabling the bomb defense', () => {
    expect(
      () =>
        new TransportClient({
          connector: new FakeTransport(),
          limits: { maxPayloadLen: Number.NaN },
        }),
    ).toThrow();
    expect(
      () =>
        new TransportClient({
          connector: new FakeTransport(),
          limits: { maxPayloadLen: -5 },
        }),
    ).toThrow();
  });

  it('invalid reconnect/heartbeat numbers are rejected', async () => {
    const { ReconnectManager, HeartbeatMonitor } = await import('../src/index.js');
    const c = new TransportClient({ connector: new FakeTransport() });
    expect(() => new ReconnectManager(c, { initialDelayMs: -1 })).toThrow();
    expect(() => new ReconnectManager(c, { multiplier: 0.5 })).toThrow();
    expect(
      () => new HeartbeatMonitor(c, { probe: async () => {}, intervalMs: Number.NaN }),
    ).toThrow();
  });
});
