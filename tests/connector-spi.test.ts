// GPT eighth-review counterexamples, pinned against the Connector/Session
// SPI. Under the old shared-Transport SPI each of these reproduced; under
// per-connection sessions they are unrepresentable — these tests document
// the structural guarantee.
import { describe, expect, it } from 'vitest';
import {
  HeartbeatMonitor,
  Packet,
  PacketType,
  TransportClient,
} from '../src/index.js';
import { FakeTransport, flush, until } from './fake-transport.js';

const text = (s: string) => new TextEncoder().encode(s);
const decode = (b: Uint8Array) => new TextDecoder().decode(b);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('P0-1: an old connection cannot answer the new one', () => {
  it("a late same-id Response from the OLD session cannot complete the NEW session's request", async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const oldSession = t.sessions[0]!;

    // Request 1 on the old connection never gets answered; the connection
    // turns over.
    const first = c.request(text('old ask'), { timeoutMs: 0 });
    void first.catch(() => {});
    await flush();
    t.fireClose();
    await c.connect();

    // Request on the NEW connection also draws id 1 (fresh per-session
    // counter — intentional, mirroring Rust).
    const second = c.request(text('new ask'), { timeoutMs: 0 });
    await flush();
    expect(t.sessions[1]!.sent[0]!.messageId).toBe(1);

    // The OLD connection's late response for id 1 arrives. Pre-SPI-fix the
    // shared Transport attributed it to the current session and the NEW
    // request resolved with OLD_RESPONSE.
    oldSession.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: 1,
        payload: text('OLD_RESPONSE'),
      }),
    );
    await sleep(30);

    // The new request is still pending; only ITS session can answer it.
    t.sessions[1]!.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: 1,
        payload: text('NEW_RESPONSE'),
      }),
    );
    expect(decode(await second)).toBe('NEW_RESPONSE');
  });
});

describe('P0-2: an async enqueue cannot cross a reconnect', () => {
  it('a session whose enqueue awaits internally still writes to ITS OWN (closed) link', async () => {
    const t = new FakeTransport();
    t.sendDelayMs = 60; // the custom-transport async-write scenario
    const c = new TransportClient({ transport: t });
    await c.connect();

    // The send enters the old session's slow enqueue…
    const sending = c.send(text('non-idempotent'));
    const observed = sending.then(
      () => 'sent',
      () => 'refused',
    );
    // …the connection turns over while it is in flight.
    t.fireClose();
    await c.connect();

    // Pre-SPI-fix the shared Transport wrote into the CURRENT socket and
    // reported ok. Now the enqueue belongs to the old session, which is
    // closed: refused, and the new session's wire stays clean.
    expect(await observed).toBe('refused');
    await sleep(80);
    expect(t.sessions[1]!.sent).toHaveLength(0);
  });
});

describe('P1-4: heartbeat does not occupy the request consumer', () => {
  it('an app request handler and a heartbeat coexist, in either order', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();

    // App registers the single request consumer FIRST…
    const off = c.on('request', () => {});
    // …and the heartbeat still starts (activity via observeActivity, not a
    // second request subscription — which used to throw here).
    const hb = new HeartbeatMonitor(c, {
      intervalMs: 60_000,
      probe: async () => {},
    });
    expect(() => hb.start()).not.toThrow();
    hb.stop();
    off();

    // Reverse order too.
    const hb2 = new HeartbeatMonitor(c, {
      intervalMs: 60_000,
      probe: async () => {},
    });
    hb2.start();
    expect(() => c.on('request', () => {})).not.toThrow();
    hb2.stop();
  });

  it('activity via the tap actually suppresses probing', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    let probes = 0;
    const hb = new HeartbeatMonitor(c, {
      intervalMs: 120,
      timeoutMs: 80,
      probe: async () => {
        probes += 1;
      },
    });
    hb.start();
    const busy = setInterval(() => void c.send(text('x')), 40);
    await sleep(350);
    clearInterval(busy);
    hb.stop();
    expect(probes).toBe(0);
  });
});

describe('shutdown() is terminal; disconnect() is not', () => {
  it('disconnect allows reconnection; shutdown refuses it', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    await c.disconnect();
    await c.connect(); // fine
    expect(c.isConnected()).toBe(true);

    await c.shutdown();
    expect(c.isConnected()).toBe(false);
    await expect(c.connect()).rejects.toThrow(/shut down/);
  });
});

// keep the until import honest
void until;
