// Heartbeat + reconnect helpers (0.3.0). Real timers with tight intervals:
// both classes are scheduling logic, and fake timers hide exactly the
// interleavings that matter.
import { describe, expect, it } from 'vitest';
import {
  HeartbeatMonitor,
  ReconnectManager,
  type ReconnectPhase,
  TransportClient,
} from '../src/index.js';
import { FakeTransport } from './fake-transport.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('HeartbeatMonitor', () => {
  function rig(probe: () => Promise<unknown>, extra: { onDead?: (e: unknown) => void } = {}) {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    const hb = new HeartbeatMonitor(c, {
      probe,
      intervalMs: 120,
      timeoutMs: 80,
      ...extra,
    });
    return { t, c, hb };
  }

  it('probes an idle connection and stays alive on success', async () => {
    let probes = 0;
    const { c, hb } = rig(async () => {
      probes += 1;
    });
    await c.connect();
    hb.start();
    await sleep(300);
    hb.stop();
    expect(probes).toBeGreaterThanOrEqual(1);
    expect(c.isConnected()).toBe(true);
  });

  it('does NOT probe while traffic is flowing (activity suppresses)', async () => {
    let probes = 0;
    const { c, hb } = rig(async () => {
      probes += 1;
    });
    await c.connect();
    hb.start();
    // Keep the connection busy for well past the idle interval.
    const busy = setInterval(() => void c.send(new Uint8Array([1])), 40);
    await sleep(350);
    clearInterval(busy);
    hb.stop();
    expect(probes).toBe(0);
  });

  it('a failing probe declares the connection dead (default: close)', async () => {
    const { t, c, hb } = rig(async () => {
      throw new Error('peer gone');
    });
    await c.connect();
    hb.start();
    await sleep(300);
    expect(t.closeCalls.length).toBeGreaterThan(0);
    expect(hb.isRunning).toBe(false); // stopped itself after the verdict
  });

  it('a probe that hangs past timeoutMs is a failure', async () => {
    const dead: unknown[] = [];
    const { c, hb } = rig(() => new Promise(() => {}), {
      onDead: (e) => dead.push(e),
    });
    await c.connect();
    hb.start();
    await sleep(400);
    hb.stop();
    expect(dead).toHaveLength(1);
    expect(String(dead[0])).toContain('exceeded');
  });

  it('never probes a disconnected client', async () => {
    let probes = 0;
    const { hb } = rig(async () => {
      probes += 1;
    });
    hb.start(); // client never connected
    await sleep(300);
    hb.stop();
    expect(probes).toBe(0);
  });
});

describe('ReconnectManager', () => {
  it('reconnects after close with backoff and reports phases', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const phases: Array<[ReconnectPhase, number]> = [];
    const rm = new ReconnectManager(c, {
      initialDelayMs: 20,
      multiplier: 2,
      onStateChange: (p, a) => phases.push([p, a]),
    });
    rm.start();

    t.connectShouldFail = new Error('still down');
    t.fireClose();
    await sleep(60); // attempt 1 fails
    t.connectShouldFail = null;
    await sleep(120); // attempt 2 succeeds

    rm.stop();
    expect(phases[0]).toEqual(['reconnecting', 1]);
    expect(phases.some(([p]) => p === 'connected')).toBe(true);
    expect(c.isConnected()).toBe(true);
    expect(t.connectCalls.length).toBeGreaterThanOrEqual(3); // initial + 2
  });

  it('stop() before an intentional close prevents reconnection', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const rm = new ReconnectManager(c, { initialDelayMs: 10 });
    rm.start();

    rm.stop(); // the documented contract for intentional disconnects
    const before = t.connectCalls.length;
    await c.disconnect();
    t.fireClose();
    await sleep(80);
    expect(t.connectCalls.length).toBe(before);
  });

  it('stop() DURING a backoff sleep abandons the cycle (no zombie connect)', async () => {
    // The close-listener unsubscribe cannot cover this path: the cycle is
    // already running when stop() arrives, so the in-cycle active check is
    // the only thing standing between stop() and a zombie reconnect.
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const rm = new ReconnectManager(c, { initialDelayMs: 60 });
    rm.start();

    t.fireClose(); // cycle enters its backoff sleep
    await sleep(20);
    rm.stop(); // mid-sleep
    const before = t.connectCalls.length;
    await sleep(120); // past the wake-up
    expect(t.connectCalls.length).toBe(before);
  });

  it('gives up after maxAttempts and deactivates', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const phases: ReconnectPhase[] = [];
    const rm = new ReconnectManager(c, {
      initialDelayMs: 10,
      maxAttempts: 2,
      onStateChange: (p) => phases.push(p),
    });
    rm.start();

    t.connectShouldFail = new Error('permanently down');
    t.fireClose();
    await sleep(150);
    expect(phases.filter((p) => p === 'reconnecting')).toHaveLength(2);
    expect(phases.at(-1)).toBe('gave-up');
    expect(rm.isActive).toBe(false);
  });

  it('a close arriving mid-cycle is absorbed (one cycle at a time)', async () => {
    const t = new FakeTransport();
    const c = new TransportClient({ transport: t });
    await c.connect();
    const phases: ReconnectPhase[] = [];
    const rm = new ReconnectManager(c, {
      initialDelayMs: 40,
      onStateChange: (p) => phases.push(p),
    });
    rm.start();

    t.fireClose();
    t.fireClose(); // second close while the first cycle is backing off
    await sleep(100);
    rm.stop();
    // exactly one reconnecting→connected pair, not two interleaved cycles
    expect(phases).toEqual(['reconnecting', 'connected']);
  });
});
