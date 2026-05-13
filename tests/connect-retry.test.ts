// TransportClient.connect() retry-with-backoff.
//
// Language mapping of Rust `msgtrans::transport::client::RetryConfig`.
// Default = no retry (single attempt) for backwards compatibility.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { TransportClient, TransportError } from '../src/index.js';
import { FakeTransport } from './fake-transport.js';

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

const tick = async (ms: number) => {
  await vi.advanceTimersByTimeAsync(ms);
};

describe('TransportClient — connectRetry', () => {
  it('default = single attempt, no retry on failure', async () => {
    const t = new FakeTransport();
    t.connectShouldFail = new TransportError('boom');
    const c = new TransportClient({ transport: t });

    await expect(c.connect()).rejects.toBeInstanceOf(TransportError);
    expect(t.connectCalls).toHaveLength(1);
  });

  it('maxRetries = 3, all fail → 4 attempts then rethrows last error', async () => {
    const t = new FakeTransport();
    t.connectShouldFail = new TransportError('always fails');
    const c = new TransportClient({
      transport: t,
      connectRetry: {
        maxRetries: 3,
        initialDelayMs: 10,
        maxDelayMs: 100,
        multiplier: 2,
      },
    });

    const p = c.connect();
    // Attach the rejection assertion BEFORE advancing fake timers, so
    // the eventual rejection has a consumer attached — otherwise the
    // promise leaks as an "unhandled rejection" in the time between
    // `tick()` resolving and `expect(...).rejects` attaching.
    const expectation = expect(p).rejects.toThrow('always fails');
    // 4 attempts × 3 backoff windows = (after attempt 1) 10ms, 20ms, 40ms.
    // Drive the timer past the total = 10 + 20 + 40 + slack.
    await tick(100);
    await expectation;
    expect(t.connectCalls).toHaveLength(4);
    // close() is called between retries to reset state — once per retry,
    // so 3 close calls for 3 retries.
    expect(t.closeCalls).toHaveLength(3);
  });

  it('maxRetries = 3, second attempt succeeds → 2 attempts only', async () => {
    const t = new FakeTransport();
    let attempts = 0;
    const origConnect = t.connect.bind(t);
    t.connect = async () => {
      attempts++;
      if (attempts === 1) throw new TransportError('first fails');
      await origConnect();
    };
    const c = new TransportClient({
      transport: t,
      connectRetry: { maxRetries: 3, initialDelayMs: 10, maxDelayMs: 100 },
    });

    const p = c.connect();
    await tick(50);
    await expect(p).resolves.toBeUndefined();
    expect(attempts).toBe(2);
  });

  it('backoff respects multiplier and caps at maxDelayMs', async () => {
    const t = new FakeTransport();
    t.connectShouldFail = new TransportError('always fails');
    const c = new TransportClient({
      transport: t,
      connectRetry: {
        maxRetries: 4,
        initialDelayMs: 100,
        maxDelayMs: 300, // hits cap at attempt 3 (400 would be next)
        multiplier: 2,
      },
    });

    const p = c.connect();
    const expectation = expect(p).rejects.toThrow('always fails');
    // Expected backoffs between attempts: 100, 200, 300 (capped), 300 (capped).
    // Verify by advancing in stages and inspecting the call count.
    await tick(0);
    // After attempt 0 (immediate), connectCalls = 1.
    expect(t.connectCalls).toHaveLength(1);

    await tick(100); // first backoff
    expect(t.connectCalls).toHaveLength(2);

    await tick(200); // second backoff
    expect(t.connectCalls).toHaveLength(3);

    await tick(300); // third backoff (capped)
    expect(t.connectCalls).toHaveLength(4);

    await tick(300); // fourth backoff (capped)
    expect(t.connectCalls).toHaveLength(5);

    await expectation;
  });

  it('maxRetries = 0 explicit → same as default (1 attempt)', async () => {
    const t = new FakeTransport();
    t.connectShouldFail = new TransportError('boom');
    const c = new TransportClient({
      transport: t,
      connectRetry: { maxRetries: 0 },
    });

    await expect(c.connect()).rejects.toBeInstanceOf(TransportError);
    expect(t.connectCalls).toHaveLength(1);
    expect(t.closeCalls).toHaveLength(0);
  });
});
