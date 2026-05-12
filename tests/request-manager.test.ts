import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import {
  Packet,
  PacketType,
  RequestManager,
  RequestTimeoutError,
  TransportError,
} from '../src/index.js';
import { FakeTransport } from './fake-transport.js';

function newRequestPacket(): Packet {
  return new Packet({ packetType: PacketType.OneWay, messageId: 0 });
}

function makeResponse(messageId: number, body = ''): Packet {
  return new Packet({
    packetType: PacketType.Response,
    messageId,
    payload: new TextEncoder().encode(body),
  });
}

describe('RequestManager — request lifecycle', () => {
  it('sets packetType=Request and allocates messageId', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);

    const packet = new Packet({
      packetType: PacketType.OneWay,
      messageId: 999,
    });
    const promise = m.request(packet);

    expect(packet.packetType).toBe(PacketType.Request);
    expect(packet.messageId).toBe(1);
    expect(t.sent).toHaveLength(1);
    expect(t.sent[0]).toBe(packet);
    expect(m.pendingCount).toBe(1);

    expect(m.handleIncoming(makeResponse(1, 'ok'))).toBe(true);
    const resp = await promise;
    expect(new TextDecoder().decode(resp.payload)).toBe('ok');
    expect(m.pendingCount).toBe(0);
  });

  it('handles multiple concurrent requests with out-of-order responses', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);

    const p1 = m.request(newRequestPacket());
    const p2 = m.request(newRequestPacket());
    const p3 = m.request(newRequestPacket());

    expect(t.sent.map((p) => p.messageId)).toEqual([1, 2, 3]);
    expect(m.pendingCount).toBe(3);

    expect(m.handleIncoming(makeResponse(2, 'two'))).toBe(true);
    expect(m.handleIncoming(makeResponse(3, 'three'))).toBe(true);
    expect(m.handleIncoming(makeResponse(1, 'one'))).toBe(true);

    const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
    expect(new TextDecoder().decode(r1.payload)).toBe('one');
    expect(new TextDecoder().decode(r2.payload)).toBe('two');
    expect(new TextDecoder().decode(r3.payload)).toBe('three');
    expect(m.pendingCount).toBe(0);
  });
});

describe('RequestManager — handleIncoming', () => {
  it('returns false for non-Response packets', () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    expect(
      m.handleIncoming(
        new Packet({ packetType: PacketType.OneWay, messageId: 1 }),
      ),
    ).toBe(false);
    expect(
      m.handleIncoming(
        new Packet({ packetType: PacketType.Request, messageId: 1 }),
      ),
    ).toBe(false);
  });

  it('returns false for unmatched Response messageId', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const promise = m.request(newRequestPacket());

    expect(m.handleIncoming(makeResponse(9999))).toBe(false);
    expect(m.pendingCount).toBe(1);

    m.reset(new Error('cleanup'));
    await expect(promise).rejects.toThrow('cleanup');
  });
});

describe('RequestManager — timeout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('rejects with RequestTimeoutError after timeoutMs and cleans pending', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const promise = m.request(newRequestPacket(), { timeoutMs: 100 });

    expect(m.pendingCount).toBe(1);
    vi.advanceTimersByTime(100);

    await expect(promise).rejects.toBeInstanceOf(RequestTimeoutError);
    expect(m.pendingCount).toBe(0);
  });

  it('does not start a timer when timeoutMs=0', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const promise = m.request(newRequestPacket(), { timeoutMs: 0 });

    vi.advanceTimersByTime(60_000);
    expect(m.pendingCount).toBe(1);

    expect(m.handleIncoming(makeResponse(1))).toBe(true);
    await promise;
  });

  it('uses defaultTimeoutMs from constructor', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t, { defaultTimeoutMs: 50 });
    const promise = m.request(newRequestPacket());
    vi.advanceTimersByTime(50);
    await expect(promise).rejects.toBeInstanceOf(RequestTimeoutError);
  });
});

describe('RequestManager — send (OneWay / Response passthrough)', () => {
  it('allocates messageId when 0 and does not register pending', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const p = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    await m.send(p);
    expect(p.messageId).toBe(1);
    expect(t.sent[0]).toBe(p);
    expect(m.pendingCount).toBe(0);
  });

  it('preserves caller-set messageId (e.g., for Response)', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const p = new Packet({ packetType: PacketType.Response, messageId: 42 });
    await m.send(p);
    expect(p.messageId).toBe(42);
    expect(m.pendingCount).toBe(0);
  });

  it('shares the counter between request() and send()', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);

    const a = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    await m.send(a); // 1
    const b = newRequestPacket();
    void m.request(b); // 2
    const c = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    await m.send(c); // 3

    expect([a.messageId, b.messageId, c.messageId]).toEqual([1, 2, 3]);
  });
});

describe('RequestManager — counter wraparound', () => {
  it('wraps from 0xffffffff back to 1 (skipping 0)', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t, { initialMessageId: 0xfffffffe });

    const p1 = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    const p2 = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    const p3 = new Packet({ packetType: PacketType.OneWay, messageId: 0 });

    await m.send(p1);
    await m.send(p2);
    await m.send(p3);

    expect(p1.messageId).toBe(0xfffffffe);
    expect(p2.messageId).toBe(0xffffffff);
    expect(p3.messageId).toBe(1);
  });

  it('treats initialMessageId=0 as 1', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t, { initialMessageId: 0 });
    const p = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    await m.send(p);
    expect(p.messageId).toBe(1);
  });
});

describe('RequestManager — reset', () => {
  it('rejects all pending requests', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const p1 = m.request(newRequestPacket());
    const p2 = m.request(newRequestPacket());

    expect(m.pendingCount).toBe(2);
    m.reset(new Error('reconnecting'));

    await expect(p1).rejects.toThrow('reconnecting');
    await expect(p2).rejects.toThrow('reconnecting');
    expect(m.pendingCount).toBe(0);
  });

  it('rejects pending on transport.onClose', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const promise = m.request(newRequestPacket());

    t.fireClose();

    await expect(promise).rejects.toBeInstanceOf(TransportError);
    expect(m.pendingCount).toBe(0);
  });

  it('rejects pending on transport.onError', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const promise = m.request(newRequestPacket());

    t.fireError(new Error('socket failed'));

    await expect(promise).rejects.toThrow('socket failed');
    expect(m.pendingCount).toBe(0);
  });

  it('is a no-op when no pending', () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    expect(() => m.reset()).not.toThrow();
  });
});

describe('RequestManager — send error', () => {
  it('rejects request and cleans pending if transport.send rejects', async () => {
    const t = new FakeTransport();
    t.sendShouldFail = new Error('send failed');
    const m = new RequestManager(t);

    const promise = m.request(newRequestPacket());
    await expect(promise).rejects.toThrow('send failed');
    expect(m.pendingCount).toBe(0);
  });
});
