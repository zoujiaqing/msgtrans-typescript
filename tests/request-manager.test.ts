import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import {
  IdExhaustedError,
  Packet,
  PacketType,
  RequestTimeoutError,
  TransportError,
} from '../src/index.js';
// INTERNAL import: RequestManager is deliberately absent from the public
// surface (ids belong to the transport); tests reach in directly.
import { RequestManager } from '../src/request-manager.js';
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
    // Attach the expectation BEFORE advancing so the rejection is handled
    // the moment it fires. The response timer is armed only after the
    // enqueue resolves — the timeout measures the peer, not this side.
    const expectation = expect(promise).rejects.toBeInstanceOf(
      RequestTimeoutError,
    );
    await vi.advanceTimersByTimeAsync(100);
    await expectation;
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
    const expectation = expect(promise).rejects.toBeInstanceOf(
      RequestTimeoutError,
    );
    await vi.advanceTimersByTimeAsync(50);
    await expectation;
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

  it('sendResponse() preserves the originating id — and ONLY accepts Responses', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const p = new Packet({ packetType: PacketType.Response, messageId: 42 });
    await m.sendResponse(p);
    expect(p.messageId).toBe(42);
    expect(m.pendingCount).toBe(0);

    // A pre-numbered Request is unrepresentable: its response could
    // complete a different pending waiter that drew the same id.
    const rogue = new Packet({ packetType: PacketType.Request, messageId: 7 });
    await expect(m.sendResponse(rogue)).rejects.toThrow(
      'only accepts Response',
    );
    expect(t.sent).toHaveLength(1);
  });

  it('send() overwrites any caller-set id: ids belong to the transport', async () => {
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const p = new Packet({ packetType: PacketType.OneWay, messageId: 999 });
    await m.send(p);
    expect(p.messageId).toBe(1);
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

describe('RequestManager — id exhaustion (never wraps)', () => {
  it('refuses to wrap: 0xffffffff is the last id, then IdExhaustedError', async () => {
    // Responses are matched on the bare u32; a wrapped id could complete a
    // DIFFERENT still-pending request. Exhaustion is explicit, and a
    // reconnect starts a fresh generation.
    const t = new FakeTransport();
    const m = new RequestManager(t, { initialMessageId: 0xfffffffe });

    const p1 = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    const p2 = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    await m.send(p1);
    await m.send(p2);
    expect(p1.messageId).toBe(0xfffffffe);
    expect(p2.messageId).toBe(0xffffffff);

    const p3 = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    await expect(m.send(p3)).rejects.toBeInstanceOf(IdExhaustedError);
    await expect(m.request(newRequestPacket())).rejects.toBeInstanceOf(
      IdExhaustedError,
    );

    // A fresh connection generation restarts the space.
    m.startGeneration();
    const p4 = new Packet({ packetType: PacketType.OneWay, messageId: 0 });
    await m.send(p4);
    expect(p4.messageId).toBe(0xfffffffe);
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

  it('a non-fatal transport error does NOT reject pending (only close does)', async () => {
    // Old behavior reset ALL pending on any transport error, so a single
    // recoverable error nuked every in-flight request while the connection
    // stayed healthy. Protocol violations now close the connection at the
    // transport layer; pending state rides the connection, nothing else.
    const t = new FakeTransport();
    const m = new RequestManager(t);
    const promise = m.request(newRequestPacket());
    await Promise.resolve();

    t.fireError(new Error('socket hiccup'));
    expect(m.pendingCount).toBe(1); // untouched

    t.fireClose();
    await expect(promise).rejects.toThrow('transport closed');
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
