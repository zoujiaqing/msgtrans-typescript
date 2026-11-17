import { describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  BackpressureError,
  Packet,
  PacketType,
  TransportClient,
  TransportError,
  TransportState,
  WebSocketConnector,
  type WebSocketLike,
} from '../src/index.js';
import {
  DEFAULT_MAX_EXT_HEADER_LEN,
  DEFAULT_MAX_PAYLOAD_LEN,
  FIXED_HEADER_LEN,
} from '../src/constants.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURES_DIR = join(__dirname, '..', 'fixtures');

function readFixture(name: string): Uint8Array {
  return new Uint8Array(readFileSync(join(FIXTURES_DIR, name)));
}

class MockWebSocket implements WebSocketLike {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  binaryType = 'blob';
  bufferedAmount = 0;
  readyState = MockWebSocket.CONNECTING;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;

  readonly sent: Uint8Array[] = [];
  readonly closeCalls: Array<{ code?: number; reason?: string }> = [];

  send(data: ArrayBufferLike | ArrayBufferView | string): void {
    if (typeof data === 'string') {
      throw new Error('mock should not receive string sends from msgtrans');
    }
    if (data instanceof ArrayBuffer) {
      this.sent.push(new Uint8Array(data.slice(0)));
      return;
    }
    if (ArrayBuffer.isView(data)) {
      const view = data as ArrayBufferView;
      const copy = new Uint8Array(view.byteLength);
      copy.set(
        new Uint8Array(view.buffer, view.byteOffset, view.byteLength),
      );
      this.sent.push(copy);
      return;
    }
    throw new Error(`mock send unexpected type`);
  }

  close(code?: number, reason?: string): void {
    this.closeCalls.push({ code, reason });
    this.readyState = MockWebSocket.CLOSED;
    queueMicrotask(() => this.onclose?.({ code, reason, wasClean: true }));
  }

  // Test helpers
  fireOpen(): void {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.({});
  }
  fireBinary(bytes: Uint8Array): void {
    const ab = bytes.buffer.slice(
      bytes.byteOffset,
      bytes.byteOffset + bytes.byteLength,
    );
    this.onmessage?.({ data: ab });
  }
  fireBinaryAsView(bytes: Uint8Array): void {
    this.onmessage?.({ data: bytes });
  }
  fireText(s: string): void {
    this.onmessage?.({ data: s });
  }
  fireClose(code = 1000, reason = ''): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.({ code, reason, wasClean: code === 1000 });
  }
  fireError(err: unknown): void {
    this.onerror?.(err);
  }
}

function makeConnector(
  mock: MockWebSocket,
  opts: Partial<
    Omit<
      ConstructorParameters<typeof WebSocketConnector>[0],
      'webSocketFactory' | 'url'
    >
  > = {},
): WebSocketConnector {
  return new WebSocketConnector({
    url: 'ws://test.invalid',
    webSocketFactory: () => mock,
    ...opts,
  });
}

const freshSignal = () => new AbortController().signal;

async function openLink(t: WebSocketConnector, m: MockWebSocket) {
  const p = t.connect({ signal: freshSignal() });
  m.fireOpen();
  return await p;
}

it('allows one complete Rust-sized frame when the WebSocket buffer is empty', async () => {
  const mock = new MockWebSocket();
  const link = await openLink(makeConnector(mock), mock);
  const packet = new Packet({
    packetType: PacketType.OneWay,
    messageId: 1,
    extHeader: new Uint8Array(DEFAULT_MAX_EXT_HEADER_LEN),
    payload: new Uint8Array(DEFAULT_MAX_PAYLOAD_LEN),
  });
  await link.enqueue(packet);
  expect(mock.sent[0]?.length).toBe(
    FIXED_HEADER_LEN + DEFAULT_MAX_EXT_HEADER_LEN + DEFAULT_MAX_PAYLOAD_LEN,
  );
  mock.bufferedAmount = 1;
  await expect(link.enqueue(packet)).rejects.toBeInstanceOf(BackpressureError);
});

describe('WebSocketConnector — connection lifecycle', () => {
  it('a successful dial yields an OPEN session with arraybuffer binaryType', async () => {
    const m = new MockWebSocket();
    const t = makeConnector(m);
    const p = t.connect({ signal: freshSignal() });
    expect(m.binaryType).toBe('arraybuffer');
    m.fireOpen();
    const link = await p;
    expect(link.isOpen()).toBe(true);
  });

  it('rejects the dial if WS closes before open', async () => {
    const m = new MockWebSocket();
    const t = makeConnector(m);
    const p = t.connect({ signal: freshSignal() });
    m.fireClose(1006, 'abnormal');
    await expect(p).rejects.toBeInstanceOf(TransportError);
  });

  it('each dial is independent: two connects yield two sessions', async () => {
    const mocks: MockWebSocket[] = [];
    const t = new WebSocketConnector({
      url: 'ws://test.invalid',
      webSocketFactory: () => {
        const m = new MockWebSocket();
        mocks.push(m);
        return m;
      },
    });
    const p1 = t.connect({ signal: freshSignal() });
    const p2 = t.connect({ signal: freshSignal() });
    mocks[0]!.fireOpen();
    mocks[1]!.fireOpen();
    const [l1, l2] = await Promise.all([p1, p2]);
    expect(l1).not.toBe(l2);
    await l1.close();
    expect(l2.isOpen()).toBe(true); // no shared state to collapse
  });

  it('an aborted dial closes the socket it was establishing', async () => {
    const m = new MockWebSocket();
    const t = makeConnector(m);
    const abort = new AbortController();
    const p = t.connect({ signal: abort.signal });
    abort.abort();
    await expect(p).rejects.toThrow(/aborted/);
    expect(m.closeCalls.length).toBeGreaterThan(0); // released, not leaked
  });

  it('session.close() closes the socket and reports close exactly once', async () => {
    const m = new MockWebSocket();
    const t = makeConnector(m);
    const link = await openLink(t, m);
    const closes: unknown[] = [];
    link.subscribe({ onClose: (ev) => closes.push(ev) });

    await link.close();
    expect(m.closeCalls).toHaveLength(1);
    expect(link.isOpen()).toBe(false);
    expect(closes).toHaveLength(1);

    // The platform's own (queued) onclose fires later — must be a no-op.
    await Promise.resolve();
    expect(closes).toHaveLength(1);
  });

  it('throws TransportError when no global WebSocket and no factory', async () => {
    // Node >= 22 SHIPS globalThis.WebSocket — without the stub this test
    // would really dial test.invalid and depend on DNS timing. Stub it out
    // to test the actual missing-platform path deterministically.
    vi.stubGlobal('WebSocket', undefined);
    try {
      const t = new WebSocketConnector({ url: 'ws://test.invalid' });
      await expect(t.connect({ signal: freshSignal() })).rejects.toThrow(
        'no global WebSocket available',
      );
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('WebSocketConnector — send', () => {
  it('refuses to enqueue on a CLOSED session (there is no pre-connect send: no session exists yet)', async () => {
    const m = new MockWebSocket();
    const t = makeConnector(m);
    const link = await openLink(t, m);
    await link.close();
    await expect(link.enqueue(Packet.oneWay(1))).rejects.toBeInstanceOf(
      TransportError,
    );
  });

  it('writes Packet.encode() bytes verbatim', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const packet = new Packet({
      packetType: PacketType.OneWay,
      messageId: 42,
      bizType: 5,
      payload: new TextEncoder().encode('hello world'),
    });

    await link.enqueue(packet);
    expect(m.sent).toHaveLength(1);
    expect(Array.from(m.sent[0]!)).toEqual(Array.from(packet.encode()));
  });

  it('emits identical bytes as Rust fixture 02_oneway_text', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const fixture = readFixture('02_oneway_text.bin');
    const packet = Packet.decodeExact(fixture);
    await link.enqueue(packet);

    expect(Array.from(m.sent[0]!)).toEqual(Array.from(fixture));
  });
});

describe('WebSocketConnector — receive', () => {
  it('decodes ArrayBuffer message frames into Packet via onMessage', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const received: Packet[] = [];
    link.subscribe({ onMessage: (p) => received.push(p) });

    const fixture = readFixture('03_request_minimal.bin');
    m.fireBinary(fixture);

    expect(received).toHaveLength(1);
    expect(received[0]!.packetType).toBe(PacketType.Request);
    expect(received[0]!.messageId).toBe(100);
    expect(received[0]!.bizType).toBe(10);
    expect(new TextDecoder().decode(received[0]!.payload)).toBe('ping');
  });

  it('also accepts ArrayBufferView (Buffer-like) frames', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const received: Packet[] = [];
    link.subscribe({ onMessage: (p) => received.push(p) });

    const fixture = readFixture('04_response_matching.bin');
    m.fireBinaryAsView(fixture);

    expect(received).toHaveLength(1);
    expect(received[0]!.packetType).toBe(PacketType.Response);
    expect(received[0]!.messageId).toBe(100);
  });

  it('routes malformed binary to onError', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const errors: unknown[] = [];
    const messages: Packet[] = [];
    link.subscribe({ onError: (e) => errors.push(e) });
    link.subscribe({ onMessage: (p) => messages.push(p) });

    m.fireBinary(new Uint8Array([0x02, 0x00, 0x00, 0x00])); // version=2 → unsupported

    expect(messages).toHaveLength(0);
    expect(errors).toHaveLength(1);
  });
});

describe('WebSocketConnector — text frame policy', () => {
  it('routes text to onError by default', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const errors: unknown[] = [];
    link.subscribe({ onError: (e) => errors.push(e) });
    m.fireText('hello');

    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(TransportError);
  });

  it('silently drops text when onTextFrame=ignore', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m, { onTextFrame: 'ignore' }), m);

    const errors: unknown[] = [];
    const messages: Packet[] = [];
    link.subscribe({ onError: (e) => errors.push(e) });
    link.subscribe({ onMessage: (p) => messages.push(p) });
    m.fireText('hello');

    expect(errors).toHaveLength(0);
    expect(messages).toHaveLength(0);
  });
});

describe('WebSocketConnector — close + error events', () => {
  it('triggers onClose with the underlying close event', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const closes: unknown[] = [];
    link.subscribe({ onClose: (ev) => closes.push(ev) });
    m.fireClose(1001, 'going away');

    expect(closes).toHaveLength(1);
    expect(link.isOpen()).toBe(false);
    expect(closes[0]).toMatchObject({ code: 1001, reason: 'going away' });
  });

  it('triggers onError on socket error', async () => {
    const m = new MockWebSocket();
    const link = await openLink(makeConnector(m), m);

    const errors: unknown[] = [];
    link.subscribe({ onError: (e) => errors.push(e) });
    m.fireError({ message: 'boom' });

    expect(errors).toHaveLength(1);
  });
});

describe('WebSocketSession — close semantics (eleventh review)', () => {
  class ThrowingCloseWebSocket extends MockWebSocket {
    closeError: Error | null = null;
    override close(code?: number, reason?: string): void {
      if (this.closeError) throw this.closeError; // stays OPEN
      super.close(code, reason);
    }
  }

  class SyncCloseWebSocket extends MockWebSocket {
    override close(code?: number, reason?: string): void {
      this.closeCalls.push({ code, reason });
      this.readyState = MockWebSocket.CLOSED;
      // SYNCHRONOUS platform onclose — fires inside session.close().
      this.onclose?.({ code: 1000, reason: 'platform sync', wasClean: true });
    }
  }

  it('a platform close() that throws is propagated AFTER local convergence', async () => {
    const mock = new ThrowingCloseWebSocket();
    const link = await openLink(makeConnector(mock), mock);
    const closes: unknown[] = [];
    link.subscribe({ onClose: (ev) => closes.push(ev) });
    mock.closeError = new Error('platform close exploded');

    await expect(link.close()).rejects.toThrow('platform close exploded');
    // Local terminal state converged and the close event fired first.
    expect(link.isOpen()).toBe(false);
    expect(closes).toEqual([
      { intentional: true, reason: 'closed locally' },
    ]);
  });

  it('shutdown() surfaces a throwing platform close over the whole stack', async () => {
    const mock = new ThrowingCloseWebSocket();
    const client = new TransportClient({ connector: makeConnector(mock) });
    const p = client.connect();
    mock.fireOpen();
    await p;
    mock.closeError = new Error('platform close exploded');

    await expect(client.shutdown()).rejects.toThrow('platform close exploded');
    expect(client.state).toBe(TransportState.Stopped);
  });

  it('a synchronous platform onclose cannot relabel an intentional close', async () => {
    const mock = new SyncCloseWebSocket();
    const link = await openLink(makeConnector(mock), mock);
    const closes: Array<{ intentional?: boolean }> = [];
    link.subscribe({
      onClose: (ev) => closes.push(ev as { intentional?: boolean }),
    });

    await link.close({ intentional: true, reason: 'wanted' });
    expect(closes).toHaveLength(1);
    expect(closes[0]!.intentional).toBe(true);
  });

  it('maps the caller close code/reason onto the wire close frame', async () => {
    const mock = new MockWebSocket();
    const link = await openLink(makeConnector(mock), mock);
    await link.close({ code: 4000, reason: 'bye' });
    expect(mock.closeCalls).toEqual([{ code: 4000, reason: 'bye' }]);

    const mock2 = new MockWebSocket();
    const link2 = await openLink(makeConnector(mock2), mock2);
    await link2.close();
    expect(mock2.closeCalls).toEqual([{ code: undefined, reason: undefined }]);
  });
});
