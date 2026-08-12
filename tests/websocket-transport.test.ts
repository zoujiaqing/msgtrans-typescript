import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  Packet,
  PacketType,
  TransportError,
  TransportState,
  WebSocketTransport,
  type WebSocketLike,
} from '../src/index.js';

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

function makeTransport(
  mock: MockWebSocket,
  opts: Partial<
    Omit<
      ConstructorParameters<typeof WebSocketTransport>[0],
      'webSocketFactory' | 'url'
    >
  > = {},
): WebSocketTransport {
  return new WebSocketTransport({
    url: 'ws://test.invalid',
    webSocketFactory: () => mock,
    ...opts,
  });
}

async function connect(t: WebSocketTransport, m: MockWebSocket): Promise<void> {
  const p = t.connect();
  m.fireOpen();
  await p;
}

describe('WebSocketTransport — connection lifecycle', () => {
  it('starts in idle state', () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    expect(t.state).toBe(TransportState.Idle);
  });

  it('transitions idle → connecting → open and sets binaryType', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);

    const p = t.connect();
    expect(t.state).toBe(TransportState.Connecting);
    expect(m.binaryType).toBe('arraybuffer');

    m.fireOpen();
    await p;
    expect(t.state).toBe(TransportState.Open);
  });

  it('rejects connect if WS closes before open', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    const p = t.connect();
    m.fireClose(1006, 'abnormal');
    await expect(p).rejects.toBeInstanceOf(TransportError);
    expect(t.state).toBe(TransportState.Closed);
  });

  it('returns the same promise for concurrent connect calls', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    const p1 = t.connect();
    const p2 = t.connect();
    expect(p1).toBe(p2);
    m.fireOpen();
    await p1;
  });

  it('close() transitions state and calls ws.close()', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const closes: unknown[] = [];
    t.onClose((ev) => closes.push(ev));

    await t.close();
    // close() resolves immediately; final state transition happens via onclose.
    expect(m.closeCalls).toHaveLength(1);

    // Drain microtask so the queued onclose fires.
    await Promise.resolve();
    expect(t.state).toBe(TransportState.Closed);
    expect(closes).toHaveLength(1);
  });

  it('throws TransportError when no global WebSocket and no factory', async () => {
    const t = new WebSocketTransport({ url: 'ws://test.invalid' });
    await expect(t.connect()).rejects.toBeInstanceOf(TransportError);
  });
});

describe('WebSocketTransport — send', () => {
  it('refuses to send before connect', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await expect(t.send(Packet.oneWay(1))).rejects.toBeInstanceOf(
      TransportError,
    );
  });

  it('writes Packet.encode() bytes verbatim', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const packet = new Packet({
      packetType: PacketType.OneWay,
      messageId: 42,
      bizType: 5,
      payload: new TextEncoder().encode('hello world'),
    });

    await t.send(packet);
    expect(m.sent).toHaveLength(1);
    expect(Array.from(m.sent[0]!)).toEqual(Array.from(packet.encode()));
  });

  it('emits identical bytes as Rust fixture 02_oneway_text', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const fixture = readFixture('02_oneway_text.bin');
    const packet = Packet.decodeExact(fixture);
    await t.send(packet);

    expect(Array.from(m.sent[0]!)).toEqual(Array.from(fixture));
  });
});

describe('WebSocketTransport — receive', () => {
  it('decodes ArrayBuffer message frames into Packet via onMessage', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const received: Packet[] = [];
    t.onMessage((p) => received.push(p));

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
    const t = makeTransport(m);
    await connect(t, m);

    const received: Packet[] = [];
    t.onMessage((p) => received.push(p));

    const fixture = readFixture('04_response_matching.bin');
    m.fireBinaryAsView(fixture);

    expect(received).toHaveLength(1);
    expect(received[0]!.packetType).toBe(PacketType.Response);
    expect(received[0]!.messageId).toBe(100);
  });

  it('routes malformed binary to onError', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const errors: unknown[] = [];
    const messages: Packet[] = [];
    t.onError((e) => errors.push(e));
    t.onMessage((p) => messages.push(p));

    m.fireBinary(new Uint8Array([0x02, 0x00, 0x00, 0x00])); // version=2 → unsupported

    expect(messages).toHaveLength(0);
    expect(errors).toHaveLength(1);
  });
});

describe('WebSocketTransport — text frame policy', () => {
  it('routes text to onError by default', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const errors: unknown[] = [];
    t.onError((e) => errors.push(e));
    m.fireText('hello');

    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(TransportError);
  });

  it('silently drops text when onTextFrame=ignore', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m, { onTextFrame: 'ignore' });
    await connect(t, m);

    const errors: unknown[] = [];
    const messages: Packet[] = [];
    t.onError((e) => errors.push(e));
    t.onMessage((p) => messages.push(p));
    m.fireText('hello');

    expect(errors).toHaveLength(0);
    expect(messages).toHaveLength(0);
  });
});

describe('WebSocketTransport — close + error events', () => {
  it('triggers onClose with the underlying close event', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const closes: unknown[] = [];
    t.onClose((ev) => closes.push(ev));
    m.fireClose(1001, 'going away');

    expect(closes).toHaveLength(1);
    expect(t.state).toBe(TransportState.Closed);
    expect(closes[0]).toMatchObject({ code: 1001, reason: 'going away' });
  });

  it('triggers onError on socket error', async () => {
    const m = new MockWebSocket();
    const t = makeTransport(m);
    await connect(t, m);

    const errors: unknown[] = [];
    t.onError((e) => errors.push(e));
    m.fireError({ message: 'boom' });

    expect(errors).toHaveLength(1);
  });
});
