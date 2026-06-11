// Transport robustness: outbound backpressure guard + orphan-Response
// rejection. Both close real-world gaps for long-lived chat connections:
//   - a slow link must fail sends loudly instead of buffering unbounded
//   - a Response no one is waiting for must not masquerade as a push
import { describe, expect, it } from 'vitest';
import {
  BackpressureError,
  DEFAULT_MAX_BUFFERED_BYTES,
  Packet,
  PacketType,
  TransportClient,
  TransportError,
  TransportState,
  WebSocketTransport,
  type WebSocketLike,
} from '../src/index.js';
import { FakeTransport } from './fake-transport.js';

class MockWebSocket implements WebSocketLike {
  binaryType = 'blob';
  readyState = 0;
  bufferedAmount?: number;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  readonly sent: Uint8Array[] = [];

  send(data: ArrayBufferLike | ArrayBufferView | string): void {
    if (typeof data === 'string') throw new Error('unexpected string send');
    if (data instanceof ArrayBuffer) {
      this.sent.push(new Uint8Array(data.slice(0)));
      return;
    }
    const view = data as ArrayBufferView;
    const copy = new Uint8Array(view.byteLength);
    copy.set(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
    this.sent.push(copy);
  }
  close(): void {
    this.readyState = 3;
  }
  fireOpen(): void {
    this.readyState = 1;
    this.onopen?.({});
  }
}

async function openTransport(
  mock: MockWebSocket,
  maxBufferedBytes?: number,
): Promise<WebSocketTransport> {
  const t = new WebSocketTransport({
    url: 'ws://test.invalid',
    webSocketFactory: () => mock,
    ...(maxBufferedBytes !== undefined ? { maxBufferedBytes } : {}),
  });
  const p = t.connect();
  mock.fireOpen();
  await p;
  return t;
}

const packet = () =>
  new Packet({
    packetType: PacketType.OneWay,
    messageId: 1,
    payload: new Uint8Array([1, 2, 3]),
  });

describe('backpressure guard', () => {
  it('rejects send with BackpressureError above the high-water mark, without closing', async () => {
    const m = new MockWebSocket();
    const t = await openTransport(m, 1024);
    m.bufferedAmount = 4096; // over the 1 KiB mark

    await expect(t.send(packet())).rejects.toBeInstanceOf(BackpressureError);
    const err = await t.send(packet()).catch((e: unknown) => e);
    expect(err).toMatchObject({ bufferedAmount: 4096, maxBufferedBytes: 1024 });
    expect(m.sent).toHaveLength(0); // nothing hit the socket
    expect(t.state).toBe(TransportState.Open); // connection stays up
  });

  it('sends normally below the mark and when the buffer drains', async () => {
    const m = new MockWebSocket();
    const t = await openTransport(m, 1024);

    m.bufferedAmount = 1000;
    await t.send(packet());
    expect(m.sent).toHaveLength(1);

    m.bufferedAmount = 5000;
    await expect(t.send(packet())).rejects.toBeInstanceOf(BackpressureError);

    m.bufferedAmount = 0; // drained → sends flow again
    await t.send(packet());
    expect(m.sent).toHaveLength(2);
  });

  it('sockets without bufferedAmount are never rejected (minimal fakes keep working)', async () => {
    const m = new MockWebSocket();
    m.bufferedAmount = undefined;
    const t = await openTransport(m, 1024);
    await t.send(packet());
    expect(m.sent).toHaveLength(1);
  });

  it('maxBufferedBytes: 0 disables the check; default is 16 MiB', async () => {
    const m = new MockWebSocket();
    m.bufferedAmount = Number.MAX_SAFE_INTEGER;
    const t = await openTransport(m, 0);
    await t.send(packet()); // disabled → goes through
    expect(m.sent).toHaveLength(1);
    expect(DEFAULT_MAX_BUFFERED_BYTES).toBe(16 * 1024 * 1024);
  });
});

describe('orphan Response handling', () => {
  it('drops a Response with no pending request and reports via the error channel', async () => {
    const fake = new FakeTransport();
    const client = new TransportClient({ transport: fake });
    await client.connect();

    const messages: unknown[] = [];
    const errors: unknown[] = [];
    client.on('message', (ctx) => messages.push(ctx));
    client.on('error', (e) => errors.push(e));

    fake.fireMessage(
      new Packet({
        packetType: PacketType.Response,
        messageId: 9999, // nobody is waiting for this
        bizType: 17,
        payload: new Uint8Array([1]),
      }),
    );

    expect(messages).toHaveLength(0); // NOT surfaced as a push
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(TransportError);
    expect(String((errors[0] as Error).message)).toContain('orphan Response');
  });

  it('OneWay packets still flow to message listeners untouched', async () => {
    const fake = new FakeTransport();
    const client = new TransportClient({ transport: fake });
    await client.connect();

    const messages: unknown[] = [];
    client.on('message', (ctx) => messages.push(ctx));

    fake.fireMessage(
      new Packet({
        packetType: PacketType.OneWay,
        messageId: 0,
        bizType: 3,
        payload: new Uint8Array([1]),
      }),
    );
    expect(messages).toHaveLength(1);
  });
});
