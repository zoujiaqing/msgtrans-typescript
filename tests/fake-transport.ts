import {
  Packet,
  type EnqueuedInfo,
  type Transport,
} from '../src/index.js';

export class FakeTransport implements Transport {
  readonly sent: Packet[] = [];
  readonly connectCalls: number[] = [];
  readonly closeCalls: number[] = [];

  sendShouldFail: Error | null = null;
  connectShouldFail: Error | null = null;
  /** When set, send() resolves only after this many ms (async enqueue). */
  sendDelayMs = 0;

  private _isConnected = false;
  private readonly messageHandlers: Array<(p: Packet) => void> = [];
  private readonly messageEnqueuedHandlers: Array<(info: EnqueuedInfo) => void> =
    [];
  private readonly closeHandlers: Array<(ev?: unknown) => void> = [];
  private readonly errorHandlers: Array<(e: unknown) => void> = [];

  async connect(): Promise<void> {
    this.connectCalls.push(Date.now());
    if (this.connectShouldFail) throw this.connectShouldFail;
    this._isConnected = true;
  }

  async send(packet: Packet): Promise<void> {
    if (this.sendDelayMs > 0) {
      await new Promise((r) => setTimeout(r, this.sendDelayMs));
    }
    if (this.sendShouldFail) throw this.sendShouldFail;
    this.sent.push(packet);
    const info: EnqueuedInfo = { messageId: packet.messageId };
    for (const cb of this.messageEnqueuedHandlers) cb(info);
  }

  async close(): Promise<void> {
    this.closeCalls.push(Date.now());
    this._isConnected = false;
  }

  isConnected(): boolean {
    return this._isConnected;
  }

  onMessage(cb: (packet: Packet) => void): void {
    this.messageHandlers.push(cb);
  }
  onMessageEnqueued(cb: (info: EnqueuedInfo) => void): void {
    this.messageEnqueuedHandlers.push(cb);
  }
  onClose(cb: (event?: unknown) => void): void {
    this.closeHandlers.push(cb);
  }
  onError(cb: (error: unknown) => void): void {
    this.errorHandlers.push(cb);
  }

  fireMessage(packet: Packet): void {
    for (const cb of this.messageHandlers) cb(packet);
  }
  fireClose(ev?: unknown): void {
    this._isConnected = false;
    for (const cb of this.closeHandlers) cb(ev);
  }
  fireError(e: unknown): void {
    for (const cb of this.errorHandlers) cb(e);
  }
}

/** Drain the microtask + macrotask queue once: inbound normalization and the
 *  outbound chain are async, so delivery is never synchronous any more. */
export function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Poll until `cond` holds (native decompression takes several async hops,
 *  so a single flush is not deterministic). Throws after `timeoutMs`. */
export async function until(
  cond: () => boolean,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) {
      throw new Error('condition not reached within timeout');
    }
    await flush();
  }
}
