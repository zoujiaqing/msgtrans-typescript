import {
  Packet,
  TransportError,
  type ConnectOptions,
  type EnqueuedInfo,
  type TransportCloseEvent,
  type TransportConnector,
  type TransportObserver,
  type TransportSession,
} from '../src/index.js';

/** One scripted in-memory connection. */
export class FakeSession implements TransportSession {
  readonly sent: Packet[] = [];
  private readonly observers: TransportObserver[] = [];
  private open = true;
  sendShouldFail: Error | null = null;
  sendDelayMs = 0;
  closeCalls = 0;

  isOpen(): boolean {
    return this.open;
  }

  async enqueue(packet: Packet): Promise<EnqueuedInfo> {
    if (this.sendDelayMs > 0) {
      await new Promise((r) => setTimeout(r, this.sendDelayMs));
    }
    if (!this.open) throw new TransportError('session is closed');
    if (this.sendShouldFail) throw this.sendShouldFail;
    this.sent.push(packet);
    return { messageId: packet.messageId };
  }

  subscribe(observer: TransportObserver): () => void {
    this.observers.push(observer);
    return () => {
      const i = this.observers.indexOf(observer);
      if (i >= 0) this.observers.splice(i, 1);
    };
  }

  close(event?: Partial<TransportCloseEvent>): Promise<void> {
    this.closeCalls += 1;
    if (this.open) {
      this.open = false;
      for (const o of [...this.observers])
        o.onClose?.({ intentional: true, reason: 'closed', ...event });
    }
    return Promise.resolve();
  }

  fireMessage(packet: Packet): void {
    for (const o of [...this.observers]) o.onMessage?.(packet);
  }
  fireClose(ev?: Partial<TransportCloseEvent>): void {
    if (!this.open) return;
    this.open = false;
    for (const o of [...this.observers])
      o.onClose?.({ intentional: false, ...ev });
  }
  fireError(e: unknown): void {
    for (const o of [...this.observers]) o.onError?.(e);
  }
}

/**
 * Scripted connector with the OLD FakeTransport's helper surface so the
 * existing tests keep their shape: `sent` aggregates across sessions,
 * `fireMessage`/`fireClose`/`fireError` act on the CURRENT session,
 * `connectCalls`/`closeCalls` count across the whole run.
 */
export class FakeTransport implements TransportConnector {
  readonly sessions: FakeSession[] = [];
  readonly connectCalls: number[] = [];
  connectShouldFail: Error | null = null;
  /** Applied to each NEW session as it is created. */
  sendDelayMs = 0;
  private _sendShouldFail: Error | null = null;

  get sendShouldFail(): Error | null {
    return this._sendShouldFail;
  }
  set sendShouldFail(e: Error | null) {
    this._sendShouldFail = e;
    for (const s of this.sessions) s.sendShouldFail = e;
  }

  get current(): FakeSession | null {
    const last = this.sessions[this.sessions.length - 1];
    return last && last.isOpen() ? last : null;
  }

  /** All packets enqueued on any session, in order. */
  get sent(): Packet[] {
    return this.sessions.flatMap((s) => s.sent);
  }

  get closeCalls(): number[] {
    // Shape-compat with the old helper (an array whose length counts closes).
    return this.sessions.flatMap((s) => Array(s.closeCalls).fill(0));
  }

  async connect(_options: ConnectOptions): Promise<TransportSession> {
    this.connectCalls.push(Date.now());
    if (this.connectShouldFail) throw this.connectShouldFail;
    const session = new FakeSession();
    session.sendDelayMs = this.sendDelayMs;
    session.sendShouldFail = this._sendShouldFail;
    this.sessions.push(session);
    return session;
  }

  fireMessage(packet: Packet): void {
    this.sessions[this.sessions.length - 1]?.fireMessage(packet);
  }
  fireClose(ev?: Partial<TransportCloseEvent>): void {
    this.sessions[this.sessions.length - 1]?.fireClose(ev);
  }
  fireError(e: unknown): void {
    this.sessions[this.sessions.length - 1]?.fireError(e);
  }
}

/** Drain the microtask + macrotask queue once. */
export function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Poll until `cond` holds. Throws after `timeoutMs`. */
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
