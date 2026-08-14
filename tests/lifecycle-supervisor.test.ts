/**
 * Lifecycle SUPERVISOR regressions (ninth review): connect / disconnect /
 * shutdown are serialized, never interleaved. Every test here is
 * deterministic — sequencing is controlled by ManualSession's explicit close
 * release, never by racing timers.
 */
import { describe, expect, it } from 'vitest';
import {
  Packet,
  TransportClient,
  TransportError,
  TransportState,
  type ConnectOptions,
  type EnqueuedInfo,
  type TransportConnector,
  type TransportObserver,
  type TransportSession,
} from '../src/index.js';
import {
  WebTransportConnector,
  type WebTransportLike,
} from '../src/experimental.js';
import { flush } from './fake-transport.js';

type CloseMode = 'resolve' | 'reject' | 'pending';

/** A session whose close() the TEST controls. */
class ManualSession implements TransportSession {
  private readonly observers: TransportObserver[] = [];
  private open = true;
  private readonly closeReleases: Array<() => void> = [];
  closeCalls = 0;

  constructor(
    readonly closeMode: CloseMode,
    private readonly syncReplayCloseOnSubscribe: boolean,
  ) {}

  isOpen(): boolean {
    return this.open;
  }

  async enqueue(packet: Packet): Promise<EnqueuedInfo> {
    if (!this.open) throw new TransportError('session is closed');
    return { messageId: packet.messageId };
  }

  subscribe(observer: TransportObserver): () => void {
    this.observers.push(observer);
    if (this.syncReplayCloseOnSubscribe && this.open) {
      // The hostile case: the close is replayed SYNCHRONOUSLY inside
      // subscribe(), before the caller's next line runs.
      this.open = false;
      observer.onClose?.({ reason: 'sync replay' });
    }
    return () => {
      const i = this.observers.indexOf(observer);
      if (i >= 0) this.observers.splice(i, 1);
    };
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    if (this.closeMode === 'reject') {
      this.open = false;
      return Promise.reject(new Error('close exploded'));
    }
    if (this.closeMode === 'resolve') {
      this.open = false;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.closeReleases.push(() => {
        this.open = false;
        resolve();
      });
    });
  }

  /** Complete every close() this session has pending. */
  releaseClose(): void {
    for (const release of this.closeReleases.splice(0)) release();
  }
}

class ManualConnector implements TransportConnector {
  readonly sessions: ManualSession[] = [];
  nextCloseMode: CloseMode = 'resolve';
  nextSyncReplay = false;
  connectError: Error | null = null;

  async connect(_options: ConnectOptions): Promise<TransportSession> {
    if (this.connectError) throw this.connectError;
    const session = new ManualSession(this.nextCloseMode, this.nextSyncReplay);
    this.sessions.push(session);
    return session;
  }
}

describe('lifecycle supervisor', () => {
  it('connect() during a pending disconnect waits for the close; the finished close cannot stomp the new session', async () => {
    const connector = new ManualConnector();
    const client = new TransportClient({ connector });
    connector.nextCloseMode = 'pending';
    await client.connect();
    const first = connector.sessions[0]!;

    const disc = client.disconnect();
    connector.nextCloseMode = 'resolve';
    const reconnect = client.connect();
    await flush();
    // The dial is queued BEHIND the close: no second session yet.
    expect(connector.sessions).toHaveLength(1);
    expect(client.state).toBe(TransportState.Closing);

    first.releaseClose();
    await disc;
    await reconnect;
    // The old close converged BEFORE the dial; it cannot write Idle over the
    // new Open state.
    expect(client.state).toBe(TransportState.Open);
    expect(client.isConnected()).toBe(true);
    expect(connector.sessions).toHaveLength(2);
    expect(connector.sessions[1]!.isOpen()).toBe(true);
  });

  it('shutdown() during a pending disconnect resolves only after the physical session is down', async () => {
    const connector = new ManualConnector();
    const client = new TransportClient({ connector });
    connector.nextCloseMode = 'pending';
    await client.connect();
    const first = connector.sessions[0]!;

    const disc = client.disconnect();
    let shutdownDone = false;
    const sd = client.shutdown().then(() => {
      shutdownDone = true;
    });
    await flush();
    // The physical close has not completed: shutdown must NOT have resolved.
    expect(shutdownDone).toBe(false);
    expect(first.isOpen()).toBe(true);

    first.releaseClose();
    await sd;
    await disc;
    expect(first.isOpen()).toBe(false);
    expect(client.state).toBe(TransportState.Stopped);
    await expect(client.connect()).rejects.toThrow('shut down');
  });

  it('a session that synchronously replays close during subscribe fails the connect instead of installing a dead session', async () => {
    const connector = new ManualConnector();
    connector.nextSyncReplay = true;
    const client = new TransportClient({ connector });
    const closeEvents: unknown[] = [];
    client.on('close', (ev) => closeEvents.push(ev));

    await expect(client.connect()).rejects.toThrow(
      'closed during establishment',
    );
    expect(client.state).toBe(TransportState.Idle);
    expect(client.isConnected()).toBe(false);
    // A session that never became Open emits no close event; the connect()
    // rejection is the report.
    expect(closeEvents).toHaveLength(0);

    // The client recovers: a healthy dial afterwards works.
    connector.nextSyncReplay = false;
    await client.connect();
    expect(client.isConnected()).toBe(true);
  });

  it('a rejecting session.close() cannot strand the client in Closing; close still notifies exactly once', async () => {
    const connector = new ManualConnector();
    connector.nextCloseMode = 'reject';
    const client = new TransportClient({ connector });
    await client.connect();
    const closeEvents: unknown[] = [];
    const errors: unknown[] = [];
    client.on('close', (ev) => closeEvents.push(ev));
    client.on('error', (e) => errors.push(e));

    await client.disconnect();
    expect(client.state).toBe(TransportState.Idle);
    expect(closeEvents).toHaveLength(1);
    expect(closeEvents[0]).toMatchObject({ intentional: true });
    expect(errors).toHaveLength(1);

    // Still reconnectable after the misbehaving close.
    connector.nextCloseMode = 'resolve';
    await client.connect();
    expect(client.isConnected()).toBe(true);
  });

  it('disconnect() interrupts the retry backoff instead of sleeping it out', async () => {
    const connector = new ManualConnector();
    connector.connectError = new Error('dial refused');
    const client = new TransportClient({
      connector,
      // An HOUR of backoff: if the sleep were not abortable this test would
      // hit the suite timeout, not pass slowly.
      connectRetry: { maxRetries: 3, initialDelayMs: 3_600_000 },
    });
    const attempt = client.connect();
    attempt.catch(() => {});
    await flush(); // first dial failed; the client is now in backoff

    await client.disconnect();
    await expect(attempt).rejects.toThrow();
    expect(client.state).toBe(TransportState.Idle);
  });

  it('the deprecated `transport` option still resolves a custom connector', async () => {
    const connector = new ManualConnector();
    const client = new TransportClient({ transport: connector });
    await client.connect();
    expect(client.isConnected()).toBe(true);
    expect(connector.sessions).toHaveLength(1);
  });

  it('WebTransport abort wins even when the platform never settles ready or createBidirectionalStream', async () => {
    const neverReady: WebTransportLike = {
      ready: new Promise<void>(() => {}),
      closed: new Promise(() => {}),
      close() {},
      createBidirectionalStream: () => Promise.reject(new Error('unreachable')),
    };
    const c1 = new WebTransportConnector({
      url: 'https://example.invalid',
      webTransportFactory: () => neverReady,
    });
    const a1 = new AbortController();
    const p1 = c1.connect({ signal: a1.signal });
    a1.abort();
    await expect(p1).rejects.toThrow('connect aborted');

    const neverStream: WebTransportLike = {
      ready: Promise.resolve(),
      closed: new Promise(() => {}),
      close() {},
      createBidirectionalStream: () =>
        new Promise<never>(() => {}) as never,
    };
    const c2 = new WebTransportConnector({
      url: 'https://example.invalid',
      webTransportFactory: () => neverStream,
    });
    const a2 = new AbortController();
    const p2 = c2.connect({ signal: a2.signal });
    await flush(); // let ready resolve so the dial reaches the stream stage
    a2.abort();
    await expect(p2).rejects.toThrow('connect aborted');
  });
});
