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
    private readonly subscribeError: Error | null = null,
  ) {}

  get observerCount(): number {
    return this.observers.length;
  }

  isOpen(): boolean {
    return this.open;
  }

  async enqueue(packet: Packet): Promise<EnqueuedInfo> {
    if (!this.open) throw new TransportError('session is closed');
    return { messageId: packet.messageId };
  }

  subscribe(observer: TransportObserver): () => void {
    if (this.subscribeError) throw this.subscribeError;
    this.observers.push(observer);
    if (this.syncReplayCloseOnSubscribe && this.open) {
      // The hostile case: the close is replayed SYNCHRONOUSLY inside
      // subscribe(), before the caller's next line runs.
      this.open = false;
      observer.onClose?.({ intentional: false, reason: 'sync replay' });
    }
    return () => {
      const i = this.observers.indexOf(observer);
      if (i >= 0) this.observers.splice(i, 1);
    };
  }

  close(): Promise<void> {
    this.closeCalls += 1;
    if (this.closeMode === 'reject') {
      // Deliberately STAYS open: a close() that both rejects and leaves the
      // physical connection up is the harshest misbehaving-SPI shape.
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
  nextSubscribeError: Error | null = null;
  nextDialPending = false;
  connectError: Error | null = null;
  connectCalls = 0;

  connect(options: ConnectOptions): Promise<TransportSession> {
    this.connectCalls += 1;
    if (this.connectError) return Promise.reject(this.connectError);
    if (this.nextDialPending) {
      // A dial that never completes on its own but honors cancellation.
      return new Promise((_resolve, reject) => {
        options.signal.addEventListener(
          'abort',
          () => reject(new TransportError('connect aborted')),
          { once: true },
        );
      });
    }
    const session = new ManualSession(
      this.nextCloseMode,
      this.nextSyncReplay,
      this.nextSubscribeError,
    );
    this.sessions.push(session);
    return Promise.resolve(session);
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
    // And the observer registration did not leak: retire ran before the
    // unsubscribe handle existed, so setUnsubscribe executed it immediately.
    expect(connector.sessions[0]!.observerCount).toBe(0);

    // The client recovers: a healthy dial afterwards works.
    connector.nextSyncReplay = false;
    await client.connect();
    expect(client.isConnected()).toBe(true);
  });

  it('a rejecting session.close() cannot strand the client in Closing; the failure is PROPAGATED, close still notifies exactly once', async () => {
    const connector = new ManualConnector();
    connector.nextCloseMode = 'reject';
    const client = new TransportClient({ connector });
    await client.connect();
    const closeEvents: unknown[] = [];
    const errors: unknown[] = [];
    client.on('close', (ev) => closeEvents.push(ev));
    client.on('error', (e) => errors.push(e));

    // The caller SEES the failure — never success over a possibly-open link.
    await expect(client.disconnect()).rejects.toThrow('close exploded');
    // ...but the state still converged (finally), with exactly one close event.
    expect(client.state).toBe(TransportState.Idle);
    expect(closeEvents).toHaveLength(1);
    expect(closeEvents[0]).toMatchObject({ intentional: true });
    expect(errors).toHaveLength(1);

    // Still reconnectable after the misbehaving close.
    connector.nextCloseMode = 'resolve';
    await client.connect();
    expect(client.isConnected()).toBe(true);
  });

  it('shutdown() propagates a close failure instead of reporting success over an open session', async () => {
    const connector = new ManualConnector();
    connector.nextCloseMode = 'reject';
    const client = new TransportClient({ connector });
    await client.connect();
    const first = connector.sessions[0]!;

    await expect(client.shutdown()).rejects.toThrow('close exploded');
    // State converged to Stopped, the session is truthfully still open — and
    // the caller KNOWS, because shutdown() rejected.
    expect(client.state).toBe(TransportState.Stopped);
    expect(first.isOpen()).toBe(true);
    await expect(client.connect()).rejects.toThrow('shut down');
  });

  it('a new connect() after cancelling an in-flight dial is not swallowed by the doomed flight', async () => {
    const connector = new ManualConnector();
    connector.nextDialPending = true;
    const client = new TransportClient({ connector });
    const p1 = client.connect();
    p1.catch(() => {});
    await flush(); // dial 1 is in flight

    const disc = client.disconnect(); // cancels dial 1
    connector.nextDialPending = false;
    const p2 = client.connect(); // fresh intent — must NOT be the doomed p1
    await expect(p1).rejects.toThrow('connect aborted');
    await disc;
    await p2;
    expect(client.isConnected()).toBe(true);
    expect(connector.connectCalls).toBe(2);
  });

  it('a throwing subscribe() rolls the installation back and closes the physical link', async () => {
    const connector = new ManualConnector();
    connector.nextSubscribeError = new Error('subscribe exploded');
    const client = new TransportClient({ connector });

    await expect(client.connect()).rejects.toThrow('subscribe exploded');
    expect(client.state).toBe(TransportState.Idle);
    const first = connector.sessions[0]!;
    expect(first.closeCalls).toBe(1);
    expect(first.isOpen()).toBe(false);

    connector.nextSubscribeError = null;
    await client.connect();
    expect(client.isConnected()).toBe(true);
  });

  it('a failed installation OWNS its rollback close: connect() settles only after the link is released', async () => {
    const connector = new ManualConnector();
    connector.nextSubscribeError = new Error('subscribe exploded');
    connector.nextCloseMode = 'pending';
    const client = new TransportClient({ connector });

    let settled = false;
    const attempt = client.connect();
    attempt.catch(() => {}).then(() => {
      settled = true;
    });
    await flush();
    // subscribe already threw, but the rollback close is still pending:
    // connect() must NOT have settled with the physical link still open.
    expect(settled).toBe(false);
    expect(connector.sessions[0]!.isOpen()).toBe(true);

    connector.sessions[0]!.releaseClose();
    await expect(attempt).rejects.toThrow('subscribe exploded');
    expect(settled).toBe(true);
    expect(connector.sessions[0]!.isOpen()).toBe(false);
  });

  it('when the rollback close ALSO fails, both errors stay visible', async () => {
    const connector = new ManualConnector();
    connector.nextSubscribeError = new Error('subscribe exploded');
    connector.nextCloseMode = 'reject';
    const client = new TransportClient({ connector });

    const rejection = await client.connect().then(
      () => null,
      (e: Error) => e.message,
    );
    expect(rejection).toContain('subscribe exploded');
    expect(rejection).toContain('close exploded');
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
