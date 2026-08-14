import {
  CompressionType,
  type DecodeLimits,
  normalizeDecodeLimits,
  Packet,
  PacketType,
} from './packet.js';
import { QueueOverflowError, TransportError } from './errors.js';
import { CodecRegistry, type CompressionCodec, ZlibCodec } from './codec.js';
import { InboundNormalizer } from './inbound-normalizer.js';
import {
  type EnqueuedInfo,
  type TransportCloseEvent,
  type TransportConnector,
  type TransportSession,
  TransportState,
} from './transport.js';
import { RequestManager } from './request-manager.js';
import {
  requireCount,
  requireFiniteAtLeast,
  requireNonNegativeFinite,
} from './validate.js';
import {
  WebSocketConnector,
  type TextFramePolicy,
  type WebSocketFactory,
} from './websocket-transport.js';

export interface SendOptions {
  bizType?: number;
  extHeader?: Uint8Array;
  /**
   * Compression algorithm. The registered codec is applied on THIS side
   * before enqueue; no codec for the algorithm rejects the send — a raw
   * payload is never shipped under a compressed header. `ZlibCodec` is
   * registered by default; Zstd is injectable via
   * `TransportClientOptions.codecs`.
   */
  compression?: CompressionType;
}

export interface RequestOptions extends SendOptions {
  /** Override the default request timeout. Pass 0 to disable. */
  timeoutMs?: number;
}

export interface TransportClientOptions {
  url?: string;
  /** A custom connection factory; defaults to `WebSocketConnector` built
   *  from `url` + the WebSocket options below. */
  connector?: TransportConnector;
  protocols?: string | string[];
  webSocketFactory?: WebSocketFactory;
  onTextFrame?: TextFramePolicy;
  /** Outbound-buffer high-water mark; see
   *  `WebSocketConnectorOptions.maxBufferedBytes`. */
  maxBufferedBytes?: number;
  defaultTimeoutMs?: number;
  /** Frame caps, both directions (defaults mirror Rust 2.0). */
  limits?: Partial<DecodeLimits>;
  /** Inbound normalization queue caps. Defaults: 8192 frames / 64 MiB. */
  maxInboundQueueFrames?: number;
  maxInboundQueueBytes?: number;
  /** Outbound chain caps (ops AND captured payload bytes). Defaults:
   *  8192 / 64 MiB. Overflow rejects with `QueueOverflowError`. */
  maxOutboundQueue?: number;
  maxOutboundQueueBytes?: number;
  /** Additional compression codecs (Zlib is built in). */
  codecs?: readonly CompressionCodec[];
  /**
   * Connect-time retry policy for a SINGLE `connect()` call. Default: no
   * retry. Leave at zero when running a `ReconnectManager` on top — the
   * manager is THE retry policy.
   */
  connectRetry?: ConnectRetryOptions;
}

export interface ConnectRetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  multiplier?: number;
}

type ClientEvent = 'message' | 'request' | 'messageEnqueued' | 'close' | 'error';

/** A one-way message delivered to the application. Always plaintext. */
export interface ClientMessage {
  readonly kind: 'message';
  readonly payload: Uint8Array;
  /** Alias for `payload`. */
  readonly data: Uint8Array;
  readonly bizType: number;
  readonly messageId: number;
  readonly extHeader: Uint8Array;
}

/**
 * A request delivered to the client that carries the obligation to answer.
 * An INTERFACE by design: the implementation is not exported, so no consumer
 * can construct one and respond with an arbitrary messageId. `respond()`
 * answers at most once, and only onto the session it arrived on.
 */
export interface ClientRequest {
  readonly kind: 'request';
  readonly payload: Uint8Array;
  /** Alias for `payload`. */
  readonly data: Uint8Array;
  readonly bizType: number;
  readonly messageId: number;
  readonly extHeader: Uint8Array;
  readonly hasResponded: boolean;
  respond(payload: Uint8Array, options?: SendOptions): Promise<void>;
}

/**
 * ONE connection's client-side ownership unit, now BOUND to its
 * `TransportSession`: the physical connection, id allocator, request
 * registry, normalizer, outbound queue and codec abort signal all live and
 * die together. An old session's inbound events route to ITS (closed)
 * manager, its sends enqueue onto ITS (closed) link, and its hung codecs
 * are aborted on retirement — there is no shared object left for stale work
 * to leak through.
 */
class ClientSession {
  readonly manager: RequestManager;
  readonly normalizer: InboundNormalizer;
  readonly abort = new AbortController();
  private unsubscribe: (() => void) | null = null;
  outboundChain: Promise<void> = Promise.resolve();
  outboundFrames = 0;
  outboundBytes = 0;
  closed = false;

  constructor(
    readonly link: TransportSession,
    defaultTimeoutMs: number | undefined,
    codecs: CodecRegistry,
    limits: DecodeLimits,
    inboundCaps: { maxFrames: number; maxBytes: number },
    onEnqueued: (info: EnqueuedInfo) => void,
  ) {
    this.manager = new RequestManager(link, { defaultTimeoutMs, onEnqueued });
    this.normalizer = new InboundNormalizer(
      codecs,
      limits,
      inboundCaps,
      this.abort.signal,
    );
  }

  /** If the session already retired (a close replayed synchronously inside
   *  subscribe(), before the handle existed), run it NOW — never leak the
   *  observer registration. */
  setUnsubscribe(fn: () => void): void {
    if (this.closed) {
      fn();
      return;
    }
    this.unsubscribe = fn;
  }

  retire(reason: TransportError): void {
    if (this.closed) return;
    this.closed = true;
    this.abort.abort(); // release hung codec work and its memory
    this.unsubscribe?.();
    this.unsubscribe = null;
    this.manager.reset(reason);
  }
}

export class TransportClient {
  private readonly connector: TransportConnector;
  private readonly limits: DecodeLimits;
  private readonly codecs: CodecRegistry;
  private readonly defaultTimeoutMs: number | undefined;
  private readonly inboundCaps: { maxFrames: number; maxBytes: number };
  private readonly maxOutboundQueue: number;
  private readonly maxOutboundQueueBytes: number;
  private readonly messageListeners: Array<(msg: ClientMessage) => void> = [];
  private requestListener: ((req: ClientRequest) => void) | null = null;
  private readonly messageEnqueuedListeners: Array<(info: EnqueuedInfo) => void> =
    [];
  private readonly closeListeners: Array<(event: TransportCloseEvent) => void> =
    [];
  private readonly errorListeners: Array<(error: unknown) => void> = [];
  private readonly activityListeners: Array<() => void> = [];
  private readonly retryOpts: Required<ConnectRetryOptions>;

  private _state: TransportState = TransportState.Idle;
  private session: ClientSession | null = null;
  /** Single-flight connect + its cancellation handle. */
  private connectFlight: Promise<void> | null = null;
  private connectAbort: AbortController | null = null;
  /** Single-flight close (disconnect/shutdown). While it is pending, no dial
   *  may start and no other lifecycle op may touch the state — the three
   *  operations are SERIALIZED, never interleaved. */
  private closeFlight: Promise<void> | null = null;

  constructor(options: TransportClientOptions) {
    this.limits = normalizeDecodeLimits(options.limits);
    this.connector = resolveConnector(options);
    this.defaultTimeoutMs =
      options.defaultTimeoutMs === undefined
        ? undefined
        : requireNonNegativeFinite('defaultTimeoutMs', options.defaultTimeoutMs);
    this.codecs = new CodecRegistry([new ZlibCodec(), ...(options.codecs ?? [])]);
    this.inboundCaps = {
      maxFrames: requireCount(
        'maxInboundQueueFrames',
        options.maxInboundQueueFrames ?? 8192,
      ),
      maxBytes: requireCount(
        'maxInboundQueueBytes',
        options.maxInboundQueueBytes ?? 64 * 1024 * 1024,
      ),
    };
    this.maxOutboundQueue = requireCount(
      'maxOutboundQueue',
      options.maxOutboundQueue ?? 8192,
    );
    this.maxOutboundQueueBytes = requireCount(
      'maxOutboundQueueBytes',
      options.maxOutboundQueueBytes ?? 64 * 1024 * 1024,
    );
    const maxRetries = options.connectRetry?.maxRetries ?? 0;
    if (!Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new TransportError(
        `invalid connectRetry.maxRetries: ${maxRetries} (must be a non-negative integer)`,
      );
    }
    this.retryOpts = {
      maxRetries,
      initialDelayMs: requireNonNegativeFinite(
        'connectRetry.initialDelayMs',
        options.connectRetry?.initialDelayMs ?? 100,
      ),
      maxDelayMs: requireNonNegativeFinite(
        'connectRetry.maxDelayMs',
        options.connectRetry?.maxDelayMs ?? 10_000,
      ),
      multiplier: requireFiniteAtLeast(
        'connectRetry.multiplier',
        options.connectRetry?.multiplier ?? 2,
        1,
      ),
    };
  }

  /** Public lifecycle state: Idle / Connecting / Open / Closing / Stopped. */
  get state(): TransportState {
    return this._state;
  }

  /** Un-narrowed read for post-await checks (TS narrows `this._state`
   *  across awaits and mistakes the comparison for dead code). */
  private stateNow(): TransportState {
    return this._state;
  }

  isConnected(): boolean {
    return (
      this._state === TransportState.Open &&
      this.session !== null &&
      this.session.link.isOpen()
    );
  }

  /**
   * Open a connection. Single-flight (concurrent calls share one attempt),
   * idempotent while Open, refused after `shutdown()`, and CANCELLABLE:
   * `disconnect()`/`shutdown()` abort an in-flight dial, and a dial that
   * "wins" after cancellation is closed instead of installed.
   */
  connect(): Promise<void> {
    if (this._state === TransportState.Stopped) {
      return Promise.reject(new TransportError('client is shut down'));
    }
    if (this.isConnected()) return Promise.resolve();
    if (
      this.connectFlight &&
      this.connectAbort &&
      !this.connectAbort.signal.aborted
    ) {
      // A LIVE single-flight is shared. A cancelled one is not: it is doomed
      // to reject, and handing it to a caller who wants a NEW connection
      // would swallow their intent — queue behind it instead.
      return this.connectFlight;
    }
    const previous = this.connectFlight;
    const abort = new AbortController();
    this.connectAbort = abort;
    const flight = (async () => {
      if (previous) await previous.catch(() => {});
      // Serialize behind an in-progress disconnect/shutdown: the dial starts
      // only after the close has fully converged, so a finishing close can
      // never stomp the state a fresh connect just wrote.
      while (this.closeFlight) {
        await this.closeFlight.catch(() => {});
      }
      if (abort.signal.aborted) throw new TransportError('connect aborted');
      if (this.stateNow() === TransportState.Stopped) {
        throw new TransportError('client is shut down');
      }
      await this.doConnect(abort);
    })().finally(() => {
      if (this.connectFlight === flight) {
        this.connectFlight = null;
        this.connectAbort = null;
      }
    });
    this.connectFlight = flight;
    return flight;
  }

  private async doConnect(abort: AbortController): Promise<void> {
    this._state = TransportState.Connecting;
    const total = this.retryOpts.maxRetries + 1;
    let lastError: unknown = new TransportError('connect failed');
    for (let attempt = 0; attempt < total; attempt++) {
      if (attempt > 0) {
        const delay = Math.min(
          this.retryOpts.maxDelayMs,
          this.retryOpts.initialDelayMs *
            Math.pow(this.retryOpts.multiplier, attempt - 1),
        );
        // Abort resolves the sleep early; the check below turns it into
        // rejection — disconnect()/shutdown() never wait a backoff out.
        await sleep(delay, abort.signal);
      }
      if (abort.signal.aborted) {
        if (this._state === TransportState.Connecting) {
          this._state = TransportState.Idle;
        }
        throw new TransportError('connect aborted');
      }
      try {
        const link = await this.connector.connect({ signal: abort.signal });
        // THE LIFECYCLE GATE: a dial that resolves after disconnect()/
        // shutdown() aborted it must not install anything — close the
        // connection it won and report cancellation.
        if (abort.signal.aborted || this.stateNow() === TransportState.Stopped) {
          await link
            .close({ intentional: true, reason: 'connect aborted' })
            .catch(() => {});
          throw new TransportError('connect aborted');
        }
        await this.installSession(link);
        this._state = TransportState.Open;
        return;
      } catch (e) {
        if (abort.signal.aborted || this.stateNow() === TransportState.Stopped) {
          if (this._state === TransportState.Connecting) {
            this._state = TransportState.Idle;
          }
          throw e instanceof Error ? e : new TransportError(String(e));
        }
        lastError = e;
      }
    }
    if (this._state === TransportState.Connecting) {
      this._state = TransportState.Idle;
    }
    throw lastError;
  }

  private async installSession(link: TransportSession): Promise<void> {
    this.retireSession(new TransportError('connection superseded'));
    const session = new ClientSession(
      link,
      this.defaultTimeoutMs,
      this.codecs,
      this.limits,
      this.inboundCaps,
      (info) => {
        if (session === this.session) {
          this.dispatchMessageEnqueued(info);
          this.dispatchActivity();
        }
      },
    );
    // Register as current BEFORE subscribing: a session that synchronously
    // replays a close during subscribe() must be recognized as current so
    // the close handler retires it — not mistaken for a stale link's echo.
    this.session = session;
    // Installation is a TRANSACTION: subscribe() is a public-SPI call that
    // may throw or synchronously replay a close; every failure path must
    // roll back current, retire the session AND release the physical link.
    let unsubscribe: () => void;
    try {
      // The observer is bound to THIS session: an old link's late events act
      // on the old (retired) session and stop there — by construction, not
      // by a generation check someone has to remember.
      unsubscribe = link.subscribe({
        onMessage: (packet) => this.handleInbound(session, packet),
        onClose: (ev) => this.handleLinkClose(session, ev),
        onError: (e) => {
          if (session === this.session && !session.closed)
            this.dispatchError(e);
        },
      });
    } catch (e) {
      if (this.session === session) this.session = null;
      session.retire(new TransportError('subscribe failed'));
      // The installation OWNS the rollback: connect() must not settle (or
      // retry, dialing a second connection) while the physical link from the
      // failed attempt is still being released.
      try {
        await link.close({
          intentional: true,
          reason: 'subscribe failed',
          cause: e,
        });
      } catch (closeError) {
        // Keep BOTH failures visible.
        throw new TransportError(
          `subscribe failed: ${describe(e)}; releasing the connection also failed: ${describe(closeError)}`,
        );
      }
      throw e instanceof Error ? e : new TransportError(String(e));
    }
    // Runs immediately when the session already retired during subscribe():
    // the observer registration must not outlive the session.
    session.setUnsubscribe(unsubscribe);
    if (session.closed || !link.isOpen()) {
      // The link died during installation (a synchronous close replay was
      // already handled above); this connect attempt FAILED — never report
      // Open over a dead session.
      if (this.session === session) this.session = null;
      session.retire(
        new TransportError('connection closed during establishment'),
      );
      throw new TransportError('connection closed during establishment');
    }
  }

  /**
   * End the current connection (and cancel a connect in flight). The client
   * returns to Idle; reconnecting is allowed.
   */
  async disconnect(): Promise<void> {
    this.connectAbort?.abort();
    if (this.connectFlight) {
      await this.connectFlight.catch(() => {});
    }
    if (this.closeFlight) {
      // A close is already in progress: JOIN it instead of racing it.
      await this.closeFlight;
      return;
    }
    const session = this.session;
    if (!session) {
      if (this.stateNow() !== TransportState.Stopped) {
        this._state = TransportState.Idle;
      }
      return;
    }
    if (this.stateNow() !== TransportState.Stopped) {
      this._state = TransportState.Closing;
    }
    // Retire first: even a link whose close() never reports cannot leave
    // the session's async work alive.
    this.retireSession(new TransportError('disconnected'));
    await this.beginClose(session, 'disconnected');
  }

  /** Terminal shutdown: disconnects and refuses every future `connect()`. */
  async shutdown(): Promise<void> {
    this._state = TransportState.Stopped;
    this.connectAbort?.abort();
    if (this.connectFlight) {
      await this.connectFlight.catch(() => {});
    }
    if (this.closeFlight) {
      // Take over the in-flight close: shutdown resolves only once the
      // physical session is actually down, not merely dereferenced.
      await this.closeFlight;
    }
    const session = this.session;
    if (session) {
      this.retireSession(new TransportError('shut down'));
      await this.beginClose(session, 'shut down');
    }
    this._state = TransportState.Stopped;
  }

  /**
   * The ONE close path for intentional session ends. State convergence and
   * the exactly-once close notification live in `finally`: a misbehaving
   * `TransportSession.close()` (rejects, throws) cannot strand the client in
   * Closing or swallow the event. The close EVENT fires for every session
   * end, intentional included — consumers (reconnect machines, UI) key
   * their lifecycle on it and distinguish wanted from unwanted closes with
   * their own flag. Retiring unsubscribed the link observer, so this is the
   * one deterministic notification.
   */
  private beginClose(session: ClientSession, reason: string): Promise<void> {
    const flight = (async () => {
      try {
        await session.link.close({ intentional: true, reason });
      } catch (e) {
        // Converge in finally FIRST, then let the caller of disconnect()/
        // shutdown() see the failure — a broken close() must not be silently
        // reported as success while the physical session may still be open.
        this.dispatchError(e);
        throw e instanceof Error ? e : new TransportError(String(e));
      } finally {
        if (this.stateNow() === TransportState.Closing) {
          this._state = TransportState.Idle;
        }
        this.emitClose({ reason, intentional: true });
      }
    })().finally(() => {
      if (this.closeFlight === flight) this.closeFlight = null;
    });
    this.closeFlight = flight;
    return flight;
  }

  /**
   * One-way send, enqueue-confirmed, SESSION-BOUND: the send belongs to the
   * connection that was current when it was CALLED, and its enqueue lands on
   * that session's link (which rejects once closed) — an async compression
   * cannot carry it across a reconnect.
   */
  send(payload: Uint8Array, options: SendOptions = {}): Promise<void> {
    const session = this.session;
    if (!session || session.closed) {
      return Promise.reject(new TransportError('not connected'));
    }
    if (this.canBypassChain(session, options)) {
      return session.manager.send(
        buildPlain(PacketType.OneWay, payload, options),
      );
    }
    return this.scheduleOutbound(session, payload.length, async () => {
      const packet = await this.buildOutbound(session, PacketType.OneWay, payload, options);
      await session.manager.send(packet);
    });
  }

  async request(
    payload: Uint8Array,
    options: RequestOptions = {},
  ): Promise<Uint8Array> {
    if (options.timeoutMs !== undefined) {
      requireNonNegativeFinite('timeoutMs', options.timeoutMs);
    }
    const session = this.session;
    if (!session || session.closed) {
      throw new TransportError('not connected');
    }
    if (this.canBypassChain(session, options)) {
      const packet = buildPlain(PacketType.Request, payload, options);
      const response = await session.manager.request(packet, {
        timeoutMs: options.timeoutMs,
      });
      return response.payload;
    }
    const { response } = await this.scheduleOutbound(
      session,
      payload.length,
      async () => {
        const packet = await this.buildOutbound(
          session,
          PacketType.Request,
          payload,
          options,
        );
        return {
          response: session.manager.request(packet, {
            timeoutMs: options.timeoutMs,
          }),
        };
      },
    );
    return (await response).payload;
  }

  on(event: 'message', cb: (msg: ClientMessage) => void): () => void;
  on(event: 'request', cb: (req: ClientRequest) => void): () => void;
  on(event: 'messageEnqueued', cb: (info: EnqueuedInfo) => void): () => void;
  on(event: 'close', cb: (event: TransportCloseEvent) => void): () => void;
  on(event: 'error', cb: (error: unknown) => void): () => void;
  on(
    event: ClientEvent,
    cb:
      | ((msg: ClientMessage) => void)
      | ((req: ClientRequest) => void)
      | ((info: EnqueuedInfo) => void)
      | ((event: TransportCloseEvent) => void)
      | ((error: unknown) => void),
  ): () => void {
    if (event === 'message') {
      const fn = cb as (msg: ClientMessage) => void;
      this.messageListeners.push(fn);
      return () => removeFrom(this.messageListeners, fn);
    }
    if (event === 'request') {
      // ONE consumer: a request carries a single response obligation.
      // Liveness monitoring must NOT occupy this slot — use
      // `observeActivity()` for that.
      if (this.requestListener) {
        throw new TransportError(
          'a request consumer is already registered; requests have exactly one',
        );
      }
      const fn = cb as (req: ClientRequest) => void;
      this.requestListener = fn;
      return () => {
        if (this.requestListener === fn) this.requestListener = null;
      };
    }
    if (event === 'messageEnqueued') {
      const fn = cb as (info: EnqueuedInfo) => void;
      this.messageEnqueuedListeners.push(fn);
      return () => removeFrom(this.messageEnqueuedListeners, fn);
    }
    if (event === 'close') {
      const fn = cb as (event: TransportCloseEvent) => void;
      this.closeListeners.push(fn);
      return () => removeFrom(this.closeListeners, fn);
    }
    const fn = cb as (error: unknown) => void;
    this.errorListeners.push(fn);
    return () => removeFrom(this.errorListeners, fn);
  }

  /**
   * Liveness tap: fires on any traffic in either direction (inbound message,
   * request, consumed response; outbound enqueue). Multi-subscriber, so a
   * heartbeat can track activity without occupying the single request
   * consumer slot.
   */
  observeActivity(cb: () => void): () => void {
    this.activityListeners.push(cb);
    return () => removeFrom(this.activityListeners, cb);
  }

  // ----- session plumbing -----

  private retireSession(reason: TransportError): void {
    const old = this.session;
    if (!old) return;
    this.session = null;
    old.retire(reason);
  }

  private handleInbound(session: ClientSession, packet: Packet): void {
    if (session.closed) return;
    session.normalizer.push(
      packet,
      (p) => {
        if (session.closed) return;
        this.route(session, p);
      },
      (e) => {
        if (session.closed) return;
        this.dispatchError(e);
        void session.link
          .close({ intentional: false, reason: 'protocol violation', cause: e })
          .catch(() => {});
      },
    );
  }

  private handleLinkClose(session: ClientSession, ev: TransportCloseEvent): void {
    const wasCurrent = session === this.session;
    session.retire(new TransportError('transport closed'));
    if (!wasCurrent) return; // an old link's close is its own business
    this.session = null;
    if (this._state === TransportState.Connecting) {
      // Closed during installation: the connect() promise carries the
      // failure; a session that never became Open emits no close event.
      return;
    }
    if (this._state !== TransportState.Stopped) {
      this._state = TransportState.Idle;
    }
    this.emitClose(ev);
  }

  private emitClose(ev: TransportCloseEvent): void {
    for (const cb of [...this.closeListeners]) {
      try {
        cb(ev);
      } catch {
        /* swallow */
      }
    }
  }

  private route(session: ClientSession, packet: Packet): void {
    this.dispatchActivity();
    if (session.manager.handleIncoming(packet)) return;
    if (packet.packetType === PacketType.Response) {
      this.dispatchError(
        new TransportError(
          `dropped orphan Response (messageId=${packet.messageId}, ` +
            `bizType=${packet.bizType}): no pending request`,
        ),
      );
      return;
    }
    if (packet.packetType === PacketType.Request) {
      const listener = this.requestListener;
      if (!listener) return; // no consumer: the peer's own timeout applies
      const req = new ClientRequestImpl(packet, (request, payload, options) =>
        this.respondTo(session, request, payload, options),
      );
      try {
        listener(req);
      } catch (e) {
        this.dispatchError(e);
      }
      return;
    }
    const msg = new ClientMessageImpl(packet);
    for (const cb of [...this.messageListeners]) {
      try {
        cb(msg);
      } catch (e) {
        this.dispatchError(e);
      }
    }
  }

  private respondTo(
    session: ClientSession,
    request: ClientRequestImpl,
    payload: Uint8Array,
    options: SendOptions,
  ): Promise<void> {
    const merged = { ...options, bizType: options.bizType ?? request.bizType };
    if (this.canBypassChain(session, merged)) {
      return session.manager.sendResponse(
        buildPlain(PacketType.Response, payload, merged, request.messageId),
      );
    }
    return this.scheduleOutbound(session, payload.length, async () => {
      const packet = await this.buildOutbound(
        session,
        PacketType.Response,
        payload,
        merged,
        request.messageId,
      );
      await session.manager.sendResponse(packet);
    });
  }

  private async buildOutbound(
    session: ClientSession,
    packetType: PacketType,
    payload: Uint8Array,
    options: SendOptions,
    messageId = 0,
  ): Promise<Packet> {
    const requested = options.compression ?? CompressionType.None;
    let body = payload;
    let compression: CompressionType = CompressionType.None;
    if (requested !== CompressionType.None) {
      const codec = this.codecs.require(requested);
      // The session's abort signal: retiring the session cancels this
      // compression instead of letting it hang onto its buffers forever.
      body = await codec.compress(
        payload,
        this.limits.maxPayloadLen,
        session.abort.signal,
      );
      compression = requested;
    }
    return new Packet({
      packetType,
      messageId,
      bizType: options.bizType ?? 0,
      extHeader: options.extHeader,
      compression,
      payload: body,
    });
  }

  private canBypassChain(session: ClientSession, options: SendOptions): boolean {
    const requested = options.compression ?? CompressionType.None;
    return (
      requested === CompressionType.None &&
      session.outboundFrames === 0 &&
      !session.closed
    );
  }

  private scheduleOutbound<T>(
    session: ClientSession,
    payloadBytes: number,
    op: () => Promise<T>,
  ): Promise<T> {
    if (
      session.outboundFrames >= this.maxOutboundQueue ||
      session.outboundBytes + payloadBytes > this.maxOutboundQueueBytes
    ) {
      return Promise.reject(
        new QueueOverflowError(
          'outbound',
          session.outboundFrames,
          session.outboundBytes,
        ),
      );
    }
    session.outboundFrames += 1;
    session.outboundBytes += payloadBytes;
    const result = session.outboundChain.then(async () => {
      try {
        return await op();
      } finally {
        session.outboundFrames -= 1;
        session.outboundBytes -= payloadBytes;
      }
    });
    session.outboundChain = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private dispatchMessageEnqueued(info: EnqueuedInfo): void {
    for (const cb of [...this.messageEnqueuedListeners]) {
      try {
        cb(info);
      } catch (e) {
        this.dispatchError(e);
      }
    }
  }

  private dispatchActivity(): void {
    for (const cb of [...this.activityListeners]) {
      try {
        cb();
      } catch {
        /* swallow */
      }
    }
  }

  private dispatchError(e: unknown): void {
    for (const cb of [...this.errorListeners]) {
      try {
        cb(e);
      } catch {
        /* swallow */
      }
    }
  }
}

// ----- internal handle implementations (NOT exported) -----

class ClientMessageImpl implements ClientMessage {
  readonly kind = 'message' as const;
  constructor(private readonly packet: Packet) {}
  get payload(): Uint8Array {
    return this.packet.payload;
  }
  get data(): Uint8Array {
    return this.packet.payload;
  }
  get bizType(): number {
    return this.packet.bizType;
  }
  get messageId(): number {
    return this.packet.messageId;
  }
  get extHeader(): Uint8Array {
    return this.packet.extHeader;
  }
}

class ClientRequestImpl implements ClientRequest {
  readonly kind = 'request' as const;
  private respondedFlag = false;

  constructor(
    private readonly packet: Packet,
    private readonly respondPath: (
      request: ClientRequestImpl,
      payload: Uint8Array,
      options: SendOptions,
    ) => Promise<void>,
  ) {}

  get payload(): Uint8Array {
    return this.packet.payload;
  }
  get data(): Uint8Array {
    return this.packet.payload;
  }
  get bizType(): number {
    return this.packet.bizType;
  }
  get messageId(): number {
    return this.packet.messageId;
  }
  get extHeader(): Uint8Array {
    return this.packet.extHeader;
  }
  get hasResponded(): boolean {
    return this.respondedFlag;
  }

  respond(payload: Uint8Array, options: SendOptions = {}): Promise<void> {
    if (this.respondedFlag) {
      return Promise.reject(
        new TransportError(
          `respond() already called for messageId=${this.messageId}`,
        ),
      );
    }
    this.respondedFlag = true;
    return this.respondPath(this, payload, options);
  }
}

function buildPlain(
  packetType: PacketType,
  payload: Uint8Array,
  options: SendOptions,
  messageId = 0,
): Packet {
  return new Packet({
    packetType,
    messageId,
    bizType: options.bizType ?? 0,
    extHeader: options.extHeader,
    compression: CompressionType.None,
    payload,
  });
}

function resolveConnector(options: TransportClientOptions): TransportConnector {
  if (options.connector) return options.connector;
  if (!options.url) {
    throw new TransportError(
      'TransportClient requires either `url` (for the default WebSocket connector) or a `connector`',
    );
  }
  return new WebSocketConnector({
    url: options.url,
    protocols: options.protocols,
    webSocketFactory: options.webSocketFactory,
    onTextFrame: options.onTextFrame,
    maxBufferedBytes: options.maxBufferedBytes,
    limits: options.limits,
  });
}

function describe(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function removeFrom<T>(arr: T[], item: T): void {
  const i = arr.indexOf(item);
  if (i >= 0) arr.splice(i, 1);
}

/** Resolves after `ms` OR as soon as `signal` aborts — a cancelled wait must
 *  not hold `disconnect()`/`shutdown()` hostage for the backoff duration. */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0 || signal?.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const finish = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal?.addEventListener('abort', finish);
  });
}
