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
import type { EnqueuedInfo, Transport } from './transport.js';
import { RequestManager } from './request-manager.js';
import {
  requireCount,
  requireFiniteAtLeast,
  requireNonNegativeFinite,
} from './validate.js';
import {
  WebSocketTransport,
  type TextFramePolicy,
  type WebSocketFactory,
} from './websocket-transport.js';

export interface SendOptions {
  bizType?: number;
  extHeader?: Uint8Array;
  /**
   * Compression algorithm. The registered codec is applied on THIS side
   * before enqueue; if no codec is registered for the algorithm the send
   * rejects — a raw payload is never shipped under a compressed header.
   * `ZlibCodec` is registered by default; Zstd is injectable via
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
  transport?: 'websocket' | Transport;
  protocols?: string | string[];
  webSocketFactory?: WebSocketFactory;
  onTextFrame?: TextFramePolicy;
  /** Outbound-buffer high-water mark; see
   *  `WebSocketTransportOptions.maxBufferedBytes`. */
  maxBufferedBytes?: number;
  defaultTimeoutMs?: number;
  /**
   * Frame caps, enforced in both directions (default: 16 MiB payload /
   * 65535 ext header — the Rust 2.0 defaults, so both peers agree on what
   * fits). Also bounds decompressed size.
   */
  limits?: Partial<DecodeLimits>;
  /**
   * Inbound normalization queue caps (packets queued behind an in-flight
   * decompression; bytes counted as full frame size — header + ext +
   * payload). Overflow closes the connection: the browser cannot pause the
   * socket, but it can refuse to buffer without limit.
   * Defaults: 8192 frames / 64 MiB.
   */
  maxInboundQueueFrames?: number;
  maxInboundQueueBytes?: number;
  /**
   * Outbound chain caps (sends queued behind an in-flight compression), in
   * operations AND captured payload bytes. Exceeding either rejects the
   * send with `QueueOverflowError` — the connection stays healthy.
   * Defaults: 8192 ops / 64 MiB.
   */
  maxOutboundQueue?: number;
  maxOutboundQueueBytes?: number;
  /**
   * Additional compression codecs. `ZlibCodec` (native
   * Compression/DecompressionStream) is always registered; entries here can
   * add Zstd or override the built-in.
   */
  codecs?: readonly CompressionCodec[];
  /**
   * Connect-time retry policy for a SINGLE `connect()` call. Default: no
   * retry (one attempt). When running a `ReconnectManager` on top, leave
   * this at its zero default — stacking both multiplies attempt counts and
   * wait times; the manager is THE retry policy.
   */
  connectRetry?: ConnectRetryOptions;
}

/**
 * Connect-time retry policy. Mirrors Rust `msgtrans` `RetryConfig`.
 *
 *   - `maxRetries`: number of ADDITIONAL attempts after the first one
 *     fails. Total attempts = `maxRetries + 1`. Default 0 (no retry).
 *   - `initialDelayMs`: delay before the FIRST retry. Default 100.
 *   - `maxDelayMs`: cap applied after exponential growth. Default 10_000.
 *   - `multiplier`: per-attempt backoff multiplier. Default 2.
 */
export interface ConnectRetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  multiplier?: number;
}

type ClientEvent = 'message' | 'request' | 'messageEnqueued' | 'close' | 'error';

/** A one-way message delivered to the application. Always plaintext: the
 *  inbound normalizer has already undone any compression. */
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
 * can construct one and respond with an arbitrary messageId — the forgeable
 * handle Rust 2.0 makes unrepresentable. `respond()` answers at most once,
 * and only onto the connection session it arrived on.
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
 * ONE connection's ownership unit — the internal mapping of Rust 2.0's
 * session: the id allocator, request registry, inbound normalizer and
 * outbound queue all belong to the session, and closing it voids every
 * piece of in-flight async work at once. Nothing here is keyed on a
 * generation NUMBER an adapter must advance by firing events: the object's
 * own identity is the generation, so an adapter that never emits `close`
 * still cannot leak old work into a replacement connection.
 */
class Session {
  readonly manager: RequestManager;
  readonly normalizer: InboundNormalizer;
  outboundChain: Promise<void> = Promise.resolve();
  outboundFrames = 0;
  outboundBytes = 0;
  closed = false;

  constructor(
    transport: Transport,
    defaultTimeoutMs: number | undefined,
    codecs: CodecRegistry,
    limits: DecodeLimits,
    inboundCaps: { maxFrames: number; maxBytes: number },
  ) {
    this.manager = new RequestManager(transport, { defaultTimeoutMs });
    this.normalizer = new InboundNormalizer(codecs, limits, inboundCaps);
  }
}

export class TransportClient {
  private readonly transport: Transport;
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
  private readonly closeListeners: Array<(event?: unknown) => void> = [];
  private readonly errorListeners: Array<(error: unknown) => void> = [];
  private readonly retryOpts: Required<ConnectRetryOptions>;
  /** The CURRENT connection's ownership unit. Replaced atomically on every
   *  real (re)connect; everything async captures the session it started on
   *  and is refused once that object is no longer current. */
  private session: Session;
  /** Single-flight guard: concurrent `connect()` calls share one attempt,
   *  so two completion paths can never each install a session (the id-ABA
   *  the seventh review reproduced). */
  private connectFlight: Promise<void> | null = null;
  private terminated = false;

  constructor(options: TransportClientOptions) {
    this.limits = normalizeDecodeLimits(options.limits);
    this.transport = resolveTransport(options);
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
    this.session = this.newSession();

    this.transport.onMessage((p) => this.handleIncoming(p));
    this.transport.onMessageEnqueued((info) =>
      this.dispatchMessageEnqueued(info),
    );
    this.transport.onClose((ev) => this.dispatchClose(ev));
    this.transport.onError((e) => this.dispatchError(e));
  }

  /**
   * Open the transport with optional retry-on-failure.
   *
   * Idempotent on a live connection, single-flight under concurrency: any
   * number of overlapping `connect()` calls share ONE attempt and observe
   * its outcome, and exactly one new session is installed per real
   * (re)connection.
   */
  connect(): Promise<void> {
    if (this.terminated) {
      return Promise.reject(new TransportError('client is shut down'));
    }
    if (this.transport.isConnected() && !this.session.closed) {
      return Promise.resolve();
    }
    if (this.connectFlight) return this.connectFlight;
    const flight = this.doConnect().finally(() => {
      if (this.connectFlight === flight) this.connectFlight = null;
    });
    this.connectFlight = flight;
    return flight;
  }

  private async doConnect(): Promise<void> {
    const total = this.retryOpts.maxRetries + 1;
    let lastError: unknown = new TransportError('connect failed');
    for (let attempt = 0; attempt < total; attempt++) {
      if (attempt > 0) {
        try {
          await this.transport.close();
        } catch {
          // Ignore — the next connect attempt will surface any real problem.
        }
        const delay = Math.min(
          this.retryOpts.maxDelayMs,
          this.retryOpts.initialDelayMs *
            Math.pow(this.retryOpts.multiplier, attempt - 1),
        );
        await sleep(delay);
        if (this.terminated) throw new TransportError('client is shut down');
      }
      try {
        await this.transport.connect();
        // Exactly one session per real connection, installed by the single
        // flight. The OLD session is closed first, which voids all of its
        // still-running async work — no event from any adapter required.
        this.retireSession(new TransportError('connection superseded'));
        this.session = this.newSession();
        return;
      } catch (e) {
        lastError = e;
      }
    }
    throw lastError;
  }

  /**
   * End the current connection. Reconnecting afterwards is allowed —
   * this is the "hang up" verb, not the "destroy the client" one.
   */
  async disconnect(): Promise<void> {
    // Retire FIRST: even an adapter that never fires a close event cannot
    // leave the old session's async work alive.
    this.retireSession(new TransportError('disconnected'));
    await this.transport.close();
  }

  /**
   * Terminal shutdown: disconnects and refuses every future `connect()`.
   * Use `disconnect()` for a connection you intend to re-establish.
   */
  async shutdown(): Promise<void> {
    this.terminated = true;
    await this.disconnect();
  }

  isConnected(): boolean {
    return this.transport.isConnected();
  }

  /**
   * One-way send. Resolves when the packet was ACCEPTED by the socket's
   * outbound buffer (enqueue-confirmed — a browser WebSocket offers no
   * write confirmation, and this API does not pretend otherwise).
   *
   * SESSION-BOUND: the send belongs to the connection that was current when
   * it was CALLED. If the connection turns over while (async) compression
   * runs, the send is refused — completing it would silently replay a
   * request the application never chose to retry.
   */
  send(payload: Uint8Array, options: SendOptions = {}): Promise<void> {
    const session = this.session;
    if (this.canBypassChain(session, options)) {
      return session.manager.send(
        this.buildPlain(PacketType.OneWay, payload, options),
      );
    }
    return this.scheduleOutbound(session, payload.length, async () => {
      const packet = await this.buildOutbound(
        PacketType.OneWay,
        payload,
        options,
      );
      this.requireCurrent(session);
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
    if (this.canBypassChain(session, options)) {
      const packet = this.buildPlain(PacketType.Request, payload, options);
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
          PacketType.Request,
          payload,
          options,
        );
        this.requireCurrent(session);
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
  on(event: 'close', cb: (event?: unknown) => void): () => void;
  on(event: 'error', cb: (error: unknown) => void): () => void;
  on(
    event: ClientEvent,
    cb:
      | ((msg: ClientMessage) => void)
      | ((req: ClientRequest) => void)
      | ((info: EnqueuedInfo) => void)
      | ((event?: unknown) => void)
      | ((error: unknown) => void),
  ): () => void {
    if (event === 'message') {
      const fn = cb as (msg: ClientMessage) => void;
      this.messageListeners.push(fn);
      return () => removeFrom(this.messageListeners, fn);
    }
    if (event === 'request') {
      // ONE consumer: a request carries a single response obligation, and
      // fanning it out to several handlers splits that responsibility —
      // whoever answers second discovers it at runtime. Rust 2.0's single
      // SessionHandler is the same rule.
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
      const fn = cb as (event?: unknown) => void;
      this.closeListeners.push(fn);
      return () => removeFrom(this.closeListeners, fn);
    }
    const fn = cb as (error: unknown) => void;
    this.errorListeners.push(fn);
    return () => removeFrom(this.errorListeners, fn);
  }

  // ----- session plumbing -----

  private newSession(): Session {
    return new Session(
      this.transport,
      this.defaultTimeoutMs,
      this.codecs,
      this.limits,
      this.inboundCaps,
    );
  }

  private retireSession(reason: TransportError): void {
    const old = this.session;
    if (old.closed) return;
    old.closed = true;
    old.manager.reset(reason);
  }

  private requireCurrent(session: Session): void {
    if (session !== this.session || session.closed) {
      throw new TransportError('connection changed; not sent');
    }
  }

  private handleIncoming(packet: Packet): void {
    // ALL inbound traffic passes through the SESSION's normalizer:
    // compressed bodies are decompressed exactly once, delivery order is
    // arrival order, and everything is bound to the session the packet
    // arrived on — a reconnect installs a fresh session, so old in-flight
    // work has nowhere to deliver, with or without an adapter close event.
    const session = this.session;
    if (session.closed) return;
    session.normalizer.push(
      packet,
      (p) => {
        if (session !== this.session || session.closed) return;
        this.route(session, p);
      },
      (e) => {
        if (session !== this.session || session.closed) return;
        this.dispatchError(e);
        void this.transport.close();
      },
    );
  }

  private route(session: Session, packet: Packet): void {
    if (session.manager.handleIncoming(packet)) return;
    if (packet.packetType === PacketType.Response) {
      // Orphan Response: dropped + reported, never surfaced as data.
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
    for (const cb of this.messageListeners) {
      try {
        cb(msg);
      } catch (e) {
        this.dispatchError(e);
      }
    }
  }

  private respondTo(
    session: Session,
    request: ClientRequestImpl,
    payload: Uint8Array,
    options: SendOptions,
  ): Promise<void> {
    const merged = { ...options, bizType: options.bizType ?? request.bizType };
    if (this.canBypassChain(session, merged)) {
      try {
        this.requireCurrent(session);
      } catch (e) {
        return Promise.reject(e);
      }
      return session.manager.sendResponse(
        this.buildPlain(PacketType.Response, payload, merged, request.messageId),
      );
    }
    return this.scheduleOutbound(session, payload.length, async () => {
      const packet = await this.buildOutbound(
        PacketType.Response,
        payload,
        merged,
        request.messageId,
      );
      // Session re-check AFTER the (async) compression: a response built
      // for one connection must never be written onto its replacement — on
      // the server it could complete a DIFFERENT request with the same id.
      this.requireCurrent(session);
      await session.manager.sendResponse(packet);
    });
  }

  private async buildOutbound(
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
      body = await codec.compress(payload, this.limits.maxPayloadLen);
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

  /** Sync bypass is safe only when there is no codec work AND nothing is
   *  queued on the session's chain (otherwise this packet would overtake
   *  it) AND the session is still current. */
  private canBypassChain(session: Session, options: SendOptions): boolean {
    const requested = options.compression ?? CompressionType.None;
    return (
      requested === CompressionType.None &&
      session.outboundFrames === 0 &&
      session === this.session &&
      !session.closed
    );
  }

  /** Synchronous packet construction for the no-compression path. */
  private buildPlain(
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

  private scheduleOutbound<T>(
    session: Session,
    payloadBytes: number,
    op: () => Promise<T>,
  ): Promise<T> {
    if (
      session.outboundFrames >= this.maxOutboundQueue ||
      session.outboundBytes + payloadBytes > this.maxOutboundQueueBytes
    ) {
      // BOUNDED in ops AND bytes: a stalled compression cannot let sends —
      // or their captured payloads — pile up without limit. The connection
      // stays healthy; the caller backs off.
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
    for (const cb of this.messageEnqueuedListeners) {
      try {
        cb(info);
      } catch (e) {
        this.dispatchError(e);
      }
    }
  }

  private dispatchClose(ev?: unknown): void {
    this.retireSession(new TransportError('transport closed'));
    for (const cb of this.closeListeners) {
      try {
        cb(ev);
      } catch {
        /* swallow */
      }
    }
  }

  private dispatchError(e: unknown): void {
    for (const cb of this.errorListeners) {
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

function resolveTransport(options: TransportClientOptions): Transport {
  if (options.transport && typeof options.transport === 'object') {
    return options.transport;
  }
  if (!options.url) {
    throw new TransportError(
      'TransportClient requires either `url` (for the default WebSocket transport) or a `transport` instance',
    );
  }
  return new WebSocketTransport({
    url: options.url,
    protocols: options.protocols,
    webSocketFactory: options.webSocketFactory,
    onTextFrame: options.onTextFrame,
    maxBufferedBytes: options.maxBufferedBytes,
    limits: options.limits,
  });
}

function removeFrom<T>(arr: T[], item: T): void {
  const i = arr.indexOf(item);
  if (i >= 0) arr.splice(i, 1);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
