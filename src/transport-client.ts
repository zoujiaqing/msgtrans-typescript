import {
  CompressionType,
  type DecodeLimits,
  normalizeDecodeLimits,
  Packet,
  PacketType,
} from './packet.js';
import { TransportError } from './errors.js';
import { CodecRegistry, type CompressionCodec, ZlibCodec } from './codec.js';
import { InboundNormalizer } from './inbound-normalizer.js';
import type { EnqueuedInfo, Transport } from './transport.js';
import { RequestManager } from './request-manager.js';
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
   * Additional compression codecs. `ZlibCodec` (native
   * Compression/DecompressionStream) is always registered; entries here can
   * add Zstd or override the built-in.
   */
  codecs?: readonly CompressionCodec[];
  /**
   * Connect-time retry policy. Default: no retry (one attempt only).
   *
   * This is the language mapping of the Rust crate's `RetryConfig`. It
   * covers the *initial* `connect()` failure path — DNS / WebSocket
   * handshake / cold-start network blips — not auto-reconnect after a
   * dropped connection, which is the consuming SDK's responsibility
   * (`@privchat/sdk` runs its own `ReconnectOptions` state machine on
   * top of this transport).
   *
   * Default 0 retries preserves the original single-attempt behavior.
   * Opt in by passing `{ maxRetries: N, ... }`.
   */
  connectRetry?: ConnectRetryOptions;
}

/**
 * Connect-time retry policy. Mirrors Rust `msgtrans` `RetryConfig`.
 *
 *   - `maxRetries`: number of ADDITIONAL attempts after the first one
 *     fails. Total attempts = `maxRetries + 1`. Default 0 (no retry).
 *   - `initialDelayMs`: delay before the FIRST retry (i.e. after attempt #0
 *     fails, before attempt #1). Default 100.
 *   - `maxDelayMs`: cap applied after exponential growth. Default 10_000.
 *   - `multiplier`: per-attempt backoff multiplier. Default 2.
 *
 * Delay formula for attempt `n` (1-indexed retry): `min(maxDelayMs,
 * initialDelayMs * multiplier^(n - 1))`.
 *
 * Retry is unconditional — any `transport.connect()` rejection triggers
 * a retry until `maxRetries` is exhausted. The last error is rethrown.
 */
export interface ConnectRetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  multiplier?: number;
}

type ClientEvent = 'message' | 'request' | 'messageEnqueued' | 'close' | 'error';

export class TransportClient {
  private readonly transport: Transport;
  private readonly requestManager: RequestManager;
  private readonly limits: DecodeLimits;
  private readonly codecs: CodecRegistry;
  private readonly normalizer: InboundNormalizer;
  private readonly messageListeners: Array<(msg: ClientMessage) => void> = [];
  private readonly requestListeners: Array<(req: ClientRequest) => void> = [];
  private readonly messageEnqueuedListeners: Array<(info: EnqueuedInfo) => void> =
    [];
  private readonly closeListeners: Array<(event?: unknown) => void> = [];
  private readonly errorListeners: Array<(error: unknown) => void> = [];
  private readonly retryOpts: Required<ConnectRetryOptions>;
  /**
   * Program-order guarantee for the outbound path. Compression is async, so
   * without serialization `send(a, {compression}); send(b);` would put `b`
   * on the wire first — the outbound mirror of the inbound reordering the
   * normalizer chain prevents. Every outbound packet passes through this
   * chain; a plain send behind a compressing one waits for it, exactly as
   * Rust's inline `SendOptions::apply` on the send path.
   */
  private outboundChain: Promise<void> = Promise.resolve();
  /** Outbound packets queued or in flight on the chain. */
  private outboundDepth = 0;
  /**
   * Connection generation, bumped on every close. A `ClientRequest` captures
   * the generation it arrived on; its `respond()` refuses to write onto a
   * LATER connection — the client-side mapping of Rust 2.0's
   * generation-bound respond claims.
   */
  private generation = 0;

  constructor(options: TransportClientOptions) {
    this.limits = normalizeDecodeLimits(options.limits);
    this.transport = resolveTransport(options);
    this.requestManager = new RequestManager(this.transport, {
      defaultTimeoutMs: options.defaultTimeoutMs,
    });
    this.codecs = new CodecRegistry([new ZlibCodec(), ...(options.codecs ?? [])]);
    this.normalizer = new InboundNormalizer(this.codecs, this.limits);
    this.retryOpts = {
      maxRetries: options.connectRetry?.maxRetries ?? 0,
      initialDelayMs: options.connectRetry?.initialDelayMs ?? 100,
      maxDelayMs: options.connectRetry?.maxDelayMs ?? 10_000,
      multiplier: options.connectRetry?.multiplier ?? 2,
    };

    this.transport.onMessage((p) => this.handleIncoming(p));
    this.transport.onMessageEnqueued((info) =>
      this.dispatchMessageEnqueued(info),
    );
    this.transport.onClose((ev) => this.dispatchClose(ev));
    this.transport.onError((e) => this.dispatchError(e));
  }

  /** The connection generation `ClientRequest`s validate against. */
  get currentGeneration(): number {
    return this.generation;
  }

  /**
   * Open the transport with optional retry-on-failure.
   *
   * Attempt 0 runs immediately. If it rejects and `connectRetry.maxRetries
   * > 0`, the transport is `close()`d (defensively, to reset any
   * mid-Connecting state that would otherwise cause subsequent
   * `transport.connect()` calls to return the prior rejected promise),
   * a backoff delay is applied, and another attempt is made — up to
   * `maxRetries` additional times. The final attempt's rejection is
   * thrown verbatim.
   */
  async connect(): Promise<void> {
    const total = this.retryOpts.maxRetries + 1;
    let lastError: unknown;
    for (let attempt = 0; attempt < total; attempt++) {
      if (attempt > 0) {
        try {
          await this.transport.close();
        } catch {
          // Ignore — we're about to retry; the next connect attempt
          // will surface any real problem.
        }
        const delay = Math.min(
          this.retryOpts.maxDelayMs,
          this.retryOpts.initialDelayMs *
            Math.pow(this.retryOpts.multiplier, attempt - 1),
        );
        await sleep(delay);
      }
      try {
        await this.transport.connect();
        // A fresh connection is a fresh id generation (Rust: a reconnect
        // gets a fresh session with a fresh counter). Safe: the close that
        // preceded it rejected every pending entry, and stale responders
        // are refused by the generation guard.
        this.requestManager.startGeneration();
        return;
      } catch (e) {
        lastError = e;
      }
    }
    throw lastError;
  }

  /** Close the underlying transport. Alias: `disconnect()`. */
  close(): Promise<void> {
    return this.transport.close();
  }

  /** Alias for `close()` — matches Rust `TransportClient::disconnect()`. */
  disconnect(): Promise<void> {
    return this.transport.close();
  }

  isConnected(): boolean {
    return this.transport.isConnected();
  }

  /**
   * One-way send. Resolves when the packet was ACCEPTED by the socket's
   * outbound buffer (enqueue-confirmed — a browser WebSocket offers no
   * write confirmation, and this API does not pretend otherwise).
   */
  send(payload: Uint8Array, options: SendOptions = {}): Promise<void> {
    // Fast path: no codec work and nothing queued — the enqueue is invoked
    // SYNCHRONOUSLY, exactly like the pre-0.2 client. Anything in flight
    // forces the chain, so a plain send can never overtake a compressing one.
    if (this.canBypassChain(options)) {
      return this.requestManager.send(
        this.buildPlain(PacketType.OneWay, payload, options),
      );
    }
    return this.scheduleOutbound(async () => {
      const packet = await this.buildOutbound(
        PacketType.OneWay,
        payload,
        options,
      );
      await this.requestManager.send(packet);
    });
  }

  async request(
    payload: Uint8Array,
    options: RequestOptions = {},
  ): Promise<Uint8Array> {
    if (this.canBypassChain(options)) {
      const packet = this.buildPlain(PacketType.Request, payload, options);
      const response = await this.requestManager.request(packet, {
        timeoutMs: options.timeoutMs,
      });
      return response.payload;
    }
    const { response } = await this.scheduleOutbound(async () => {
      const packet = await this.buildOutbound(
        PacketType.Request,
        payload,
        options,
      );
      // Hand the packet to the manager INSIDE the chain (so enqueue order is
      // program order) but do not await the response here — that would
      // serialize whole round-trips, not just enqueues. The holder object
      // keeps the promise from being flattened by the async return.
      return {
        response: this.requestManager.request(packet, {
          timeoutMs: options.timeoutMs,
        }),
      };
    });
    return (await response).payload;
  }

  /** INTERNAL — respond path for `ClientRequest`. */
  respondTo(
    request: ClientRequest,
    payload: Uint8Array,
    options: SendOptions,
  ): Promise<void> {
    if (this.canBypassChain(options)) {
      // Generation already verified by the caller (`ClientRequest.respond`)
      // and nothing async happens between here and the enqueue.
      return this.requestManager.sendResponse(
        this.buildPlain(PacketType.Response, payload, {
          ...options,
          bizType: options.bizType ?? request.bizType,
        }, request.messageId),
      );
    }
    return this.scheduleOutbound(async () => {
      const packet = await this.buildOutbound(
        PacketType.Response,
        payload,
        { ...options, bizType: options.bizType ?? request.bizType },
        request.messageId,
      );
      // Generation re-check AFTER the (async) compression: the connection
      // may have turned over while the codec ran, and a response built for
      // generation N must never be written onto generation N+1 — on the
      // server it could complete a DIFFERENT request that drew the same id.
      if (request.generation !== this.generation) {
        throw new TransportError(
          'connection generation changed; response not sent',
        );
      }
      await this.requestManager.sendResponse(packet);
    });
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
      const fn = cb as (req: ClientRequest) => void;
      this.requestListeners.push(fn);
      return () => removeFrom(this.requestListeners, fn);
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

  private handleIncoming(packet: Packet): void {
    // ALL inbound traffic passes through the serial normalizer: compressed
    // bodies are decompressed exactly once, delivery order is arrival order,
    // and a packet that cannot be normalized closes the connection instead
    // of reaching the application as compressed bytes.
    this.normalizer.push(
      packet,
      (p) => this.route(p),
      (e) => {
        this.dispatchError(e);
        void this.transport.close();
      },
    );
  }

  private route(packet: Packet): void {
    if (this.requestManager.handleIncoming(packet)) return;
    if (packet.packetType === PacketType.Response) {
      // Orphan Response: a Response whose messageId matches no pending
      // request (timed out, reset on reconnect, or a misbehaving server).
      // Do NOT surface it as a normal message — apps would mistake it for
      // server-pushed data. Drop it and report via the error channel so it
      // stays observable in logs.
      this.dispatchError(
        new TransportError(
          `dropped orphan Response (messageId=${packet.messageId}, ` +
            `bizType=${packet.bizType}): no pending request`,
        ),
      );
      return;
    }
    if (packet.packetType === PacketType.Request) {
      const req = new ClientRequest(packet, this, this.generation);
      for (const cb of this.requestListeners) {
        try {
          cb(req);
        } catch (e) {
          this.dispatchError(e);
        }
      }
      return;
    }
    const msg = new ClientMessage(packet);
    for (const cb of this.messageListeners) {
      try {
        cb(msg);
      } catch (e) {
        this.dispatchError(e);
      }
    }
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
      // `require` throws when no codec is registered: the send fails
      // instead of shipping a raw payload under a compressed header.
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
   *  queued on the chain (otherwise this packet would overtake it). */
  private canBypassChain(options: SendOptions): boolean {
    const requested = options.compression ?? CompressionType.None;
    return requested === CompressionType.None && this.outboundDepth === 0;
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

  private scheduleOutbound<T>(op: () => Promise<T>): Promise<T> {
    this.outboundDepth += 1;
    const result = this.outboundChain.then(async () => {
      try {
        return await op();
      } finally {
        this.outboundDepth -= 1;
      }
    });
    this.outboundChain = result.then(
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
    this.generation += 1;
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

/** A one-way message delivered to the application. Always plaintext: the
 *  inbound normalizer has already undone any compression. */
export class ClientMessage {
  constructor(private readonly packet: Packet) {}

  get payload(): Uint8Array {
    return this.packet.payload;
  }
  /** Alias for `payload`. */
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

/**
 * A request delivered to the client that carries the obligation to answer —
 * the TS mapping of Rust 2.0's `ClientRequest`. `respond()` answers it at
 * most once (runtime-enforced; TS cannot consume ownership at the type
 * level), and only onto the connection generation it arrived on: a stale
 * response written after a reconnect could complete a DIFFERENT server-side
 * request that drew the same id.
 */
export class ClientRequest {
  private respondedFlag = false;

  constructor(
    private readonly packet: Packet,
    private readonly client: TransportClient,
    readonly generation: number,
  ) {}

  get payload(): Uint8Array {
    return this.packet.payload;
  }
  /** Alias for `payload`. */
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
    if (this.generation !== this.client.currentGeneration) {
      return Promise.reject(
        new TransportError(
          'connection generation changed; response not sent',
        ),
      );
    }
    this.respondedFlag = true;
    return this.client.respondTo(this, payload, options);
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
