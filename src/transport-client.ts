import {
  CompressionType,
  Packet,
  PacketType,
} from './packet.js';
import { TransportError, UnsupportedCompressionError } from './errors.js';
import type { SentInfo, Transport } from './transport.js';
import { RequestManager } from './request-manager.js';
import {
  WebSocketTransport,
  type TextFramePolicy,
  type WebSocketFactory,
} from './websocket-transport.js';

export interface SendOptions {
  bizType?: number;
  extHeader?: Uint8Array;
  reserved?: number;
  /**
   * Compression algorithm.
   *
   * **Status:** Only `CompressionType.None` is currently supported. Passing any
   * other value will reject with `UnsupportedCompressionError`. The field is
   * intentionally part of the API surface so that callers writing against v0.1
   * stay forward-compatible when a codec arrives in v0.3.
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
  defaultTimeoutMs?: number;
  initialMessageId?: number;
}

type ClientEvent = 'message' | 'messageSent' | 'close' | 'error';

export class TransportClient {
  private readonly transport: Transport;
  private readonly requestManager: RequestManager;
  private readonly messageListeners: Array<(ctx: TransportContext) => void> =
    [];
  private readonly messageSentListeners: Array<(info: SentInfo) => void> = [];
  private readonly closeListeners: Array<(event?: unknown) => void> = [];
  private readonly errorListeners: Array<(error: unknown) => void> = [];

  constructor(options: TransportClientOptions) {
    this.transport = resolveTransport(options);
    this.requestManager = new RequestManager(this.transport, {
      defaultTimeoutMs: options.defaultTimeoutMs,
      initialMessageId: options.initialMessageId,
    });

    this.transport.onMessage((p) => this.handleIncoming(p));
    this.transport.onMessageSent((info) => this.dispatchMessageSent(info));
    this.transport.onClose((ev) => this.dispatchClose(ev));
    this.transport.onError((e) => this.dispatchError(e));
  }

  connect(): Promise<void> {
    return this.transport.connect();
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

  send(payload: Uint8Array, options: SendOptions = {}): Promise<void> {
    const guard = checkCompression(options);
    if (guard) return guard;
    const packet = buildPacket(PacketType.OneWay, payload, options);
    return this.requestManager.send(packet);
  }

  async request(
    payload: Uint8Array,
    options: RequestOptions = {},
  ): Promise<Uint8Array> {
    const guard = checkCompression(options);
    if (guard) return guard as unknown as Promise<Uint8Array>;
    const packet = buildPacket(PacketType.Request, payload, options);
    const response = await this.requestManager.request(packet, {
      timeoutMs: options.timeoutMs,
    });
    return response.payload;
  }

  /** Low-level: send a fully-constructed Packet (no compression validation). */
  sendPacket(packet: Packet): Promise<void> {
    return this.requestManager.send(packet);
  }

  /** Low-level: send a Packet as Request and await the matching Response. */
  requestPacket(packet: Packet, options: RequestOptions = {}): Promise<Packet> {
    return this.requestManager.request(packet, {
      timeoutMs: options.timeoutMs,
    });
  }

  on(event: 'message', cb: (ctx: TransportContext) => void): () => void;
  on(event: 'messageSent', cb: (info: SentInfo) => void): () => void;
  on(event: 'close', cb: (event?: unknown) => void): () => void;
  on(event: 'error', cb: (error: unknown) => void): () => void;
  on(
    event: ClientEvent,
    cb:
      | ((ctx: TransportContext) => void)
      | ((info: SentInfo) => void)
      | ((event?: unknown) => void)
      | ((error: unknown) => void),
  ): () => void {
    if (event === 'message') {
      const fn = cb as (ctx: TransportContext) => void;
      this.messageListeners.push(fn);
      return () => removeFrom(this.messageListeners, fn);
    }
    if (event === 'messageSent') {
      const fn = cb as (info: SentInfo) => void;
      this.messageSentListeners.push(fn);
      return () => removeFrom(this.messageSentListeners, fn);
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
    if (this.requestManager.handleIncoming(packet)) return;
    const ctx = new TransportContext(packet, this);
    for (const cb of this.messageListeners) {
      try {
        cb(ctx);
      } catch (e) {
        this.dispatchError(e);
      }
    }
  }

  private dispatchMessageSent(info: SentInfo): void {
    for (const cb of this.messageSentListeners) {
      try {
        cb(info);
      } catch (e) {
        this.dispatchError(e);
      }
    }
  }

  private dispatchClose(ev?: unknown): void {
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

export class TransportContext {
  readonly packet: Packet;
  private readonly client: TransportClient;
  private respondedFlag = false;

  constructor(packet: Packet, client: TransportClient) {
    this.packet = packet;
    this.client = client;
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
  get packetType(): PacketType {
    return this.packet.packetType;
  }
  get isRequest(): boolean {
    return this.packet.packetType === PacketType.Request;
  }
  get hasResponded(): boolean {
    return this.respondedFlag;
  }

  respond(payload: Uint8Array, options: SendOptions = {}): Promise<void> {
    if (!this.isRequest) {
      return Promise.reject(
        new TransportError(
          `respond() is only valid on Request packets (got packetType=${this.packet.packetType})`,
        ),
      );
    }
    if (this.respondedFlag) {
      return Promise.reject(
        new TransportError(
          `respond() already called for messageId=${this.messageId}`,
        ),
      );
    }
    const guard = checkCompression(options);
    if (guard) return guard;

    this.respondedFlag = true;
    const response = new Packet({
      packetType: PacketType.Response,
      messageId: this.packet.messageId,
      bizType: options.bizType ?? this.packet.bizType,
      extHeader: options.extHeader,
      reserved: options.reserved ?? 0,
      compression: options.compression ?? CompressionType.None,
      payload,
    });
    return this.client.sendPacket(response);
  }
}

function buildPacket(
  packetType: PacketType,
  payload: Uint8Array,
  options: SendOptions,
): Packet {
  return new Packet({
    packetType,
    messageId: 0, // RequestManager will allocate
    bizType: options.bizType ?? 0,
    extHeader: options.extHeader,
    reserved: options.reserved ?? 0,
    compression: options.compression ?? CompressionType.None,
    payload,
  });
}

function checkCompression(options: SendOptions): Promise<never> | null {
  if (
    options.compression !== undefined &&
    options.compression !== CompressionType.None
  ) {
    return Promise.reject(new UnsupportedCompressionError(options.compression));
  }
  return null;
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
  });
}

function removeFrom<T>(arr: T[], item: T): void {
  const i = arr.indexOf(item);
  if (i >= 0) arr.splice(i, 1);
}
