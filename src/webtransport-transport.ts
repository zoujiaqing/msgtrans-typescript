import {
  type DecodeLimits,
  normalizeDecodeLimits,
  Packet,
  ReservedFlag,
} from './packet.js';
import { FIXED_HEADER_LEN } from './constants.js';
import { DecodeLimitError, TransportError, WireFormatError } from './errors.js';
import {
  type EnqueuedInfo,
  type Transport,
  TransportState,
} from './transport.js';

/**
 * The subset of the WebTransport API this adapter needs — injectable so
 * tests (and non-browser hosts) can supply a fake.
 */
export interface WebTransportLike {
  readonly ready: Promise<void>;
  readonly closed: Promise<unknown>;
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
  createBidirectionalStream(): Promise<{
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  }>;
}

export type WebTransportFactory = (url: string) => WebTransportLike;

export interface WebTransportTransportOptions {
  /** https:// URL of the WebTransport endpoint. */
  url: string;
  /** Override the WebTransport constructor. Defaults to
   *  `globalThis.WebTransport`. */
  webTransportFactory?: WebTransportFactory;
  /** Frame caps, both directions; defaults mirror the Rust 2.0 defaults. */
  limits?: Partial<DecodeLimits>;
}

/** Length prefix is a u32 (matches the Rust QUIC adapter's framing). */
const LEN_PREFIX = 4;

/**
 * msgtrans over WebTransport (HTTP/3).
 *
 * Framing mirrors the Rust QUIC adapter so the two can interoperate the day
 * a server exposes a WebTransport endpoint: ONE bidirectional stream per
 * connection, each msgtrans frame preceded by a 4-byte big-endian length.
 * The stream is a byte stream (unlike WebSocket's message framing), so this
 * adapter owns reassembly: partial prefixes, partial frames and multiple
 * frames per chunk are all normal.
 *
 * Semantics match the WebSocket adapter exactly:
 * - `send()` resolves when the chunk was ACCEPTED by the stream writer
 *   (enqueue-confirmed; the writer's promise is the platform's own
 *   backpressure, so no separate high-water mark is layered on top).
 * - Strict inbound: an over-limit length prefix or an undecodable frame
 *   closes the connection.
 * - `ReservedFlag.Fragmented` is refused (nothing implements reassembly).
 * - Generation isolation: a previous session's late readable/closed events
 *   cannot act on the replacement connection.
 */
export class WebTransportTransport implements Transport {
  private readonly opts: WebTransportTransportOptions;
  private readonly limits: DecodeLimits;
  private session: WebTransportLike | null = null;
  private writer: WritableStreamDefaultWriter<Uint8Array> | null = null;
  private _state: TransportState = TransportState.Idle;
  private readonly messageHandlers: Array<(p: Packet) => void> = [];
  private readonly messageEnqueuedHandlers: Array<(info: EnqueuedInfo) => void> =
    [];
  private readonly closeHandlers: Array<(ev?: unknown) => void> = [];
  private readonly errorHandlers: Array<(e: unknown) => void> = [];

  constructor(opts: WebTransportTransportOptions) {
    this.opts = opts;
    this.limits = normalizeDecodeLimits(opts.limits);
  }

  get state(): TransportState {
    return this._state;
  }

  isConnected(): boolean {
    return this._state === TransportState.Open;
  }

  async connect(): Promise<void> {
    if (this._state === TransportState.Open) return;
    if (
      this._state !== TransportState.Idle &&
      this._state !== TransportState.Closed
    ) {
      throw new TransportError(`cannot connect in state ${this._state}`);
    }
    this._state = TransportState.Connecting;

    const factory =
      this.opts.webTransportFactory ?? defaultWebTransportFactory;
    let session: WebTransportLike;
    try {
      session = factory(this.opts.url);
      this.session = session;
      await session.ready;
      const stream = await session.createBidirectionalStream();
      // Guard against a turnover during the awaits above.
      if (this.session !== session) {
        throw new TransportError('connection superseded during connect');
      }
      this.writer = stream.writable.getWriter();
      this._state = TransportState.Open;

      // GENERATION ISOLATION: both long-lived loops close over THEIR
      // session and check it is still current before acting — the
      // WebTransport twin of the WebSocket adapter's socket guard.
      const isCurrent = () => this.session === session;
      void this.readLoop(stream.readable, isCurrent);
      void session.closed.then(
        (info) => {
          if (isCurrent()) this.handleClose(info);
        },
        (e) => {
          if (isCurrent()) {
            this.dispatchError(e);
            this.handleClose(e);
          }
        },
      );
    } catch (e) {
      if (this.session === session!) {
        this._state = TransportState.Closed;
        this.session = null;
        this.writer = null;
      }
      throw e;
    }
  }

  async send(packet: Packet): Promise<void> {
    if (this._state !== TransportState.Open || !this.writer) {
      throw new TransportError(`cannot send in state ${this._state}`);
    }
    if (packet.hasFlag(ReservedFlag.Fragmented)) {
      throw new WireFormatError(
        'ReservedFlag.Fragmented is not supported over WebTransport transport',
      );
    }
    // Encode failures are the caller's packet problem, never the
    // connection's.
    const bytes = packet.encode(this.limits);
    const framed = new Uint8Array(LEN_PREFIX + bytes.length);
    new DataView(framed.buffer).setUint32(0, bytes.length, false);
    framed.set(bytes, LEN_PREFIX);
    // The writer's own promise IS the backpressure: it resolves when the
    // chunk is accepted by the stream queue (enqueue-confirmed, the same
    // honest tier as the WebSocket adapter).
    await this.writer.write(framed);
    const info: EnqueuedInfo = { messageId: packet.messageId };
    for (const cb of this.messageEnqueuedHandlers) {
      try {
        cb(info);
      } catch {
        /* swallow */
      }
    }
  }

  async close(): Promise<void> {
    if (
      this._state === TransportState.Closed ||
      this._state === TransportState.Idle
    ) {
      return;
    }
    this._state = TransportState.Closing;
    try {
      this.session?.close();
    } catch {
      /* closed promise still resolves */
    }
    // The definitive Closed transition arrives via the `closed` promise
    // handler (or has already run); force it if the platform never settles.
    this.handleClose({ reason: 'closed locally' });
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

  /**
   * Reassemble length-prefixed frames from the byte stream and dispatch
   * strictly decoded packets. Every protocol violation is fatal.
   */
  private async readLoop(
    readable: ReadableStream<Uint8Array>,
    isCurrent: () => boolean,
  ): Promise<void> {
    const reader = readable.getReader();
    const maxFrame =
      FIXED_HEADER_LEN + this.limits.maxExtHeaderLen + this.limits.maxPayloadLen;
    let buffer: Uint8Array = new Uint8Array(0);
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (!isCurrent()) return;
        if (done) {
          this.handleClose({ reason: 'stream ended' });
          return;
        }
        buffer = concat(buffer, value);
        for (;;) {
          if (buffer.length < LEN_PREFIX) break;
          const frameLen = new DataView(
            buffer.buffer,
            buffer.byteOffset,
          ).getUint32(0, false);
          if (frameLen > maxFrame) {
            // Declared length over the configured cap: refuse BEFORE
            // buffering it (the Rust QUIC adapter's sanity check, but
            // against the configured limits rather than a constant).
            this.fatal(
              new DecodeLimitError(
                `declared frame ${frameLen} > limit ${maxFrame}`,
              ),
            );
            return;
          }
          if (buffer.length < LEN_PREFIX + frameLen) break;
          const frame = buffer.slice(LEN_PREFIX, LEN_PREFIX + frameLen);
          buffer = buffer.slice(LEN_PREFIX + frameLen);
          let packet: Packet;
          try {
            packet = Packet.decodeExact(frame, this.limits);
          } catch (e) {
            this.fatal(e);
            return;
          }
          for (const cb of this.messageHandlers) {
            try {
              cb(packet);
            } catch (e) {
              this.dispatchError(e);
            }
          }
        }
      }
    } catch (e) {
      if (isCurrent()) {
        this.dispatchError(e);
        this.handleClose(e);
      }
    } finally {
      reader.releaseLock();
    }
  }

  private fatal(e: unknown): void {
    this.dispatchError(e);
    void this.close();
  }

  private handleClose(ev: unknown): void {
    if (this._state === TransportState.Closed) return;
    this._state = TransportState.Closed;
    this.writer = null;
    for (const cb of this.closeHandlers) {
      try {
        cb(ev);
      } catch {
        /* swallow */
      }
    }
  }

  private dispatchError(e: unknown): void {
    for (const cb of this.errorHandlers) {
      try {
        cb(e);
      } catch {
        /* swallow */
      }
    }
  }
}

function defaultWebTransportFactory(url: string): WebTransportLike {
  const Ctor = (globalThis as { WebTransport?: unknown }).WebTransport as
    | (new (url: string) => WebTransportLike)
    | undefined;
  if (!Ctor) {
    throw new TransportError(
      'no global WebTransport available; pass webTransportFactory in options',
    );
  }
  return new Ctor(url);
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) return b;
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}
