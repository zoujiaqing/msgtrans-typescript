import {
  type DecodeLimits,
  normalizeDecodeLimits,
  Packet,
  ReservedFlag,
} from './packet.js';
import { DecodeLimitError, TransportError, WireFormatError } from './errors.js';
import type {
  ConnectOptions,
  EnqueuedInfo,
  TransportCloseEvent,
  TransportConnector,
  TransportObserver,
  TransportSession,
} from './transport.js';
import { FIXED_HEADER_LEN } from './constants.js';

/** The subset of the WebTransport API this adapter needs — injectable so
 *  tests (and non-browser hosts) can supply a fake. */
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

export interface WebTransportConnectorOptions {
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
 * WebTransport connector (EXPERIMENTAL): configuration only. Each connect
 * dials a fresh HTTP/3 session and returns a `WebTransportSession` that owns
 * it. Framing mirrors the Rust QUIC adapter — one bidirectional stream,
 * 4-byte big-endian length prefix per msgtrans frame.
 */
export class WebTransportConnector implements TransportConnector {
  private readonly opts: WebTransportConnectorOptions;
  private readonly limits: DecodeLimits;

  constructor(opts: WebTransportConnectorOptions) {
    this.opts = opts;
    this.limits = normalizeDecodeLimits(opts.limits);
  }

  async connect(options: ConnectOptions): Promise<TransportSession> {
    const { signal } = options;
    if (signal.aborted) throw new TransportError('connect aborted');
    const factory =
      this.opts.webTransportFactory ?? defaultWebTransportFactory;
    const wt = factory(this.opts.url);
    const closeQuietly = () => {
      try {
        wt.close();
      } catch {
        /* already dead */
      }
    };
    // Abort must win even if the platform never settles `ready` (or
    // `createBidirectionalStream`): both dial stages RACE the signal instead
    // of trusting `close()` to make them reject.
    let onAbort: () => void = () => {};
    const aborted = new Promise<never>((_, reject) => {
      onAbort = () => {
        closeQuietly();
        reject(new TransportError('connect aborted'));
      };
      signal.addEventListener('abort', onAbort, { once: true });
    });
    // The race's loser must not surface as an unhandled rejection.
    aborted.catch(() => {});
    try {
      await Promise.race([wt.ready, aborted]);
      const stream = await Promise.race([
        wt.createBidirectionalStream(),
        aborted,
      ]);
      if (signal.aborted) {
        // Cancel AND release the connection the dial won.
        closeQuietly();
        throw new TransportError('connect aborted');
      }
      return new WebTransportSession(wt, stream, this.limits);
    } catch (e) {
      closeQuietly();
      throw e instanceof Error ? e : new TransportError(String(e));
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }
}

/** One live WebTransport connection. */
class WebTransportSession implements TransportSession {
  private readonly observers: TransportObserver[] = [];
  private readonly writer: WritableStreamDefaultWriter<Uint8Array>;
  private open = true;

  constructor(
    private readonly wt: WebTransportLike,
    stream: {
      readable: ReadableStream<Uint8Array>;
      writable: WritableStream<Uint8Array>;
    },
    private readonly limits: DecodeLimits,
  ) {
    this.writer = stream.writable.getWriter();
    void this.readLoop(stream.readable);
    void wt.closed.then(
      (info) =>
        this.handleClose({ intentional: false, reason: 'closed', cause: info }),
      (e) => {
        this.dispatchError(e);
        this.handleClose({ intentional: false, reason: 'closed', cause: e });
      },
    );
  }

  isOpen(): boolean {
    return this.open;
  }

  async enqueue(packet: Packet): Promise<EnqueuedInfo> {
    if (!this.open) throw new TransportError('session is closed');
    if (packet.hasFlag(ReservedFlag.Fragmented)) {
      throw new WireFormatError(
        'ReservedFlag.Fragmented is not supported over WebTransport transport',
      );
    }
    const bytes = packet.encode(this.limits);
    const framed = new Uint8Array(LEN_PREFIX + bytes.length);
    new DataView(framed.buffer).setUint32(0, bytes.length, false);
    framed.set(bytes, LEN_PREFIX);
    // The writer's promise IS the platform's backpressure (accepted by the
    // stream queue = enqueue-confirmed, the same honest tier as WebSocket).
    await this.writer.write(framed);
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
    if (!this.open) return Promise.resolve();
    // Local intent first (see WebSocketSession.close): a synchronous platform
    // callback must not relabel an intentional close, and convergence must
    // not depend on the platform at all.
    this.handleClose({ intentional: true, reason: 'closed locally', ...event });
    try {
      // Map the caller's code/reason onto the WebTransport close info.
      if (event?.code !== undefined || event?.reason !== undefined) {
        this.wt.close({ closeCode: event.code, reason: event.reason });
      } else {
        this.wt.close();
      }
    } catch (e) {
      return Promise.reject(
        e instanceof Error ? e : new TransportError(String(e)),
      );
    }
    return Promise.resolve();
  }

  /** Length-prefixed byte-stream reassembly; every violation is fatal. */
  private async readLoop(readable: ReadableStream<Uint8Array>): Promise<void> {
    const reader = readable.getReader();
    const maxFrame =
      FIXED_HEADER_LEN + this.limits.maxExtHeaderLen + this.limits.maxPayloadLen;
    let buffer: Uint8Array = new Uint8Array(0);
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (!this.open) return;
        if (done) {
          this.handleClose({ intentional: false, reason: 'stream ended' });
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
            // Refuse BEFORE buffering it (framing-layer bomb defense).
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
          for (const o of [...this.observers]) {
            try {
              o.onMessage?.(packet);
            } catch (e) {
              this.dispatchError(e);
            }
          }
        }
      }
    } catch (e) {
      if (this.open) {
        this.dispatchError(e);
        this.handleClose({ intentional: false, reason: 'read failed', cause: e });
      }
    } finally {
      reader.releaseLock();
    }
  }

  private fatal(e: unknown): void {
    this.dispatchError(e);
    void this.close({ intentional: false, reason: 'protocol violation', cause: e });
  }

  private handleClose(ev: TransportCloseEvent): void {
    if (!this.open) return;
    this.open = false;
    for (const o of [...this.observers]) {
      try {
        o.onClose?.(ev);
      } catch {
        /* swallow */
      }
    }
  }

  private dispatchError(e: unknown): void {
    if (!this.open) return;
    for (const o of [...this.observers]) {
      try {
        o.onError?.(e);
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
