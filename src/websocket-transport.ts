import {
  type DecodeLimits,
  normalizeDecodeLimits,
  Packet,
  ReservedFlag,
} from './packet.js';
import {
  BackpressureError,
  TransportError,
  WireFormatError,
} from './errors.js';
import type {
  ConnectOptions,
  EnqueuedInfo,
  TransportConnector,
  TransportObserver,
  TransportSession,
} from './transport.js';
import { requireNonNegativeFinite } from './validate.js';

export interface WebSocketLike {
  binaryType: string;
  readyState: number;
  /** Bytes queued via send() but not yet flushed to the network.
   *  Standard on browser/`ws` WebSockets; optional here so minimal
   *  fakes keep working (absence disables the backpressure check). */
  bufferedAmount?: number;
  send(data: ArrayBufferLike | ArrayBufferView | string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
}

export type WebSocketFactory = (
  url: string,
  protocols?: string | string[],
) => WebSocketLike;

export type TextFramePolicy = 'error' | 'ignore';

export interface WebSocketConnectorOptions {
  url: string;
  protocols?: string | string[];
  /** Override the WebSocket constructor. Defaults to `globalThis.WebSocket`. */
  webSocketFactory?: WebSocketFactory;
  /** Text frames are non-protocol traffic. Default `'error'`: report and
   *  CLOSE (strict). `'ignore'` drops them silently. */
  onTextFrame?: TextFramePolicy;
  /**
   * Outbound-buffer high-water mark in bytes. `enqueue()` rejects with
   * `BackpressureError` when `bufferedAmount` PLUS this frame would exceed
   * the mark. The connection stays open. Default 16 MiB; `0` disables.
   */
  maxBufferedBytes?: number;
  /** Frame caps, both directions. Defaults mirror the Rust 2.0 defaults. */
  limits?: Partial<DecodeLimits>;
}

/** Default outbound high-water mark: 16 MiB. */
export const DEFAULT_MAX_BUFFERED_BYTES = 16 * 1024 * 1024;

function defaultWebSocketFactory(
  url: string,
  protocols?: string | string[],
): WebSocketLike {
  const Ctor = (globalThis as { WebSocket?: unknown }).WebSocket as
    | (new (url: string, protocols?: string | string[]) => WebSocketLike)
    | undefined;
  if (!Ctor) {
    throw new TransportError(
      'no global WebSocket available; pass webSocketFactory in options',
    );
  }
  return new Ctor(url, protocols);
}

/**
 * WebSocket connector: CONFIGURATION only. Every `connect()` dials a fresh
 * socket and hands it to a brand-new `WebSocketSession` that owns it — there
 * is no shared mutable connection state for a late event to revive or an old
 * send to leak through. (The previous shared-Transport shape produced exactly
 * those faults: a late onopen flipping Closed back to Open, an old socket's
 * traffic delivered as the new one's.)
 */
export class WebSocketConnector implements TransportConnector {
  private readonly opts: WebSocketConnectorOptions;
  private readonly limits: DecodeLimits;

  constructor(opts: WebSocketConnectorOptions) {
    if (opts.maxBufferedBytes !== undefined) {
      requireNonNegativeFinite('maxBufferedBytes', opts.maxBufferedBytes);
    }
    this.opts = opts;
    this.limits = normalizeDecodeLimits(opts.limits);
  }

  connect(options: ConnectOptions): Promise<TransportSession> {
    const { signal } = options;
    if (signal.aborted) {
      return Promise.reject(new TransportError('connect aborted'));
    }
    const factory = this.opts.webSocketFactory ?? defaultWebSocketFactory;
    let ws: WebSocketLike;
    try {
      ws = factory(this.opts.url, this.opts.protocols);
    } catch (e) {
      return Promise.reject(e);
    }
    ws.binaryType = 'arraybuffer';

    return new Promise<TransportSession>((resolve, reject) => {
      let settled = false;
      const onAbort = () => {
        if (settled) return;
        settled = true;
        // Cancel AND release: the socket must not keep connecting toward a
        // caller that already gave up.
        detach();
        try {
          ws.close();
        } catch {
          /* already dead */
        }
        reject(new TransportError('connect aborted'));
      };
      const detach = () => {
        signal.removeEventListener('abort', onAbort);
        ws.onopen = null;
        ws.onerror = null;
        ws.onclose = null;
      };
      signal.addEventListener('abort', onAbort);

      ws.onopen = () => {
        if (settled) return;
        settled = true;
        signal.removeEventListener('abort', onAbort);
        resolve(new WebSocketSession(ws, this.opts, this.limits));
      };
      ws.onerror = (ev) => {
        if (settled) return;
        settled = true;
        detach();
        try {
          ws.close();
        } catch {
          /* already dead */
        }
        reject(ev instanceof Error ? ev : new TransportError('WebSocket error'));
      };
      ws.onclose = (ev) => {
        if (settled) return;
        settled = true;
        detach();
        reject(
          new TransportError(
            `connection closed before open: ${JSON.stringify(ev ?? null)}`,
          ),
        );
      };
    });
  }
}

/** One live WebSocket connection. */
class WebSocketSession implements TransportSession {
  private readonly observers: TransportObserver[] = [];
  private open = true;

  constructor(
    private readonly ws: WebSocketLike,
    private readonly opts: WebSocketConnectorOptions,
    private readonly limits: DecodeLimits,
  ) {
    // The session takes over the socket's handlers for its whole life. A
    // handler firing after close is harmless: `open` gates every path.
    ws.onmessage = (ev) => this.handleMessage(ev);
    ws.onerror = (ev) => this.dispatchError(ev);
    ws.onclose = (ev) => this.handleClose(ev);
  }

  isOpen(): boolean {
    return this.open;
  }

  enqueue(packet: Packet): Promise<EnqueuedInfo> {
    if (!this.open) {
      return Promise.reject(new TransportError('session is closed'));
    }
    if (packet.hasFlag(ReservedFlag.Fragmented)) {
      return Promise.reject(
        new WireFormatError(
          'ReservedFlag.Fragmented is not supported over WebSocket transport',
        ),
      );
    }
    // Projected backpressure: current buffer PLUS this frame.
    const max = this.opts.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;
    const buffered = this.ws.bufferedAmount;
    if (
      max > 0 &&
      buffered !== undefined &&
      buffered + packet.totalLength() > max
    ) {
      return Promise.reject(new BackpressureError(buffered, max));
    }
    let bytes: Uint8Array;
    try {
      bytes = packet.encode(this.limits);
    } catch (e) {
      // The caller's packet problem, never the connection's.
      return Promise.reject(e);
    }
    try {
      this.ws.send(bytes);
    } catch (e) {
      const WS_OPEN = 1;
      if (this.ws.readyState !== WS_OPEN) {
        // The socket died and its onclose is lagging: drive the close NOW so
        // the owner reacts immediately (idempotent — the real onclose is a
        // no-op afterwards).
        this.handleClose({
          code: 1006,
          reason: 'send failed on dead socket',
          wasClean: false,
          cause: e,
        });
      }
      return Promise.reject(e);
    }
    return Promise.resolve({ messageId: packet.messageId });
  }

  subscribe(observer: TransportObserver): () => void {
    this.observers.push(observer);
    return () => {
      const i = this.observers.indexOf(observer);
      if (i >= 0) this.observers.splice(i, 1);
    };
  }

  close(reason?: unknown): Promise<void> {
    if (!this.open) return Promise.resolve();
    try {
      this.ws.close();
    } catch {
      /* the deterministic close below still runs */
    }
    // Deterministic: do not depend on the platform ever firing onclose.
    this.handleClose(reason ?? { reason: 'closed locally' });
    return Promise.resolve();
  }

  private handleMessage(ev: { data: unknown }): void {
    if (!this.open) return;
    const data = ev.data;
    if (data instanceof ArrayBuffer) {
      this.decodeAndDispatch(new Uint8Array(data));
      return;
    }
    if (ArrayBuffer.isView(data)) {
      const view = data as ArrayBufferView;
      this.decodeAndDispatch(
        new Uint8Array(view.buffer, view.byteOffset, view.byteLength),
      );
      return;
    }
    if (typeof data === 'string') {
      if (this.opts.onTextFrame === 'ignore') return;
      this.fatal(
        new TransportError(
          'received text frame; msgtrans uses binary frames only',
        ),
      );
      return;
    }
    this.fatal(
      new TransportError(`unsupported WebSocket message type: ${typeof data}`),
    );
  }

  private decodeAndDispatch(bytes: Uint8Array): void {
    let packet: Packet;
    try {
      packet = Packet.decodeExact(bytes, this.limits);
    } catch (e) {
      // STRICT: a desynchronized peer is not trustworthy.
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

  private fatal(e: unknown): void {
    this.dispatchError(e);
    void this.close({ reason: 'protocol violation', cause: e });
  }

  private handleClose(ev: unknown): void {
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
