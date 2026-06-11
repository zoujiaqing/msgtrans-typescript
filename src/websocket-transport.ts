import { Packet } from './packet.js';
import { BackpressureError, TransportError } from './errors.js';
import {
  type SentInfo,
  type Transport,
  TransportState,
} from './transport.js';

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

export interface WebSocketTransportOptions {
  url: string;
  protocols?: string | string[];
  /**
   * Override the WebSocket constructor. Defaults to `globalThis.WebSocket`.
   * Use this to inject `ws` in Node, or a mock in tests.
   */
  webSocketFactory?: WebSocketFactory;
  /**
   * What to do with WebSocket text frames. msgtrans canonical traffic is
   * Binary-only (see WIRE_FORMAT.md §9.1); text frames indicate misuse.
   * Default: `'error'`.
   */
  onTextFrame?: TextFramePolicy;
  /**
   * Outbound-buffer high-water mark in bytes. When the socket's
   * `bufferedAmount` is already above this value, `send()` rejects with
   * `BackpressureError` instead of silently piling more bytes onto a
   * link that isn't draining (slow network → unbounded memory growth).
   * The connection itself is left open. Default 16 MiB; `0` disables
   * the check (legacy behavior). Sockets that don't report
   * `bufferedAmount` (minimal fakes) are never rejected.
   */
  maxBufferedBytes?: number;
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

export class WebSocketTransport implements Transport {
  private readonly opts: WebSocketTransportOptions;
  private ws: WebSocketLike | null = null;
  private _state: TransportState = TransportState.Idle;
  private readonly messageHandlers: Array<(p: Packet) => void> = [];
  private readonly messageSentHandlers: Array<(info: SentInfo) => void> = [];
  private readonly closeHandlers: Array<(ev?: unknown) => void> = [];
  private readonly errorHandlers: Array<(e: unknown) => void> = [];
  private connectPromise: Promise<void> | null = null;
  private connectResolve: (() => void) | null = null;
  private connectReject: ((e: unknown) => void) | null = null;

  constructor(opts: WebSocketTransportOptions) {
    this.opts = opts;
  }

  get state(): TransportState {
    return this._state;
  }

  isConnected(): boolean {
    return this._state === TransportState.Open;
  }

  connect(): Promise<void> {
    if (this._state === TransportState.Open) return Promise.resolve();
    if (this._state === TransportState.Connecting && this.connectPromise) {
      return this.connectPromise;
    }
    if (
      this._state !== TransportState.Idle &&
      this._state !== TransportState.Closed
    ) {
      return Promise.reject(
        new TransportError(`cannot connect in state ${this._state}`),
      );
    }

    this._state = TransportState.Connecting;
    const factory = this.opts.webSocketFactory ?? defaultWebSocketFactory;

    let ws: WebSocketLike;
    try {
      ws = factory(this.opts.url, this.opts.protocols);
    } catch (e) {
      this._state = TransportState.Closed;
      return Promise.reject(e);
    }
    ws.binaryType = 'arraybuffer';
    this.ws = ws;

    this.connectPromise = new Promise<void>((resolve, reject) => {
      this.connectResolve = resolve;
      this.connectReject = reject;
    });

    ws.onopen = () => {
      this._state = TransportState.Open;
      const resolve = this.connectResolve;
      this.connectResolve = null;
      this.connectReject = null;
      resolve?.();
    };
    ws.onmessage = (ev) => this.handleMessage(ev);
    ws.onerror = (ev) => this.handleError(ev);
    ws.onclose = (ev) => this.handleClose(ev);

    return this.connectPromise;
  }

  send(packet: Packet): Promise<void> {
    if (this._state !== TransportState.Open || !this.ws) {
      return Promise.reject(
        new TransportError(`cannot send in state ${this._state}`),
      );
    }
    // Backpressure guard: reject (without closing) when the outbound
    // buffer is already above the high-water mark. Checked BEFORE
    // encoding so an over-pressure caller pays no encode cost.
    const max = this.opts.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;
    const buffered = this.ws.bufferedAmount;
    if (max > 0 && buffered !== undefined && buffered > max) {
      return Promise.reject(new BackpressureError(buffered, max));
    }
    // Encode OUTSIDE the socket try: an encode failure (oversized
    // ext-header/payload) is the caller's packet problem — it must
    // reject this send but never affect the connection.
    let bytes: Uint8Array;
    try {
      bytes = packet.encode();
    } catch (e) {
      return Promise.reject(e);
    }
    try {
      this.ws.send(bytes);
    } catch (e) {
      // ws.send() threw synchronously. Browsers throw when the socket
      // closed mid-call (INVALID_STATE_ERR) — but the `onclose` event
      // may lag behind, leaving this transport claiming Open against a
      // dead socket. If readyState says the socket is no longer OPEN,
      // drive the close path NOW (idempotent — the eventual real
      // onclose becomes a no-op) so request managers reject pending
      // work and reconnect state machines react immediately. A throw
      // while readyState is still OPEN (exotic) stays a plain
      // per-send rejection: the connection is not assumed dead.
      const WS_OPEN = 1;
      if (this.ws.readyState !== WS_OPEN) {
        this.handleClose({
          code: 1006,
          reason: 'send failed on dead socket',
          wasClean: false,
          cause: e,
        });
      }
      return Promise.reject(e);
    }
    const info: SentInfo = { messageId: packet.messageId };
    for (const cb of this.messageSentHandlers) {
      try {
        cb(info);
      } catch {
        /* swallow */
      }
    }
    return Promise.resolve();
  }

  close(): Promise<void> {
    if (
      this._state === TransportState.Closed ||
      this._state === TransportState.Idle
    ) {
      return Promise.resolve();
    }
    if (this._state === TransportState.Closing) {
      return Promise.resolve();
    }
    this._state = TransportState.Closing;
    try {
      this.ws?.close();
    } catch {
      // The transition to Closed will still happen via onclose, or
      // if the WS never fires onclose, we leave it in Closing — caller
      // can read state to detect.
    }
    return Promise.resolve();
  }

  onMessage(cb: (packet: Packet) => void): void {
    this.messageHandlers.push(cb);
  }

  onMessageSent(cb: (info: SentInfo) => void): void {
    this.messageSentHandlers.push(cb);
  }

  onClose(cb: (event?: unknown) => void): void {
    this.closeHandlers.push(cb);
  }

  onError(cb: (error: unknown) => void): void {
    this.errorHandlers.push(cb);
  }

  private handleMessage(ev: { data: unknown }): void {
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
      this.dispatchError(
        new TransportError(
          'received text frame; msgtrans uses binary frames only',
        ),
      );
      return;
    }
    this.dispatchError(
      new TransportError(`unsupported WebSocket message type: ${typeof data}`),
    );
  }

  private decodeAndDispatch(bytes: Uint8Array): void {
    let packet: Packet;
    try {
      packet = Packet.decode(bytes);
    } catch (e) {
      this.dispatchError(e);
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

  private handleError(ev: unknown): void {
    this.dispatchError(ev);
    if (this._state === TransportState.Connecting && this.connectReject) {
      const reject = this.connectReject;
      this.connectResolve = null;
      this.connectReject = null;
      reject(ev instanceof Error ? ev : new TransportError('WebSocket error'));
    }
  }

  private handleClose(ev: unknown): void {
    // Idempotent: the close path can be driven twice — once synthetically
    // by a send() that failed on a dead socket, once by the socket's own
    // (lagging) onclose. Consumers must see close exactly once, otherwise
    // e.g. a reconnect state machine schedules two reconnect cycles.
    if (this._state === TransportState.Closed) return;
    const wasConnecting = this._state === TransportState.Connecting;
    this._state = TransportState.Closed;
    for (const cb of this.closeHandlers) {
      try {
        cb(ev);
      } catch {
        /* swallow */
      }
    }
    if (wasConnecting && this.connectReject) {
      const reject = this.connectReject;
      this.connectResolve = null;
      this.connectReject = null;
      reject(new TransportError('connection closed before open'));
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
