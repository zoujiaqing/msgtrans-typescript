import {
  type DecodeLimits,
  normalizeDecodeLimits,
  Packet,
  ReservedFlag,
} from './packet.js';
import { BackpressureError, TransportError, WireFormatError } from './errors.js';
import {
  type EnqueuedInfo,
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
   * Binary-only (see WIRE_FORMAT.md §9.1); a text frame is non-protocol
   * traffic. Default `'error'`: report and CLOSE the connection (strict,
   * matching Rust 2.0's frame policy — a desynchronized peer is not
   * trustworthy). `'ignore'` drops them silently.
   */
  onTextFrame?: TextFramePolicy;
  /**
   * Outbound-buffer high-water mark in bytes. `send()` rejects with
   * `BackpressureError` when the socket's `bufferedAmount` PLUS this frame
   * would exceed the mark — the projection matters: checking only the
   * current level let a 10 MiB frame sail through at 15.9/16 MiB. The
   * connection itself is left open. Default 16 MiB; `0` disables the check.
   * Sockets that don't report `bufferedAmount` are never rejected.
   */
  maxBufferedBytes?: number;
  /**
   * Frame-decoding caps, enforced in BOTH directions with the same numbers
   * (mirroring Rust 2.0, where one `DecodeLimits` serves every protocol):
   * inbound frames over the caps close the connection; outbound frames over
   * the caps are rejected here instead of letting the server kill the
   * connection over them. Defaults match the Rust defaults.
   */
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

export class WebSocketTransport implements Transport {
  private readonly opts: WebSocketTransportOptions;
  private readonly limits: DecodeLimits;
  private ws: WebSocketLike | null = null;
  private _state: TransportState = TransportState.Idle;
  private readonly messageHandlers: Array<(p: Packet) => void> = [];
  private readonly messageEnqueuedHandlers: Array<(info: EnqueuedInfo) => void> =
    [];
  private readonly closeHandlers: Array<(ev?: unknown) => void> = [];
  private readonly errorHandlers: Array<(e: unknown) => void> = [];
  private connectPromise: Promise<void> | null = null;
  private connectResolve: (() => void) | null = null;
  private connectReject: ((e: unknown) => void) | null = null;

  constructor(opts: WebSocketTransportOptions) {
    this.opts = opts;
    this.limits = normalizeDecodeLimits(opts.limits);
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

    // GENERATION ISOLATION: every handler closes over ITS socket and bails
    // unless that socket is still the current one. Without this, a previous
    // socket's late-arriving onclose/onerror/onmessage acted on the shared
    // instance — closing the REPLACEMENT connection's state, nuking its
    // pending requests, or delivering the old generation's traffic into the
    // new one. This is the client-side twin of Rust 2.0's slot generations.
    const isCurrent = () => this.ws === ws;
    ws.onopen = () => {
      if (!isCurrent()) return;
      this._state = TransportState.Open;
      const resolve = this.connectResolve;
      this.connectResolve = null;
      this.connectReject = null;
      resolve?.();
    };
    ws.onmessage = (ev) => {
      if (!isCurrent()) return;
      this.handleMessage(ev);
    };
    ws.onerror = (ev) => {
      if (!isCurrent()) return;
      this.handleError(ev);
    };
    ws.onclose = (ev) => {
      if (!isCurrent()) return;
      this.handleClose(ev);
    };

    return this.connectPromise;
  }

  send(packet: Packet): Promise<void> {
    if (this._state !== TransportState.Open || !this.ws) {
      return Promise.reject(
        new TransportError(`cannot send in state ${this._state}`),
      );
    }
    if (packet.hasFlag(ReservedFlag.Fragmented)) {
      // Refuse to ship a mechanism nothing implements: a Fragmented frame
      // promises reassembly the receiver cannot perform.
      return Promise.reject(
        new WireFormatError(
          'ReservedFlag.Fragmented is not supported over WebSocket transport',
        ),
      );
    }
    // Backpressure guard: PROJECTED, not current-level — the question is
    // whether the buffer would exceed the mark AFTER this frame, otherwise a
    // large frame at 15.9/16 MiB sails through. `totalLength()` is the exact
    // wire size, so the projection costs no encode.
    const max = this.opts.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;
    const buffered = this.ws.bufferedAmount;
    if (max > 0 && buffered !== undefined && buffered + packet.totalLength() > max) {
      return Promise.reject(new BackpressureError(buffered, max));
    }
    // Encode OUTSIDE the socket try: an encode failure (over-limit
    // ext-header/payload) is the caller's packet problem — it must
    // reject this send but never affect the connection.
    let bytes: Uint8Array;
    try {
      bytes = packet.encode(this.limits);
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
    const info: EnqueuedInfo = { messageId: packet.messageId };
    for (const cb of this.messageEnqueuedHandlers) {
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
    const wasConnecting = this._state === TransportState.Connecting;
    this._state = TransportState.Closing;
    try {
      this.ws?.close();
    } catch {
      /* the deterministic close below still runs */
    }
    if (wasConnecting) {
      // CANCEL SAFETY: a close during the handshake must settle the pending
      // connect() — a hung handshake may never fire onclose, and the old
      // behavior left that promise pending FOREVER (the state had already
      // left Connecting, so the eventual onclose no longer rejected it).
      const reject = this.connectReject;
      this.connectResolve = null;
      this.connectReject = null;
      reject?.(new TransportError('connection closed during connect'));
      this.handleClose({ reason: 'closed during connect' });
    }
    return Promise.resolve();
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
      // STRICT: an undecodable or over-limit frame means the peer and this
      // client no longer agree on the protocol — the stream is not
      // trustworthy, so the connection closes (matching Rust 2.0's default
      // FramePolicy). The old behavior reported the error and kept reading.
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

  /** Report a protocol violation and close: strict framing has no
   *  keep-going path. */
  private fatal(e: unknown): void {
    this.dispatchError(e);
    void this.close();
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
