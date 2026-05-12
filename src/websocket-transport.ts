import { Packet } from './packet.js';
import { TransportError } from './errors.js';
import {
  type SentInfo,
  type Transport,
  TransportState,
} from './transport.js';

export interface WebSocketLike {
  binaryType: string;
  readyState: number;
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
}

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
    try {
      this.ws.send(packet.encode());
    } catch (e) {
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
