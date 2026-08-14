import type { Packet } from './packet.js';

/** Public lifecycle states of a `TransportClient`. */
export const TransportState = {
  Idle: 'idle',
  Connecting: 'connecting',
  Open: 'open',
  Closing: 'closing',
  /** Terminal: `shutdown()` was called; every later `connect()` is refused. */
  Stopped: 'stopped',
} as const;
export type TransportState =
  (typeof TransportState)[keyof typeof TransportState];

/**
 * Emitted when a packet has been ACCEPTED by the underlying socket's outbound
 * buffer. Deliberately named "enqueued", not "sent": a browser WebSocket
 * offers no write completion, so nothing at this layer can prove the bytes
 * reached the network.
 */
export interface EnqueuedInfo {
  messageId: number;
}

/**
 * The connection-factory half of the SPI.
 *
 * A connector holds CONFIGURATION only — no socket, no writer, no event
 * handlers, no mutable connection state. Every successful `connect()` returns
 * a brand-new `TransportSession` that OWNS its physical connection. This is
 * the structural fix for a whole family of faults the previous shared
 * `Transport` object produced: an old connection's late events being
 * attributed to the new one, an old async send writing into the replacement
 * socket, a stale close reviving state. With per-connection sessions those
 * are unrepresentable — the old session's events and writes land on the old
 * session, which is closed.
 */
export interface TransportConnector {
  /**
   * Dial one connection. `signal` aborting must cancel the attempt: reject
   * the promise AND release whatever was being established — a connect that
   * "wins" after the caller gave up must not leave a live socket behind.
   */
  connect(options: ConnectOptions): Promise<TransportSession>;
}

export interface ConnectOptions {
  signal: AbortSignal;
}

/**
 * ONE physical connection.
 *
 * - `enqueue` resolves when the packet was accepted by THIS connection's
 *   outbound buffer, and rejects once the session is closed — an enqueue can
 *   never migrate to a different connection, because the session IS the
 *   connection.
 * - `subscribe` delivers this connection's inbound traffic and lifecycle.
 *   `close` is delivered exactly once; after it, no further events.
 * - `close()` is idempotent and terminal.
 */
export interface TransportSession {
  enqueue(packet: Packet): Promise<EnqueuedInfo>;
  subscribe(observer: TransportObserver): () => void;
  close(reason?: unknown): Promise<void>;
  isOpen(): boolean;
}

export interface TransportObserver {
  onMessage?(packet: Packet): void;
  onClose?(info?: unknown): void;
  /** Non-fatal, connection-scoped errors (protocol violations arrive here
   *  AND close the session). */
  onError?(error: unknown): void;
}
