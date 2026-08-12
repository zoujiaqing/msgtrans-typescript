import type { Packet } from './packet.js';

export const TransportState = {
  Idle: 'idle',
  Connecting: 'connecting',
  Open: 'open',
  Closing: 'closing',
  Closed: 'closed',
} as const;
export type TransportState =
  (typeof TransportState)[keyof typeof TransportState];

/**
 * Emitted when a packet has been ACCEPTED by the underlying socket's outbound
 * buffer. Deliberately named "enqueued", not "sent": a browser WebSocket
 * offers no write completion, so nothing at this layer can prove the bytes
 * reached the network. (Rust 2.0's `send` is write-confirmed; this tier is
 * the honest browser equivalent, and pretending otherwise would recreate the
 * fake write confirmations 2.0 was built to kill.)
 */
export interface EnqueuedInfo {
  messageId: number;
}

export interface Transport {
  connect(): Promise<void>;
  send(packet: Packet): Promise<void>;
  close(): Promise<void>;
  isConnected(): boolean;
  onMessage(cb: (packet: Packet) => void): void;
  onMessageEnqueued(cb: (info: EnqueuedInfo) => void): void;
  onClose(cb: (event?: unknown) => void): void;
  onError(cb: (error: unknown) => void): void;
}
