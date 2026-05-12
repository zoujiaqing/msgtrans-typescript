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

export interface SentInfo {
  messageId: number;
}

export interface Transport {
  connect(): Promise<void>;
  send(packet: Packet): Promise<void>;
  close(): Promise<void>;
  isConnected(): boolean;
  onMessage(cb: (packet: Packet) => void): void;
  onMessageSent(cb: (info: SentInfo) => void): void;
  onClose(cb: (event?: unknown) => void): void;
  onError(cb: (error: unknown) => void): void;
}
