import { Packet, type SentInfo, type Transport } from '../src/index.js';

export class FakeTransport implements Transport {
  readonly sent: Packet[] = [];
  readonly connectCalls: number[] = [];
  readonly closeCalls: number[] = [];

  sendShouldFail: Error | null = null;
  connectShouldFail: Error | null = null;

  private _isConnected = false;
  private readonly messageHandlers: Array<(p: Packet) => void> = [];
  private readonly messageSentHandlers: Array<(info: SentInfo) => void> = [];
  private readonly closeHandlers: Array<(ev?: unknown) => void> = [];
  private readonly errorHandlers: Array<(e: unknown) => void> = [];

  async connect(): Promise<void> {
    this.connectCalls.push(Date.now());
    if (this.connectShouldFail) throw this.connectShouldFail;
    this._isConnected = true;
  }

  async send(packet: Packet): Promise<void> {
    if (this.sendShouldFail) throw this.sendShouldFail;
    this.sent.push(packet);
    const info: SentInfo = { messageId: packet.messageId };
    for (const cb of this.messageSentHandlers) cb(info);
  }

  async close(): Promise<void> {
    this.closeCalls.push(Date.now());
    this._isConnected = false;
  }

  isConnected(): boolean {
    return this._isConnected;
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

  fireMessage(packet: Packet): void {
    for (const cb of this.messageHandlers) cb(packet);
  }
  fireClose(ev?: unknown): void {
    this._isConnected = false;
    for (const cb of this.closeHandlers) cb(ev);
  }
  fireError(e: unknown): void {
    for (const cb of this.errorHandlers) cb(e);
  }
}
