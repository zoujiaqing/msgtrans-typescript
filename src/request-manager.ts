import { Packet, PacketType } from './packet.js';
import { TransportError } from './errors.js';
import type { Transport } from './transport.js';

const DEFAULT_TIMEOUT_MS = 10_000;
const U32_MAX = 0xffffffff;

export class RequestTimeoutError extends Error {
  readonly messageId: number;
  readonly timeoutMs: number;
  constructor(messageId: number, timeoutMs: number) {
    super(`request ${messageId} timed out after ${timeoutMs}ms`);
    this.name = 'RequestTimeoutError';
    this.messageId = messageId;
    this.timeoutMs = timeoutMs;
  }
}

export interface RequestManagerOptions {
  /** Default request timeout in milliseconds. Default: 10_000. */
  defaultTimeoutMs?: number;
  /**
   * Starting value of the messageId counter (1..0xffffffff, 0 is reserved).
   * Default: 1. Useful for testing wraparound or for advanced reconnect flows.
   */
  initialMessageId?: number;
}

export interface RequestOptions {
  /** Override the default timeout. Pass 0 to disable. */
  timeoutMs?: number;
}

interface PendingEntry {
  resolve: (packet: Packet) => void;
  reject: (error: unknown) => void;
  timer: ReturnType<typeof setTimeout> | null;
}

export class RequestManager {
  private readonly transport: Transport;
  private readonly defaultTimeoutMs: number;
  private readonly pending = new Map<number, PendingEntry>();
  private counter: number;

  constructor(transport: Transport, options: RequestManagerOptions = {}) {
    this.transport = transport;
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.counter = normalizeCounterStart(options.initialMessageId ?? 1);

    transport.onClose(() =>
      this.reset(new TransportError('transport closed')),
    );
    transport.onError((e) =>
      this.reset(e instanceof Error ? e : new TransportError(String(e))),
    );
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  /**
   * Send a packet through the transport without registering it for response.
   *
   * - If `packet.messageId === 0`, allocates the next id from the shared
   *   counter (so OneWay and Request share the same id space, per
   *   WIRE_FORMAT.md §5.5).
   * - Otherwise leaves the id intact (used when sending a `Response` that
   *   must reuse the originating Request's id).
   * - Mutates `packet.messageId` when allocating.
   */
  async send(packet: Packet): Promise<void> {
    if (packet.messageId === 0) {
      packet.messageId = this.nextMessageId();
    }
    await this.transport.send(packet);
  }

  /**
   * Send a Request packet and resolve when the matching Response arrives.
   *
   * Always sets `packetType = Request` and allocates a fresh messageId,
   * regardless of the packet's prior state. Mutates the input packet.
   *
   * If the transport's `send` rejects, the pending entry is cleaned up
   * and the returned promise rejects with the same error.
   */
  request(packet: Packet, options: RequestOptions = {}): Promise<Packet> {
    const timeoutMs = options.timeoutMs ?? this.defaultTimeoutMs;
    const messageId = this.nextMessageId();
    packet.packetType = PacketType.Request;
    packet.messageId = messageId;

    return new Promise<Packet>((resolve, reject) => {
      const timer =
        timeoutMs > 0 && Number.isFinite(timeoutMs)
          ? setTimeout(() => {
              if (this.pending.delete(messageId)) {
                reject(new RequestTimeoutError(messageId, timeoutMs));
              }
            }, timeoutMs)
          : null;

      this.pending.set(messageId, { resolve, reject, timer });

      this.transport.send(packet).catch((sendError) => {
        const entry = this.pending.get(messageId);
        if (!entry) return;
        if (entry.timer) clearTimeout(entry.timer);
        this.pending.delete(messageId);
        reject(sendError);
      });
    });
  }

  /**
   * Inspect an incoming packet:
   *   - If it's a Response matching a pending request, resolve that request
   *     and return `true` (the packet has been consumed).
   *   - Otherwise return `false` so the caller can dispatch normally.
   */
  handleIncoming(packet: Packet): boolean {
    if (packet.packetType !== PacketType.Response) return false;
    const entry = this.pending.get(packet.messageId);
    if (!entry) return false;
    this.pending.delete(packet.messageId);
    if (entry.timer) clearTimeout(entry.timer);
    entry.resolve(packet);
    return true;
  }

  /**
   * Reject all pending requests with the given reason. Auto-invoked on
   * transport close/error; callers may also invoke before reconnect.
   */
  reset(reason: unknown = new TransportError('request manager reset')): void {
    if (this.pending.size === 0) return;
    const entries = Array.from(this.pending.values());
    this.pending.clear();
    for (const entry of entries) {
      if (entry.timer) clearTimeout(entry.timer);
      entry.reject(reason);
    }
  }

  private nextMessageId(): number {
    const id = this.counter;
    // Wrap u32: ...fffe → ...ffff → 1 (skip 0 so id=0 stays "unset").
    this.counter = this.counter >= U32_MAX ? 1 : this.counter + 1;
    return id;
  }
}

function normalizeCounterStart(n: number): number {
  const u32 = (n >>> 0);
  return u32 === 0 ? 1 : u32;
}
