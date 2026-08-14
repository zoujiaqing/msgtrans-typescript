import { Packet, PacketType } from './packet.js';
import {
  IdExhaustedError,
  RequestTimeoutError,
  TransportError,
} from './errors.js';
import type { TransportSession } from './transport.js';

const DEFAULT_TIMEOUT_MS = 10_000;
const U32_MAX = 0xffffffff;

export interface RequestManagerOptions {
  /** Default request timeout in milliseconds. Default: 10_000. */
  defaultTimeoutMs?: number;
  /** Fired after every successful enqueue (send / request / response). */
  onEnqueued?: (info: { messageId: number }) => void;
  /**
   * Starting value of the messageId counter (1..0xffffffff, 0 is reserved).
   * INTERNAL — exists so tests can reach the exhaustion boundary without
   * 2^32 allocations. Not exposed through `TransportClientOptions`: ids
   * belong to the transport.
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

/**
 * INTERNAL request lifecycle owner — the TS mapping of Rust 2.0's
 * `RequestRegistry`. Not part of the public API: message ids are allocated
 * here and nowhere else, so no caller-numbered `Request` can complete a
 * different waiter that happened to draw the same id.
 */
export class RequestManager {
  private readonly link: TransportSession;
  private readonly onEnqueued: (info: { messageId: number }) => void;
  private readonly defaultTimeoutMs: number;
  private readonly initialMessageId: number;
  private readonly pending = new Map<number, PendingEntry>();
  private counter: number;

  constructor(link: TransportSession, options: RequestManagerOptions = {}) {
    this.link = link;
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.initialMessageId = normalizeCounterStart(
      options.initialMessageId ?? 1,
    );
    this.counter = this.initialMessageId;
    this.onEnqueued = options.onEnqueued ?? (() => {});
    // NO transport subscriptions: a manager belongs to ONE session and the
    // session's owner resets it. Subscribing here would leak one listener
    // per reconnect (the Transport SPI has no unsubscribe) and re-create the
    // shared-mutable coupling the session model removes.
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  /**
   * Send a one-way packet. Always allocates the id — callers cannot number
   * their own packets through this path.
   */
  async send(packet: Packet): Promise<void> {
    packet.messageId = this.nextMessageId();
    await this.link.enqueue(packet);
    this.onEnqueued({ messageId: packet.messageId });
  }

  /**
   * INTERNAL respond path: ship a Response that must reuse its originating
   * Request's id. The only place a pre-numbered packet is accepted, and it
   * only accepts Responses — a pre-numbered Request is unrepresentable.
   */
  async sendResponse(packet: Packet): Promise<void> {
    if (packet.packetType !== PacketType.Response) {
      throw new TransportError(
        'sendResponse only accepts Response packets',
      );
    }
    await this.link.enqueue(packet);
    this.onEnqueued({ messageId: packet.messageId });
  }

  /**
   * Send a Request packet and resolve when the matching Response arrives.
   *
   * Lifecycle, in order:
   * 1. Register the pending entry FIRST — a transport that loops back
   *    synchronously must find it.
   * 2. Enqueue. A rejection removes the entry and rejects the caller.
   * 3. Arm the response timer only AFTER the enqueue succeeds: the timeout
   *    measures the peer's response time, not this side's enqueue time. The
   *    old order started the clock before `send()` was even called, so a
   *    slow async transport could "time out" a request that had not been
   *    sent — and then send it anyway, minting an orphan response.
   * 4. If the response arrived before the enqueue promise resolved, there is
   *    no entry left and no timer is armed.
   */
  request(packet: Packet, options: RequestOptions = {}): Promise<Packet> {
    const timeoutMs = options.timeoutMs ?? this.defaultTimeoutMs;
    let messageId: number;
    try {
      messageId = this.nextMessageId();
    } catch (e) {
      return Promise.reject(e);
    }
    packet.packetType = PacketType.Request;
    packet.messageId = messageId;

    return new Promise<Packet>((resolve, reject) => {
      const entry: PendingEntry = { resolve, reject, timer: null };
      this.pending.set(messageId, entry);

      this.link.enqueue(packet).then(
        () => {
          this.onEnqueued({ messageId });
          if (this.pending.get(messageId) !== entry) return; // already resolved
          if (timeoutMs > 0 && Number.isFinite(timeoutMs)) {
            entry.timer = setTimeout(() => {
              if (this.pending.delete(messageId)) {
                reject(new RequestTimeoutError(messageId, timeoutMs));
              }
            }, timeoutMs);
          }
        },
        (sendError) => {
          if (this.pending.get(messageId) !== entry) return;
          this.pending.delete(messageId);
          reject(sendError);
        },
      );
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
   * transport close; callers may also invoke before reconnect.
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
    // NEVER wraps within a generation: responses are matched on the bare
    // u32, so a wrapped id could complete a different, still-pending
    // request. Exhaustion is an explicit error; a reconnect starts a fresh
    // generation via `startGeneration()`.
    if (this.counter > U32_MAX) {
      throw new IdExhaustedError();
    }
    const id = this.counter;
    this.counter += 1;
    return id;
  }
}

function normalizeCounterStart(n: number): number {
  const u32 = n >>> 0;
  return u32 === 0 ? 1 : u32;
}
