import type { TransportClient } from './transport-client.js';

export type ReconnectPhase = 'reconnecting' | 'connected' | 'gave-up';

export interface ReconnectOptions {
  /** Delay before the first reconnect attempt. Default 1_000. */
  initialDelayMs?: number;
  /** Backoff cap. Default 30_000. */
  maxDelayMs?: number;
  /** Per-attempt multiplier. Default 2. */
  multiplier?: number;
  /** Give up after this many consecutive failures. Default Infinity. */
  maxAttempts?: number;
  /** Observability hook; `attempt` is 1-based within the current outage. */
  onStateChange?: (phase: ReconnectPhase, attempt: number) => void;
}

/**
 * Automatic reconnection with exponential backoff.
 *
 * Contract, stated bluntly because every reconnect bug in this project has
 * been a contract ambiguity:
 *
 * - While the handle is ACTIVE, every `close` event triggers a reconnect
 *   cycle. An intentional disconnect must call `stop()` FIRST — the
 *   transport cannot tell a wanted close from an unwanted one.
 * - One cycle runs at a time; a `close` arriving mid-cycle is absorbed
 *   (the cycle is already doing the right thing).
 * - Backoff resets after a successful `connect()`, and the pending requests
 *   of the old connection are NOT replayed — the transport rejected them at
 *   close (pending state rides the connection); replay policy belongs to
 *   the application layer, which knows what is idempotent.
 * - `stop()` during a backoff sleep abandons the cycle at the next wake.
 */
export class ReconnectManager {
  private readonly client: TransportClient;
  private readonly opts: Required<Omit<ReconnectOptions, 'onStateChange'>>;
  private readonly onStateChange: (phase: ReconnectPhase, attempt: number) => void;

  private active = false;
  private cycleRunning = false;
  private unsubscribe: (() => void) | null = null;

  constructor(client: TransportClient, options: ReconnectOptions = {}) {
    this.client = client;
    this.opts = {
      initialDelayMs: options.initialDelayMs ?? 1_000,
      maxDelayMs: options.maxDelayMs ?? 30_000,
      multiplier: options.multiplier ?? 2,
      maxAttempts: options.maxAttempts ?? Infinity,
    };
    this.onStateChange = options.onStateChange ?? (() => {});
  }

  get isActive(): boolean {
    return this.active;
  }

  start(): void {
    if (this.active) return;
    this.active = true;
    this.unsubscribe = this.client.on('close', () => {
      void this.runCycle();
    });
  }

  stop(): void {
    this.active = false;
    if (this.unsubscribe) {
      this.unsubscribe();
      this.unsubscribe = null;
    }
  }

  private async runCycle(): Promise<void> {
    if (!this.active || this.cycleRunning) return;
    this.cycleRunning = true;
    try {
      let delay = this.opts.initialDelayMs;
      for (let attempt = 1; attempt <= this.opts.maxAttempts; attempt++) {
        this.onStateChange('reconnecting', attempt);
        await sleep(delay);
        if (!this.active) return; // stopped during backoff
        try {
          await this.client.connect();
          this.onStateChange('connected', attempt);
          return;
        } catch {
          delay = Math.min(this.opts.maxDelayMs, delay * this.opts.multiplier);
        }
      }
      this.onStateChange('gave-up', this.opts.maxAttempts);
      this.stop();
    } finally {
      this.cycleRunning = false;
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
