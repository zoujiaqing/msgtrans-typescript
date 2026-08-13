import type { TransportClient } from './transport-client.js';
import {
  requireAttempts,
  requireFiniteAtLeast,
  requireNonNegativeFinite,
  requireRatio,
} from './validate.js';

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
  /**
   * Backoff jitter, 0..1. Each delay is multiplied by
   * `1 - jitterRatio * random()`, spreading a fleet's reconnects instead of
   * synchronizing a thundering herd on the same schedule. Default 0.2;
   * 0 disables (deterministic delays, e.g. for tests).
   */
  jitterRatio?: number;
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
 * - This is THE retry policy. `TransportClientOptions.connectRetry` defaults
 *   to zero retries; leave it there when using this manager — stacking both
 *   multiplies attempt counts and wait times.
 */
export class ReconnectManager {
  private readonly client: TransportClient;
  private readonly opts: Required<Omit<ReconnectOptions, 'onStateChange'>>;
  /** A close observed while a cycle was running: re-examined when it ends. */
  private pendingRerun = false;
  private readonly onStateChange: (phase: ReconnectPhase, attempt: number) => void;

  private active = false;
  private cycleRunning = false;
  /**
   * Lifecycle epoch: bumped by every start() and stop(). A cycle belongs to
   * the epoch it started in — after stop();start() during its backoff sleep,
   * the OLD cycle wakes into a mismatched epoch and abandons itself instead
   * of zombie-connecting under the new lifecycle's `active` flag.
   */
  private epoch = 0;
  private unsubscribe: (() => void) | null = null;

  constructor(client: TransportClient, options: ReconnectOptions = {}) {
    this.client = client;
    this.opts = {
      initialDelayMs: requireNonNegativeFinite(
        'initialDelayMs',
        options.initialDelayMs ?? 1_000,
      ),
      maxDelayMs: requireNonNegativeFinite(
        'maxDelayMs',
        options.maxDelayMs ?? 30_000,
      ),
      multiplier: requireFiniteAtLeast('multiplier', options.multiplier ?? 2, 1),
      maxAttempts: requireAttempts('maxAttempts', options.maxAttempts ?? Infinity),
      jitterRatio: requireRatio('jitterRatio', options.jitterRatio ?? 0.2),
    };
    this.onStateChange = options.onStateChange ?? (() => {});
  }

  get isActive(): boolean {
    return this.active;
  }

  start(): void {
    if (this.active) return;
    this.active = true;
    this.epoch += 1;
    this.unsubscribe = this.client.on('close', () => {
      void this.runCycle();
    });
  }

  stop(): void {
    this.active = false;
    this.epoch += 1;
    if (this.unsubscribe) {
      this.unsubscribe();
      this.unsubscribe = null;
    }
  }

  private async runCycle(): Promise<void> {
    if (!this.active) return;
    if (this.cycleRunning) {
      // A close landing while a cycle unwinds is NOT swallowed: it is
      // re-examined the moment the cycle ends. The old behavior dropped it,
      // reported `connected`, and left the client down forever.
      this.pendingRerun = true;
      return;
    }
    this.cycleRunning = true;
    const epoch = this.epoch;
    try {
      let delay = this.opts.initialDelayMs;
      for (let attempt = 1; attempt <= this.opts.maxAttempts; attempt++) {
        this.onStateChange('reconnecting', attempt);
        await sleep(this.jittered(delay));
        if (this.epoch !== epoch) return; // stopped/restarted during backoff
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
      if (this.pendingRerun) {
        this.pendingRerun = false;
        if (this.epoch === epoch && this.active && !this.client.isConnected()) {
          void this.runCycle();
        }
      }
    }
  }

  private jittered(delay: number): number {
    return delay * (1 - this.opts.jitterRatio * Math.random());
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
