import type { TransportClient } from './transport-client.js';
import { requirePositiveFinite } from './validate.js';

/**
 * Application-layer heartbeat.
 *
 * msgtrans has NO protocol-level ping — a liveness probe is application
 * traffic by design (the wire stays payload-agnostic). This monitor owns the
 * scheduling and the verdict; the caller supplies the probe: any
 * request/response the peer is obliged to answer.
 */
export interface HeartbeatOptions {
  /**
   * The liveness probe. Called when the connection has been idle for
   * `intervalMs`; resolving counts as proof of life, rejecting (or exceeding
   * `timeoutMs`) declares the connection dead. Typically a cheap
   * request/response, e.g. `() => client.request(pingBytes, { timeoutMs })`.
   */
  probe: () => Promise<unknown>;
  /** Idle time before a probe fires. Default 30_000. */
  intervalMs?: number;
  /** Probe deadline. Default 10_000. */
  timeoutMs?: number;
  /**
   * Called when the probe fails — the connection is presumed dead. Default:
   * `client.disconnect()`, which lets a reconnect manager (or the app's own
   * close handling) take over. The monitor never reconnects by itself:
   * detection and recovery are separate jobs.
   */
  onDead?: (error: unknown) => void;
}

/**
 * Idle-based heartbeat: traffic in EITHER direction counts as activity, so a
 * busy connection is never probed — the probe exists to disambiguate
 * "quiet because idle" from "quiet because dead", and only the former needs
 * disambiguating.
 */
export class HeartbeatMonitor {
  private readonly client: TransportClient;
  private readonly probe: () => Promise<unknown>;
  private readonly intervalMs: number;
  private readonly timeoutMs: number;
  private readonly onDead: (error: unknown) => void;

  private lastActivity = 0;
  private timer: ReturnType<typeof setInterval> | null = null;
  private probing = false;
  private readonly unsubscribes: Array<() => void> = [];

  /**
   * Bumped by every stop(): an in-flight probe belongs to the epoch it was
   * launched in, and a verdict from a dead epoch is VOID — the old behavior
   * let a stale probe failure close whatever connection came next.
   */
  private epoch = 0;

  constructor(client: TransportClient, options: HeartbeatOptions) {
    this.client = client;
    this.probe = options.probe;
    this.intervalMs = requirePositiveFinite(
      'intervalMs',
      options.intervalMs ?? 30_000,
    );
    this.timeoutMs = requirePositiveFinite(
      'timeoutMs',
      options.timeoutMs ?? 10_000,
    );
    this.onDead =
      options.onDead ??
      (() => {
        void this.client.disconnect();
      });
  }

  get isRunning(): boolean {
    return this.timer !== null;
  }

  start(): void {
    if (this.timer) return;
    this.epoch += 1; // a fresh lifecycle owns a fresh probing flag
    this.probing = false;
    this.lastActivity = Date.now();
    const bump = () => {
      this.lastActivity = Date.now();
    };
    // Traffic in either direction is proof of life.
    this.unsubscribes.push(
      this.client.on('message', bump),
      this.client.on('request', bump),
      this.client.on('messageEnqueued', bump),
    );
    // The tick is a cheap idle check, not the probe itself; scheduling at
    // a fraction of the interval bounds probe latency to ~interval*1.25
    // without a per-activity timer reset.
    this.timer = setInterval(
      () => void this.tick(),
      Math.max(250, Math.floor(this.intervalMs / 4)),
    );
  }

  stop(): void {
    this.epoch += 1;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    for (const off of this.unsubscribes.splice(0)) off();
  }

  private async tick(): Promise<void> {
    if (this.probing) return;
    // A disconnected client has nothing to probe; reconnect logic (not
    // this monitor) owns that state. Activity resets so a fresh connection
    // gets a full idle interval before its first probe.
    if (!this.client.isConnected()) {
      this.lastActivity = Date.now();
      return;
    }
    if (Date.now() - this.lastActivity < this.intervalMs) return;

    this.probing = true;
    const epoch = this.epoch;
    try {
      await withDeadline(this.probe(), this.timeoutMs);
      // Success from a DEAD epoch is as void as failure: it must not bump
      // the restarted cycle's activity clock.
      if (epoch !== this.epoch) return;
      this.lastActivity = Date.now();
    } catch (e) {
      if (epoch !== this.epoch) return; // stale verdict: stop() ran meanwhile
      this.stop();
      this.onDead(e);
    } finally {
      // Only the CURRENT epoch owns the probing flag; a hung old probe's
      // eventual settle must not unblock (or keep blocked) the new cycle.
      if (epoch === this.epoch) this.probing = false;
    }
  }
}

class HeartbeatTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`heartbeat probe exceeded ${timeoutMs}ms`);
    this.name = 'HeartbeatTimeoutError';
  }
}

function withDeadline<T>(p: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new HeartbeatTimeoutError(timeoutMs)),
      timeoutMs,
    );
    p.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      },
    );
  });
}
