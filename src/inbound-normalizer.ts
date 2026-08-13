import { CompressionType, type DecodeLimits, type Packet } from './packet.js';
import { TransportError } from './errors.js';
import type { CodecRegistry } from './codec.js';

export interface InboundQueueCaps {
  /** Max packets queued behind an in-flight decompression. */
  maxFrames: number;
  /** Max total queued payload bytes. */
  maxBytes: number;
}

/**
 * The single inbound normalization point — the TS mapping of Rust 2.0's
 * event-pipe boundary, where every adapter's inbound traffic is decompressed
 * ONCE so consumers only ever see plaintext with a self-consistent header.
 *
 * Decompression is async (platform streams), and browser `message` events
 * fire in arrival order but complete work in completion order — so without
 * serialization, an early compressed packet would overtake nothing but BE
 * overtaken by a later plain one, reordering responses against pushes. All
 * inbound packets therefore pass through one serial promise chain: delivery
 * order is arrival order, always.
 *
 * A packet that cannot be normalized (no codec for its algorithm, corrupt
 * body, decompressed size over the limit) is FATAL: the `fatal` callback
 * fires (the owner closes the connection) and nothing is delivered — the
 * application never receives compressed bytes it would mistake for plaintext.
 */
export class InboundNormalizer {
  private chain: Promise<void> = Promise.resolve();
  /** Packets queued or in flight on the chain. */
  private depth = 0;
  /** Total payload bytes queued or in flight. */
  private queuedBytes = 0;
  /**
   * Once a packet fails normalization the STREAM is poisoned: everything
   * already queued behind it (and anything that arrives later) is dropped.
   * The peer and this client no longer agree on the protocol, so packets
   * after the violation are not trustworthy — delivering them would hand
   * the application a stream with a hole in it.
   */
  private poisoned = false;

  constructor(
    private readonly registry: CodecRegistry,
    private readonly limits: DecodeLimits,
    private readonly caps: InboundQueueCaps = {
      maxFrames: 8192,
      maxBytes: 64 * 1024 * 1024,
    },
  ) {}

  push(
    packet: Packet,
    deliver: (packet: Packet) => void,
    fatal: (error: unknown) => void,
  ): void {
    // Fast path: nothing queued and nothing to undo — deliver
    // SYNCHRONOUSLY. Arrival order is trivially preserved (the chain is
    // empty), plain traffic pays no async hop, and consumers keep the
    // synchronous emit the pre-0.2 client gave them. The moment anything
    // is in flight, everything queues behind it, so a plain packet can
    // never overtake an earlier compressed one.
    if (this.poisoned) return;
    if (this.depth === 0 && packet.compression === CompressionType.None) {
      deliver(packet);
      return;
    }
    // BOUNDED: a peer that stalls this side inside a slow decompression
    // while continuing to send cannot grow the queue without limit. The
    // browser cannot pause the underlying read, but it CAN refuse to buffer
    // past a cap — overflow closes the connection.
    if (
      this.depth + 1 > this.caps.maxFrames ||
      this.queuedBytes + packet.payload.length > this.caps.maxBytes
    ) {
      this.poisoned = true;
      fatal(
        new TransportError(
          `inbound normalization queue overflow (${this.depth} frames, ${this.queuedBytes} bytes)`,
        ),
      );
      return;
    }
    this.depth += 1;
    this.queuedBytes += packet.payload.length;
    const size = packet.payload.length;
    this.chain = this.chain.then(async () => {
      if (this.poisoned) {
        this.depth -= 1;
        this.queuedBytes -= size;
        return;
      }
      let normalized: Packet;
      try {
        normalized = await this.normalize(packet);
      } catch (e) {
        this.depth -= 1;
        this.queuedBytes -= size;
        this.poisoned = true;
        fatal(e);
        return;
      }
      this.depth -= 1;
      this.queuedBytes -= size;
      deliver(normalized);
    });
  }

  private async normalize(packet: Packet): Promise<Packet> {
    if (packet.compression === CompressionType.None) return packet;
    const codec = this.registry.require(packet.compression);
    packet.payload = await codec.decompress(
      packet.payload,
      this.limits.maxPayloadLen,
    );
    // Clear the flag it acted on: the header must not still claim a
    // compression that was undone.
    packet.compression = CompressionType.None;
    return packet;
  }
}
