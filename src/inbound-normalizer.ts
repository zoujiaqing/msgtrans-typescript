import { CompressionType, type DecodeLimits, type Packet } from './packet.js';
import type { CodecRegistry } from './codec.js';

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

  constructor(
    private readonly registry: CodecRegistry,
    private readonly limits: DecodeLimits,
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
    if (this.depth === 0 && packet.compression === CompressionType.None) {
      deliver(packet);
      return;
    }
    this.depth += 1;
    this.chain = this.chain.then(async () => {
      let normalized: Packet;
      try {
        normalized = await this.normalize(packet);
      } catch (e) {
        this.depth -= 1;
        fatal(e);
        return;
      }
      this.depth -= 1;
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
