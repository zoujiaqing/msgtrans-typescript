import { CompressionType } from './packet.js';
import { DecodeLimitError, UnsupportedCompressionError } from './errors.js';

/**
 * A compression algorithm implementation.
 *
 * ONE registry serves both directions, so the API can never end up
 * asymmetric (able to decompress an algorithm it cannot compress, or vice
 * versa). `limit` bounds the OUTPUT size: `decompress` must stop and throw
 * as soon as the produced bytes exceed it — never inflate fully and check
 * afterwards, which is exactly how a decompression bomb wins.
 */
export interface CompressionCodec {
  readonly type: CompressionType;
  compress(input: Uint8Array, limit: number): Promise<Uint8Array>;
  decompress(input: Uint8Array, limit: number): Promise<Uint8Array>;
}

export class CodecRegistry {
  private readonly codecs = new Map<CompressionType, CompressionCodec>();

  constructor(codecs: readonly CompressionCodec[] = []) {
    for (const codec of codecs) this.register(codec);
  }

  register(codec: CompressionCodec): void {
    if (codec.type === CompressionType.None) {
      throw new UnsupportedCompressionError(codec.type);
    }
    this.codecs.set(codec.type, codec);
  }

  get(type: CompressionType): CompressionCodec | undefined {
    return this.codecs.get(type);
  }

  /** The codec, or throw — used by send/receive paths that must not proceed. */
  require(type: CompressionType): CompressionCodec {
    const codec = this.codecs.get(type);
    if (!codec) throw new UnsupportedCompressionError(type);
    return codec;
  }
}

type StreamCtor = new (format: string) => {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
};

function requireStream(name: 'CompressionStream' | 'DecompressionStream'): StreamCtor {
  const ctor = (globalThis as Record<string, unknown>)[name] as
    | StreamCtor
    | undefined;
  if (!ctor) {
    throw new UnsupportedCompressionError(CompressionType.Zlib);
  }
  return ctor;
}

/**
 * Zlib (RFC 1950) via the platform's native `CompressionStream` /
 * `DecompressionStream('deflate')` — the Web Streams `'deflate'` format IS
 * zlib (deflate with the zlib wrapper), matching Rust's `flate2` Zlib codec.
 * Available in all modern browsers and Node ≥ 18; no wasm, no dependency.
 */
export class ZlibCodec implements CompressionCodec {
  readonly type = CompressionType.Zlib;

  async compress(input: Uint8Array, limit: number): Promise<Uint8Array> {
    const Ctor = requireStream('CompressionStream');
    return pumpThrough(new Ctor('deflate'), input, limit, 'compressed');
  }

  async decompress(input: Uint8Array, limit: number): Promise<Uint8Array> {
    const Ctor = requireStream('DecompressionStream');
    return pumpThrough(new Ctor('deflate'), input, limit, 'decompressed');
  }
}

/**
 * Push `input` through a transform stream, accumulating the output and
 * cancelling the moment it exceeds `limit`. The cancel is the bomb defense:
 * the transform stops producing instead of inflating gigabytes first.
 */
async function pumpThrough(
  stream: {
    readonly readable: ReadableStream<Uint8Array>;
    readonly writable: WritableStream<Uint8Array>;
  },
  input: Uint8Array,
  limit: number,
  direction: string,
): Promise<Uint8Array> {
  const writer = stream.writable.getWriter();
  const writing = (async () => {
    await writer.write(input);
    await writer.close();
  })();
  // A structurally invalid input surfaces on BOTH promises; the read loop's
  // rejection is the one we report, so the writer's twin must not become an
  // unhandled rejection.
  writing.catch(() => {});

  const reader = stream.readable.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel();
      throw new DecodeLimitError(
        `${direction} size exceeds limit: > ${limit} bytes`,
      );
    }
    chunks.push(value);
  }
  await writing;

  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}
