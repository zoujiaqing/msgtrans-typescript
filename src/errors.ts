export class WireFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WireFormatError';
  }
}

export class UnsupportedVersionError extends WireFormatError {
  readonly version: number;
  constructor(version: number) {
    super(`unsupported protocol version: ${version}`);
    this.name = 'UnsupportedVersionError';
    this.version = version;
  }
}

export class IncompletePacketError extends WireFormatError {
  constructor(message: string) {
    super(message);
    this.name = 'IncompletePacketError';
  }
}

export class TransportError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TransportError';
  }
}

/**
 * Thrown by `send()` when the WebSocket's outbound buffer
 * (`bufferedAmount`) is above the configured high-water mark. The socket
 * is NOT closed — the connection stays healthy; the caller should back
 * off (drop, queue, or retry later) instead of piling more bytes onto a
 * link that isn't draining.
 */
export class BackpressureError extends TransportError {
  /** Bytes currently sitting in the socket's outbound buffer. */
  readonly bufferedAmount: number;
  /** The high-water mark that was exceeded. */
  readonly maxBufferedBytes: number;
  constructor(bufferedAmount: number, maxBufferedBytes: number) {
    super(
      `outbound buffer over high-water mark: bufferedAmount=${bufferedAmount} > max=${maxBufferedBytes}`,
    );
    this.name = 'BackpressureError';
    this.bufferedAmount = bufferedAmount;
    this.maxBufferedBytes = maxBufferedBytes;
  }
}

export class UnsupportedCompressionError extends WireFormatError {
  readonly compression: number;
  constructor(compression: number) {
    super(
      `compression ${compression} is not implemented in @msgtrans/client v0.1; only CompressionType.None is supported`,
    );
    this.name = 'UnsupportedCompressionError';
    this.compression = compression;
  }
}
