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

/**
 * An unknown wire byte where an enum is expected (packet type, compression).
 *
 * Mirrors Rust 2.0's `TryFrom<u8>` decode: an unrecognized value is a protocol
 * error, never silently accepted as a bogus enum member the way the old
 * unchecked `as PacketType` cast did.
 */
export class UnknownWireValueError extends WireFormatError {
  readonly field: string;
  readonly value: number;
  constructor(field: string, value: number) {
    super(`unknown wire value for ${field}: ${value}`);
    this.name = 'UnknownWireValueError';
    this.field = field;
    this.value = value;
  }
}

/**
 * A structurally valid frame that exceeds the configured `DecodeLimits`
 * (payload, ext header, or decompressed size). A protocol-level violation:
 * under strict semantics the connection is closed, matching the Rust side.
 */
export class DecodeLimitError extends WireFormatError {
  constructor(message: string) {
    super(message);
    this.name = 'DecodeLimitError';
  }
}

export class TransportError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TransportError';
  }
}

/**
 * Thrown by `send()` when the packet would push the WebSocket's outbound
 * buffer (`bufferedAmount` + this frame) over the configured high-water mark.
 * The socket is NOT closed — the connection stays healthy; the caller should
 * back off (drop, queue, or retry later) instead of piling more bytes onto a
 * link that isn't draining.
 */
export class BackpressureError extends TransportError {
  /** Bytes currently sitting in the socket's outbound buffer. */
  readonly bufferedAmount: number;
  /** The high-water mark that would be exceeded. */
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

/**
 * No codec is available for the requested compression algorithm.
 *
 * On SEND this rejects the call — a raw payload is never shipped under a
 * compressed header. On RECEIVE it is fatal: the peer is speaking an algorithm
 * this client cannot undo, so the connection is closed rather than handing the
 * application compressed bytes it would mistake for plaintext.
 */
export class UnsupportedCompressionError extends WireFormatError {
  readonly compression: number;
  constructor(compression: number) {
    super(
      `no codec registered for compression ${compression}; ` +
        `register one via TransportClientOptions.codecs`,
    );
    this.name = 'UnsupportedCompressionError';
    this.compression = compression;
  }
}

/**
 * The per-connection message-id counter reached the u32 ceiling.
 *
 * Ids never repeat within a connection generation (responses are matched on
 * the bare u32, so a wrapped id could complete someone else's request — the
 * ABA the Rust 2.0 registry closed). A reconnect starts a fresh generation
 * with a fresh counter.
 */
export class IdExhaustedError extends TransportError {
  constructor() {
    super('message-id space exhausted for this connection generation');
    this.name = 'IdExhaustedError';
  }
}

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
