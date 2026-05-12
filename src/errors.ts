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
