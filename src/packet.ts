import {
  DEFAULT_MAX_EXT_HEADER_LEN,
  DEFAULT_MAX_PAYLOAD_LEN,
  FIXED_HEADER_LEN,
  PROTOCOL_VERSION,
  WIRE_MAX_EXT_HEADER_LEN,
  WIRE_MAX_PAYLOAD_LEN,
} from './constants.js';
import {
  DecodeLimitError,
  IncompletePacketError,
  UnknownWireValueError,
  UnsupportedVersionError,
  WireFormatError,
} from './errors.js';

export const PacketType = {
  OneWay: 0,
  Request: 1,
  Response: 2,
} as const;
export type PacketType = (typeof PacketType)[keyof typeof PacketType];

export const CompressionType = {
  None: 0,
  Zstd: 1,
  Zlib: 2,
} as const;
export type CompressionType =
  (typeof CompressionType)[keyof typeof CompressionType];

/**
 * Wire metadata bits in the `reserved` field.
 *
 * `HighPriority` and `HasRouteTag` are annotations a peer may read.
 * `Fragmented` describes a mechanism this client does not implement —
 * `WebSocketTransport.send()` refuses to send it rather than shipping a
 * promise the receiver cannot rely on.
 */
export const ReservedFlag = {
  Fragmented: 0x0001,
  HighPriority: 0x0002,
  HasRouteTag: 0x0004,
} as const;

/**
 * Frame-decoding caps, mirroring Rust 2.0's `DecodeLimits`.
 *
 * The frame cap is DERIVED (header + ext + payload), never set independently:
 * a max frame that disagreed with its parts would reject frames whose parts
 * were each individually legal. The default payload cap matches the Rust
 * decompression-bomb cap (16 MiB), so both peers agree on what fits.
 */
export interface DecodeLimits {
  readonly maxPayloadLen: number;
  readonly maxExtHeaderLen: number;
}

export const DEFAULT_DECODE_LIMITS: DecodeLimits = {
  maxPayloadLen: DEFAULT_MAX_PAYLOAD_LEN,
  maxExtHeaderLen: DEFAULT_MAX_EXT_HEADER_LEN,
};

/**
 * Validate caller-supplied limits and clamp them into what the wire format
 * can express. VALIDATE, not coerce: `NaN` makes every `>` comparison false,
 * which silently disables the decompression-bomb defense — an invalid limit
 * is a thrown error, never a quietly-open door.
 */
export function normalizeDecodeLimits(
  limits?: Partial<DecodeLimits>,
): DecodeLimits {
  const payload = requireNonNegativeInt(
    'maxPayloadLen',
    limits?.maxPayloadLen ?? DEFAULT_MAX_PAYLOAD_LEN,
  );
  const ext = requireNonNegativeInt(
    'maxExtHeaderLen',
    limits?.maxExtHeaderLen ?? DEFAULT_MAX_EXT_HEADER_LEN,
  );
  return {
    maxPayloadLen: Math.min(payload, WIRE_MAX_PAYLOAD_LEN),
    maxExtHeaderLen: Math.min(ext, WIRE_MAX_EXT_HEADER_LEN),
  };
}

function requireNonNegativeInt(field: string, value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new WireFormatError(
      `invalid ${field}: ${value} (must be a finite non-negative number)`,
    );
  }
  return Math.floor(value);
}

const EMPTY: Uint8Array = new Uint8Array(0);

export interface PacketInit {
  packetType: PacketType;
  messageId: number;
  bizType?: number;
  compression?: CompressionType;
  extHeader?: Uint8Array;
  payload?: Uint8Array;
  reserved?: number;
  version?: number;
}

export class Packet {
  version: number;
  compression: CompressionType;
  packetType: PacketType;
  bizType: number;
  messageId: number;
  reserved: number;
  extHeader: Uint8Array;
  payload: Uint8Array;

  constructor(init: PacketInit) {
    this.version = init.version ?? PROTOCOL_VERSION;
    this.compression = init.compression ?? CompressionType.None;
    this.packetType = init.packetType;
    this.bizType = (init.bizType ?? 0) & 0xff;
    this.messageId = (init.messageId ?? 0) >>> 0;
    this.reserved = (init.reserved ?? 0) & 0xffff;
    this.extHeader = init.extHeader ?? EMPTY;
    this.payload = init.payload ?? EMPTY;
  }

  static oneWay(messageId: number, payload: Uint8Array = EMPTY): Packet {
    return new Packet({ packetType: PacketType.OneWay, messageId, payload });
  }

  static request(messageId: number, payload: Uint8Array = EMPTY): Packet {
    return new Packet({ packetType: PacketType.Request, messageId, payload });
  }

  static response(messageId: number, payload: Uint8Array = EMPTY): Packet {
    return new Packet({ packetType: PacketType.Response, messageId, payload });
  }

  hasFlag(flag: number): boolean {
    return (this.reserved & flag) !== 0;
  }

  setFlag(flag: number, on = true): void {
    if (on) {
      this.reserved = (this.reserved | flag) & 0xffff;
    } else {
      this.reserved = this.reserved & ~flag & 0xffff;
    }
  }

  totalLength(): number {
    return FIXED_HEADER_LEN + this.extHeader.length + this.payload.length;
  }

  /**
   * Encode to wire bytes.
   *
   * Enforces the SAME caps the peer decodes with: without `limits` the
   * defaults apply, so this client cannot produce a frame a default-configured
   * Rust 2.0 server is guaranteed to close the connection over. (The old
   * behavior checked only the raw u16/u32 wire ceilings — up to 4 GiB.)
   */
  encode(limits: DecodeLimits = DEFAULT_DECODE_LIMITS): Uint8Array {
    if (this.extHeader.length > limits.maxExtHeaderLen) {
      throw new DecodeLimitError(
        `ext_header too large: ${this.extHeader.length} > ${limits.maxExtHeaderLen}`,
      );
    }
    if (this.payload.length > limits.maxPayloadLen) {
      throw new DecodeLimitError(
        `payload too large: ${this.payload.length} > ${limits.maxPayloadLen}`,
      );
    }
    // Wire ceilings are a hard floor under any configured limits.
    if (this.extHeader.length > WIRE_MAX_EXT_HEADER_LEN) {
      throw new WireFormatError(
        `ext_header exceeds wire format: ${this.extHeader.length}`,
      );
    }
    if (this.payload.length > WIRE_MAX_PAYLOAD_LEN) {
      throw new WireFormatError(
        `payload exceeds wire format: ${this.payload.length}`,
      );
    }

    const buf = new Uint8Array(this.totalLength());
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

    buf[0] = this.version & 0xff;
    buf[1] = this.compression & 0xff;
    buf[2] = this.packetType & 0xff;
    buf[3] = this.bizType & 0xff;
    view.setUint32(4, this.messageId >>> 0, false);
    view.setUint16(8, this.extHeader.length & 0xffff, false);
    view.setUint32(10, this.payload.length >>> 0, false);
    view.setUint16(14, this.reserved & 0xffff, false);

    buf.set(this.extHeader, FIXED_HEADER_LEN);
    buf.set(this.payload, FIXED_HEADER_LEN + this.extHeader.length);

    return buf;
  }

  /**
   * STRICT decode of exactly one packet.
   *
   * One WebSocket binary message carries exactly one packet, so this is the
   * only decode shape: trailing bytes mean the stream is desynchronized and
   * are a protocol error, not tolerated silence. Unknown packet-type or
   * compression bytes are protocol errors (`UnknownWireValueError`) instead
   * of being cast into bogus enum members. Declared lengths over the
   * configured `DecodeLimits` are rejected before any allocation.
   */
  static decodeExact(
    bytes: Uint8Array,
    limits: DecodeLimits = DEFAULT_DECODE_LIMITS,
  ): Packet {
    if (bytes.length < FIXED_HEADER_LEN) {
      throw new IncompletePacketError(
        `header too short: ${bytes.length} < ${FIXED_HEADER_LEN}`,
      );
    }

    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const version = bytes[0]!;
    if (version !== PROTOCOL_VERSION) {
      throw new UnsupportedVersionError(version);
    }
    const compressionByte = bytes[1]!;
    if (compressionByte > 2) {
      throw new UnknownWireValueError('compression', compressionByte);
    }
    const packetTypeByte = bytes[2]!;
    if (packetTypeByte > 2) {
      throw new UnknownWireValueError('packet_type', packetTypeByte);
    }
    const compression = compressionByte as CompressionType;
    const packetType = packetTypeByte as PacketType;
    const bizType = bytes[3]!;
    const messageId = view.getUint32(4, false);
    const extHeaderLen = view.getUint16(8, false);
    const payloadLen = view.getUint32(10, false);
    const reserved = view.getUint16(14, false);

    if (extHeaderLen > limits.maxExtHeaderLen) {
      throw new DecodeLimitError(
        `declared ext_header ${extHeaderLen} > limit ${limits.maxExtHeaderLen}`,
      );
    }
    if (payloadLen > limits.maxPayloadLen) {
      throw new DecodeLimitError(
        `declared payload ${payloadLen} > limit ${limits.maxPayloadLen}`,
      );
    }

    const expected = FIXED_HEADER_LEN + extHeaderLen + payloadLen;
    if (bytes.length < expected) {
      throw new IncompletePacketError(
        `packet incomplete: have ${bytes.length}, need ${expected}`,
      );
    }
    if (bytes.length > expected) {
      throw new WireFormatError(
        `trailing bytes after packet: have ${bytes.length}, frame is ${expected}`,
      );
    }

    const extStart = FIXED_HEADER_LEN;
    const extEnd = extStart + extHeaderLen;
    const payloadEnd = extEnd + payloadLen;
    const extHeader =
      extHeaderLen === 0 ? EMPTY : bytes.slice(extStart, extEnd);
    const payload = payloadLen === 0 ? EMPTY : bytes.slice(extEnd, payloadEnd);

    return new Packet({
      version,
      compression,
      packetType,
      bizType,
      messageId,
      reserved,
      extHeader,
      payload,
    });
  }
}
