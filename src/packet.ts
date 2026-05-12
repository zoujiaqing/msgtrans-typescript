import {
  FIXED_HEADER_LEN,
  MAX_EXT_HEADER_LEN,
  MAX_PAYLOAD_LEN,
  PROTOCOL_VERSION,
} from './constants.js';
import {
  IncompletePacketError,
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

export const ReservedFlag = {
  Fragmented: 0x0001,
  HighPriority: 0x0002,
  HasRouteTag: 0x0004,
} as const;

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

  encode(): Uint8Array {
    if (this.extHeader.length > MAX_EXT_HEADER_LEN) {
      throw new WireFormatError(
        `ext_header too large: ${this.extHeader.length} > ${MAX_EXT_HEADER_LEN}`,
      );
    }
    if (this.payload.length > MAX_PAYLOAD_LEN) {
      throw new WireFormatError(
        `payload too large: ${this.payload.length} > ${MAX_PAYLOAD_LEN}`,
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

  static decode(bytes: Uint8Array): Packet {
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
    const compression = bytes[1]! as CompressionType;
    const packetType = bytes[2]! as PacketType;
    const bizType = bytes[3]!;
    const messageId = view.getUint32(4, false);
    const extHeaderLen = view.getUint16(8, false);
    const payloadLen = view.getUint32(10, false);
    const reserved = view.getUint16(14, false);

    const expected = FIXED_HEADER_LEN + extHeaderLen + payloadLen;
    if (bytes.length < expected) {
      throw new IncompletePacketError(
        `packet incomplete: have ${bytes.length}, need ${expected}`,
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
