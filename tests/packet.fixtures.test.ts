import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  CompressionType,
  FIXED_HEADER_LEN,
  Packet,
  PacketType,
} from '../src/index.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURES_DIR = join(__dirname, '..', 'fixtures');

interface FixtureJson {
  version: number;
  compression: number;
  packet_type: number;
  biz_type: number;
  message_id: number;
  ext_header_hex: string;
  payload_hex: string;
  reserved: number;
}

function hexDecode(s: string): Uint8Array {
  if (s.length % 2 !== 0) {
    throw new Error(`hex length must be even: ${s.length}`);
  }
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) {
    const hi = parseInt(s[i * 2]!, 16);
    const lo = parseInt(s[i * 2 + 1]!, 16);
    if (Number.isNaN(hi) || Number.isNaN(lo)) {
      throw new Error(`invalid hex at offset ${i * 2}: ${s.slice(i * 2, i * 2 + 2)}`);
    }
    out[i] = (hi << 4) | lo;
  }
  return out;
}

function listBinFixtures(): string[] {
  return readdirSync(FIXTURES_DIR)
    .filter((f) => f.endsWith('.bin'))
    .sort();
}

function buildFromSpec(spec: FixtureJson): Packet {
  return new Packet({
    version: spec.version,
    compression: spec.compression as CompressionType,
    packetType: spec.packet_type as PacketType,
    bizType: spec.biz_type,
    messageId: spec.message_id,
    extHeader: hexDecode(spec.ext_header_hex),
    payload: hexDecode(spec.payload_hex),
    reserved: spec.reserved,
  });
}

describe('FIXED_HEADER_LEN', () => {
  it('is exactly 16 bytes', () => {
    expect(FIXED_HEADER_LEN).toBe(16);
    const empty = new Packet({
      packetType: PacketType.OneWay,
      messageId: 0,
    });
    expect(empty.encode().length).toBe(16);
  });
});

describe('fixture batch 1 — directory layout', () => {
  it('has at least 8 fixtures with prefixes 01_..08_', () => {
    const bins = listBinFixtures();
    expect(bins.length).toBeGreaterThanOrEqual(8);
    for (let n = 1; n <= 8; n++) {
      const prefix = String(n).padStart(2, '0') + '_';
      expect(
        bins.some((b) => b.startsWith(prefix)),
        `missing fixture with prefix ${prefix}`,
      ).toBe(true);
    }
  });
});

describe('fixture batch 1 — round-trip', () => {
  const bins = listBinFixtures();

  for (const binFile of bins) {
    const name = binFile.replace(/\.bin$/, '');
    it(`round-trips ${name}`, () => {
      const binPath = join(FIXTURES_DIR, binFile);
      const jsonPath = join(FIXTURES_DIR, `${name}.json`);

      const binBytes = new Uint8Array(readFileSync(binPath));
      const spec = JSON.parse(
        readFileSync(jsonPath, 'utf8'),
      ) as FixtureJson;

      // Direction 1: decode .bin, fields match .json
      const parsed = Packet.decodeExact(binBytes);
      expect(parsed.version).toBe(spec.version);
      expect(parsed.compression).toBe(spec.compression);
      expect(parsed.packetType).toBe(spec.packet_type);
      expect(parsed.bizType).toBe(spec.biz_type);
      expect(parsed.messageId).toBe(spec.message_id);
      expect(parsed.reserved).toBe(spec.reserved);

      const expectedExt = hexDecode(spec.ext_header_hex);
      const expectedPayload = hexDecode(spec.payload_hex);
      expect(Array.from(parsed.extHeader)).toEqual(Array.from(expectedExt));
      expect(Array.from(parsed.payload)).toEqual(Array.from(expectedPayload));

      // Direction 2: rebuild from .json, encode == .bin byte-for-byte
      const rebuilt = buildFromSpec(spec);
      const rebuiltBytes = rebuilt.encode();
      expect(Array.from(rebuiltBytes)).toEqual(Array.from(binBytes));

      // Total length consistency
      expect(binBytes.length).toBe(
        FIXED_HEADER_LEN + expectedExt.length + expectedPayload.length,
      );
    });
  }
});
