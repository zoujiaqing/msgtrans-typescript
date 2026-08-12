export const PROTOCOL_VERSION = 1;

export const FIXED_HEADER_LEN = 16;

/** Wire-format ceiling for `ext_header_len` — the field is a u16. */
export const WIRE_MAX_EXT_HEADER_LEN = 0xffff;

/** Wire-format ceiling for `payload_len` — the field is a u32. */
export const WIRE_MAX_PAYLOAD_LEN = 0xffffffff;

/**
 * Default maximum decoded payload per frame: 16 MiB.
 *
 * Mirrors the Rust 2.0 `DecodeLimits` default (the decompression-bomb cap),
 * enforced there identically by TCP / WebSocket / QUIC. The old TS default
 * was the raw wire ceiling (4 GiB), so this client could happily encode a
 * frame the server was guaranteed to kill the connection over.
 */
export const DEFAULT_MAX_PAYLOAD_LEN = 16 * 1024 * 1024;

/** Default maximum ext-header per frame: the u16 wire ceiling. */
export const DEFAULT_MAX_EXT_HEADER_LEN = WIRE_MAX_EXT_HEADER_LEN;
