export {
  Packet,
  PacketType,
  CompressionType,
  ReservedFlag,
  DEFAULT_DECODE_LIMITS,
  normalizeDecodeLimits,
} from './packet.js';
export type { PacketInit, DecodeLimits } from './packet.js';

export { TransportState } from './transport.js';
export type {
  ConnectOptions,
  EnqueuedInfo,
  TransportCloseEvent,
  TransportConnector,
  TransportObserver,
  TransportSession,
} from './transport.js';

export {
  WebSocketConnector,
  DEFAULT_MAX_BUFFERED_BYTES,
} from './websocket-transport.js';
export type {
  WebSocketLike,
  WebSocketFactory,
  WebSocketConnectorOptions,
  TextFramePolicy,
} from './websocket-transport.js';

export { HeartbeatMonitor } from './heartbeat.js';
export type { HeartbeatOptions } from './heartbeat.js';

export { ReconnectManager } from './reconnect.js';
export type { ReconnectOptions, ReconnectPhase } from './reconnect.js';

export { CodecRegistry, ZlibCodec } from './codec.js';
export type { CompressionCodec } from './codec.js';

export { TransportClient } from './transport-client.js';
export type { ClientMessage, ClientRequest } from './transport-client.js';
export type {
  TransportClientOptions,
  ConnectRetryOptions,
  SendOptions,
  RequestOptions,
} from './transport-client.js';

export {
  WireFormatError,
  UnsupportedVersionError,
  IncompletePacketError,
  UnknownWireValueError,
  DecodeLimitError,
  TransportError,
  BackpressureError,
  QueueOverflowError,
  UnsupportedCompressionError,
  IdExhaustedError,
  RequestTimeoutError,
} from './errors.js';

export {
  FIXED_HEADER_LEN,
  PROTOCOL_VERSION,
  WIRE_MAX_EXT_HEADER_LEN,
  WIRE_MAX_PAYLOAD_LEN,
  DEFAULT_MAX_PAYLOAD_LEN,
  DEFAULT_MAX_EXT_HEADER_LEN,
} from './constants.js';
