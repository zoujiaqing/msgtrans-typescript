// `@msgtrans/client/experimental` — APIs whose contract is not yet frozen.
//
// WebTransport lives here until a real Rust WebTransport server exists to
// run interop E2E against: the framing mirrors the Rust QUIC adapter by
// construction, but a contract nobody has exercised across the wire is a
// candidate, not a promise. Root-API stability rules do not apply.
export { WebTransportConnector } from './webtransport-transport.js';
export type {
  WebTransportLike,
  WebTransportFactory,
  WebTransportConnectorOptions,
} from './webtransport-transport.js';
