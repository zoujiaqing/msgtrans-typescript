// E2E against the official multi-protocol `echo_server` running in passive mode.
// Verifies the @msgtrans/client TS SDK works against the *real* Rust server
// (TCP / WebSocket / QUIC bound simultaneously, just like a production gateway).
import { afterAll, beforeAll } from 'vitest';
import { spawnOfficialEchoServer, type SpawnedServer } from './setup.js';
import { defineEchoSuite } from './echo-suite.js';

const TCP_PORT = 18091;
const WS_PORT = 18092;
const QUIC_PORT = 18093;

let server: SpawnedServer;

beforeAll(async () => {
  server = await spawnOfficialEchoServer({
    tcpPort: TCP_PORT,
    wsPort: WS_PORT,
    quicPort: QUIC_PORT,
  });
});

afterAll(async () => {
  if (server) await server.stop();
});

defineEchoSuite(
  'E2E (official) — Rust echo_server (passive, TCP+WS+QUIC) ↔ TS TransportClient',
  {
    url: () => `ws://127.0.0.1:${WS_PORT}`,
  },
);
