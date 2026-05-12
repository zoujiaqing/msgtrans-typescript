// E2E against the dedicated WS-only `e2e_echo_server` (fast smoke).
import { afterAll, beforeAll } from 'vitest';
import { spawnDedicatedEchoServer, type SpawnedServer } from './setup.js';
import { defineEchoSuite } from './echo-suite.js';

const PORT = 18080;
let server: SpawnedServer;

beforeAll(async () => {
  server = await spawnDedicatedEchoServer(PORT);
});

afterAll(async () => {
  if (server) await server.stop();
});

defineEchoSuite('E2E (dedicated) — Rust e2e_echo_server ↔ TS TransportClient', {
  url: () => `ws://127.0.0.1:${PORT}`,
});
