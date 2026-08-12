import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { connect } from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const TS_PKG_ROOT = resolve(__dirname, '..', '..');
const MSGTRANS_DIR = resolve(TS_PKG_ROOT, '..', 'msgtrans');
const READY_PREFIX = 'MSGTRANS_E2E_READY ';

export interface SpawnedServer {
  url: string;
  port: number;
  stop: () => Promise<void>;
}

export interface SpawnServerOptions {
  /** Cargo example name (e.g. `e2e_echo_server`, `echo_server`). */
  example: string;
  /** Env vars passed to the server. */
  env: Record<string, string>;
  /** WS port, used for the post-ready TCP connectability probe. */
  wsPort: number;
}

/**
 * Spawn a Rust example as a child process, wait for its ready line,
 * verify the WS port is connectable, and return a handle.
 *
 * Prefers a prebuilt binary at `target/debug/examples/<example>` if it exists
 * (fast path); otherwise falls back to `cargo run --example <example>`.
 */
export async function spawnServer(
  options: SpawnServerOptions,
): Promise<SpawnedServer> {
  if (!existsSync(MSGTRANS_DIR)) {
    throw new Error(
      `msgtrans crate not found at ${MSGTRANS_DIR}. ` +
        `E2E tests require the Rust workspace alongside this package.`,
    );
  }

  const prebuilt = join(
    MSGTRANS_DIR,
    'target',
    'debug',
    'examples',
    options.example,
  );
  const useBinary = existsSync(prebuilt);
  const cmd = useBinary ? prebuilt : 'cargo';
  const args = useBinary
    ? []
    : ['run', '--quiet', '--example', options.example, '--features', 'flate2'];

  const child = spawn(cmd, args, {
    cwd: MSGTRANS_DIR,
    env: { ...process.env, ...options.env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stderr?.on('data', (chunk: Buffer) => {
    process.stderr.write(`[${options.example} stderr] ${chunk.toString('utf8')}`);
  });

  const url = await waitForReadyLine(child, options.example);
  await waitForTcpConnectable(options.wsPort);

  const stop = (): Promise<void> => {
    if (child.exitCode !== null) return Promise.resolve();
    return new Promise<void>((finish) => {
      let killed = false;
      const onExit = (): void => {
        if (killed) return;
        killed = true;
        finish();
      };
      child.once('exit', onExit);
      child.kill('SIGINT');
      setTimeout(() => {
        if (child.exitCode === null) child.kill('SIGKILL');
      }, 2000);
    });
  };

  return { url, port: options.wsPort, stop };
}

/**
 * Spawn the dedicated, WS-only `e2e_echo_server` (fast smoke target).
 */
export function spawnDedicatedEchoServer(port: number): Promise<SpawnedServer> {
  return spawnServer({
    example: 'e2e_echo_server',
    env: { MSGTRANS_E2E_PORT: String(port) },
    wsPort: port,
  });
}

/**
 * Spawn the official multi-protocol `echo_server` in passive mode.
 * Binds TCP / WS / QUIC simultaneously; only the WS port is probed by tests.
 */
export function spawnOfficialEchoServer(opts: {
  tcpPort: number;
  wsPort: number;
  quicPort: number;
}): Promise<SpawnedServer> {
  return spawnServer({
    example: 'echo_server',
    env: {
      MSGTRANS_ECHO_MODE: 'passive',
      MSGTRANS_ECHO_TCP_PORT: String(opts.tcpPort),
      MSGTRANS_ECHO_WS_PORT: String(opts.wsPort),
      MSGTRANS_ECHO_QUIC_PORT: String(opts.quicPort),
    },
    wsPort: opts.wsPort,
  });
}

function waitForReadyLine(
  child: ChildProcess,
  label: string,
): Promise<string> {
  return new Promise<string>((resolveReady, reject) => {
    let buffer = '';
    const onData = (chunk: Buffer): void => {
      buffer += chunk.toString('utf8');
      let nl: number;
      while ((nl = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        if (line.startsWith(READY_PREFIX)) {
          child.stdout?.off('data', onData);
          child.off('exit', onExit);
          resolveReady(line.slice(READY_PREFIX.length).trim());
          return;
        }
      }
    };
    const onExit = (code: number | null): void => {
      child.stdout?.off('data', onData);
      reject(
        new Error(
          `${label} exited before printing ready line (exit code=${code})`,
        ),
      );
    };
    child.stdout?.on('data', onData);
    child.once('exit', onExit);
  });
}

async function waitForTcpConnectable(
  port: number,
  attempts = 50,
  intervalMs = 100,
): Promise<void> {
  for (let i = 0; i < attempts; i++) {
    try {
      await new Promise<void>((ok, fail) => {
        const sock = connect({ host: '127.0.0.1', port });
        sock.once('connect', () => {
          sock.end();
          ok();
        });
        sock.once('error', fail);
      });
      return;
    } catch {
      await new Promise((r) => setTimeout(r, intervalMs));
    }
  }
  throw new Error(
    `127.0.0.1:${port} did not become connectable in ${attempts * intervalMs}ms`,
  );
}
