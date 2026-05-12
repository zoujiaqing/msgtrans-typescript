import { describe, expect, it } from 'vitest';
import WS from 'ws';
import {
  RequestTimeoutError,
  TransportClient,
  TransportError,
  type WebSocketLike,
} from '../../src/index.js';

const BIZ_DROP = 250;

export interface EchoSuiteContext {
  /** Returns the WS URL the test should connect to. Re-evaluated per test. */
  url: () => string;
}

export function defineEchoSuite(label: string, ctx: EchoSuiteContext): void {
  describe(label, () => {
    function createClient(): TransportClient {
      return new TransportClient({
        url: ctx.url(),
        webSocketFactory: (u, p) =>
          new WS(u, p) as unknown as WebSocketLike,
      });
    }

    it('Request returns identical payload bytes (byte-for-byte)', async () => {
      const client = createClient();
      await client.connect();

      const input = new TextEncoder().encode(
        'byte-for-byte echo 你好 🌐 \x00\x01\x02',
      );
      const out = await client.request(input, { bizType: 5 });
      expect(Array.from(out)).toEqual(Array.from(input));

      await client.close();
    });

    it('OneWay echoes back via on("message") with same biz_type and payload', async () => {
      const client = createClient();
      await client.connect();

      const input = new TextEncoder().encode(`oneway-${Date.now()}`);
      const received: { bizType: number; data: Uint8Array }[] = [];
      client.on('message', (m) => {
        received.push({ bizType: m.bizType, data: m.data });
      });

      await client.send(input, { bizType: 42 });

      await waitFor(() => received.length >= 1, 2000, 'oneway echo');
      expect(received).toHaveLength(1);
      expect(received[0]!.bizType).toBe(42);
      expect(Array.from(received[0]!.data)).toEqual(Array.from(input));

      await client.close();
    });

    it('100 concurrent requests resolve with their matching payloads', async () => {
      const client = createClient();
      await client.connect();

      const N = 100;
      const inputs = Array.from({ length: N }, (_, i) =>
        new TextEncoder().encode(`concurrent-${i}-${Math.random()}`),
      );
      const responses = await Promise.all(
        inputs.map((p, i) =>
          client.request(p, { bizType: ((i % 200) + 1) as number }),
        ),
      );
      for (let i = 0; i < N; i++) {
        expect(Array.from(responses[i]!)).toEqual(Array.from(inputs[i]!));
      }

      await client.close();
    });

    it('Request rejects with RequestTimeoutError when server drops biz_type=250', async () => {
      const client = createClient();
      await client.connect();

      await expect(
        client.request(new TextEncoder().encode('void'), {
          bizType: BIZ_DROP,
          timeoutMs: 200,
        }),
      ).rejects.toBeInstanceOf(RequestTimeoutError);

      await client.close();
    });

    it('close() rejects all pending requests with TransportError', async () => {
      const client = createClient();
      await client.connect();

      const promise = client.request(new TextEncoder().encode('pending'), {
        bizType: BIZ_DROP,
        timeoutMs: 30_000,
      });

      await new Promise((r) => setTimeout(r, 50));
      await client.close();

      await expect(promise).rejects.toBeInstanceOf(TransportError);
    });
  });
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error(`waitFor(${label}) timed out after ${timeoutMs}ms`);
}
