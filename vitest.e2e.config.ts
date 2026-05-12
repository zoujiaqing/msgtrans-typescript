import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/e2e/**/*.test.ts'],
    environment: 'node',
    pool: 'forks',
    poolOptions: {
      forks: { singleFork: true },
    },
    testTimeout: 30_000,
    // beforeAll may need to run `cargo build` on first run.
    hookTimeout: 180_000,
  },
});
