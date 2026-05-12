#!/usr/bin/env node
import { copyFileSync, existsSync, mkdirSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SRC = join(__dirname, '..', '..', 'msgtrans', 'tests', 'fixtures');
const DST = join(__dirname, '..', 'fixtures');

if (!existsSync(SRC)) {
  console.error(`source fixtures dir not found: ${SRC}`);
  console.error(
    `regenerate via: cd ../msgtrans && cargo run --example regenerate_fixtures`,
  );
  process.exit(1);
}

mkdirSync(DST, { recursive: true });

const files = readdirSync(SRC)
  .filter((f) => f.endsWith('.bin') || f.endsWith('.json'))
  .sort();

for (const f of files) {
  copyFileSync(join(SRC, f), join(DST, f));
}

console.log(`Synced ${files.length} fixture files`);
console.log(`  from: ${SRC}`);
console.log(`  to:   ${DST}`);
