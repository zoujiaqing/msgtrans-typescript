#!/usr/bin/env node
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const rustDir = resolve(process.env.MSGTRANS_RUST_DIR ?? join(__dirname, '..', '..', 'msgtrans'));
const relativeCandidates = [['crates', 'msgtrans'], []];
const SRC = relativeCandidates
  .map((parts) => join(rustDir, ...parts, 'tests', 'fixtures'))
  .find((path) => existsSync(path));
const DST = join(__dirname, '..', 'fixtures');

if (!SRC) {
  console.error(`source fixtures dir not found: ${rustDir}`);
  console.error(
    `regenerate via: cd ../msgtrans && cargo run --example regenerate_fixtures`,
  );
  process.exit(1);
}

mkdirSync(DST, { recursive: true });

const files = readdirSync(SRC)
  .filter((f) => f.endsWith('.bin') || f.endsWith('.json'))
  .sort();

const check = process.argv.includes('--check');
const localFiles = readdirSync(DST).filter((f) => f.endsWith('.bin') || f.endsWith('.json')).sort();
if (check && (files.length === 0 || files.join("\n") !== localFiles.join("\n"))) {
  throw new Error('fixture file list differs from the Rust candidate');
}
for (const f of files) {
  const from = join(SRC, f);
  const to = join(DST, f);
  if (check) {
    if (!readFileSync(from).equals(readFileSync(to))) throw new Error(`fixture differs: ${f}`);
  } else {
    copyFileSync(from, to);
  }
}

console.log(`${check ? 'Verified' : 'Synced'} ${files.length} fixture files`);
console.log(`  from: ${SRC}`);
console.log(`  to:   ${DST}`);
