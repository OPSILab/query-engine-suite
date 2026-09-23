#!/usr/bin/env node
/**
 * Thin wrapper around the local `ng` binary.
 *
 * It used to add --openssl-legacy-provider on Node 17+, because Angular 11's
 * build tooling was webpack 4, which hashes chunk ids with MD4 - an algorithm
 * OpenSSL 3 dropped ("error:0308010C:digital envelope routines::unsupported").
 * That workaround is obsolete since the upgrade: Angular 12+ builds on
 * webpack 5, which hashes with xxhash64 and never touches the OpenSSL
 * provider. The flag is gone, and with it the Node-version sniffing that only
 * existed to avoid passing it on older Node where it isn't a valid option.
 *
 * The script itself stays so that `npm start` / `npm run build` keep working
 * unchanged, and so there's one obvious place to put the next wrapper hack if
 * one is ever needed again.
 */
const { spawnSync } = require('child_process');
const path = require('path');

const ngBin = path.join(
  __dirname,
  '..',
  'node_modules',
  '.bin',
  process.platform === 'win32' ? 'ng.cmd' : 'ng'
);

const result = spawnSync(ngBin, process.argv.slice(2), {
  stdio: 'inherit',
  shell: process.platform === 'win32',
});

process.exit(result.status === null ? 1 : result.status);
