#!/usr/bin/env node
/**
 * Thin wrapper around the local `ng` binary that only adds
 * --openssl-legacy-provider when the running Node actually supports it.
 *
 * Angular 11's build tooling (webpack 4) breaks under Node 17+ because
 * OpenSSL 3 dropped the MD4 hash algorithm webpack 4 uses for chunk ids
 * ("error:0308010C:digital envelope routines::unsupported"). The fix is
 * the --openssl-legacy-provider flag - but that flag was only introduced
 * in Node 17, and on older Node it isn't a recognized option at all, so
 * putting it in NODE_OPTIONS unconditionally fails there with:
 *   "--openssl-legacy-provider is not allowed in NODE_OPTIONS"
 * (that's the exact error you hit - your local Node is older than 17).
 *
 * This script checks process.versions.node itself and only sets the flag
 * when it's actually needed, so the same "npm start"/"npm run build"
 * works whether you're on an old Node (matching the original ngx-admin
 * template's era) or a current one.
 */
const { spawnSync } = require('child_process');
const path = require('path');

const [major] = process.versions.node.split('.').map(Number);
const env = { ...process.env };

if (major >= 17) {
  env.NODE_OPTIONS = [env.NODE_OPTIONS, '--openssl-legacy-provider'].filter(Boolean).join(' ');
}

const ngBin = path.join(
  __dirname,
  '..',
  'node_modules',
  '.bin',
  process.platform === 'win32' ? 'ng.cmd' : 'ng'
);

const result = spawnSync(ngBin, process.argv.slice(2), {
  stdio: 'inherit',
  env,
  shell: process.platform === 'win32',
});

process.exit(result.status === null ? 1 : result.status);
