---
layout: default
title: Testing
---

# Testing

The backends run their tests against real, throwaway databases started with Docker; the frontend needs nothing.

| Project | Databases (`docker-compose.test.yml`) | Commands |
|---|---|---|
| Source-Connector | MongoDB 4.4 on **27018**, PostgreSQL 16 on **5434** | `npm run test:db`, `npm test`, `npm run test:db:down` |
| Query-Engine | MongoDB 4.4 on **27019** | the same |
| Frontend | — | `npm test` (Vitest), `npm run test:types` |

```bash
cd Source-Connector
npm run test:db      # starts the databases and waits until they are ready
npm test             # node --test "test/**/*.test.js"
npm run test:db:down # removes them
```

On one line: `npm run test:db && npm test && npm run test:db:down` (PowerShell 7 or bash; Windows PowerShell 5.1
does not support `&&`: use `;` or separate commands).

- The ports are chosen not to clash with a local MongoDB (27017) or PostgreSQL (5432 / 5433). The test databases are
  separate containers: they never touch yours.
- Each test file uses its own database (`qes_test_<file>` in the Source-Connector, `qes_qe_test_<file>` in the
  Query-Engine), dropped at start.
- Without the databases the tests that need them are cancelled, not failed: start them first.
- The same tests run in the GitHub workflows `tests-source-connector.yml`, `tests-query-engine.yml`, `tests-frontend.yml`.

## The HTTP client (`utils/httpClient.js`)

The Source-Connector and the Query-Engine make their HTTP requests with `utils/httpClient.js` (the same file in both),
no dependencies, the same shape as axios for what they use: `get` / `post` / `put` / `patch` / `delete` / `request` /
`getUri`, params (also repeated: `paramsSerializer: { indexes: null }`), timeout, agents, redirects, decompression,
interceptors, errors with `response`, `status`, `code`; `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` through the agents
of Node (`proxyEnv`, Node >= 22.21 / 24.5). `test/httpClient.test.js` sends the same requests with it and with axios
(a devDependency, for these tests only) to a local server and compares them; in the Source-Connector,
`test/requestLog.test.js` compares the statuslog documents too. Not as axios, on purpose: errors are `HttpError`
(`isHttpError`), `error.request` is `{ method, url }`, the User-Agent is `query-engine-suite`.

## Dependencies (npm audit)

`npm audit --omit=dev` checks only what runs in production. What is left on purpose:

- **Source-Connector, Query-Engine**: no axios in production (see above); the Dockerfiles install with
  `npm ci --omit=dev` (the devDependencies stay out of the image). nodemon is a dev dependency; `overrides.nodemon` forces a recent one also inside
  `percocologger`, which lists it as a dependency (remove the override when it no longer does). The dependencies of
  `minio` (`query-string`, `decode-uri-component`, `stream-json`): fixed only by breaking versions of `minio`, and the
  MinIO server is ours. Query-Engine: `@graphql-tools/utils`, inside `@apollo/server`, fixed only in its version 13.
- **Frontend**: nothing left (`npm audit`, dev tools included). Angular 22 needs Node `^22.22.3`, `^24.15.0` or
  `>=26`. Regenerate `package-lock.json` (a fresh `npm install` without lockfile) with npm 11 or later: npm 10 stops on
  the peer dependencies of the Angular builder and Vitest ("Cannot read properties of null (reading 'edgesOut')");
  `npm ci` and `npm install` with the lockfile work with both.
