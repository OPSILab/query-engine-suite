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
