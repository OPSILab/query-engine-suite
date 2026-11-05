---
layout: default
title: Docker
---

# Docker Deployment

Each part has its own Docker Compose files:

| Folder | Files | Content |
|---|---|---|
| `Query-Engine` | `docker-compose.yml`, `docker-compose.prod.yml` | Query-Engine, MongoDB, PostgreSQL, MinIO |
| `Source-Connector` | `docker-compose.yml`, `docker-compose.dev.yml`, `docker-compose.prod.yml` | Source-Connector, MongoDB, PostgreSQL |
| `frontend` | `docker-compose.yml` (nginx, `nginx.conf`) | the frontend |
| `Query-Engine`, `Source-Connector` | `docker-compose.test.yml` | throwaway databases for the [tests](testing.html) only |

## Example

```bash
cd Source-Connector
docker compose up -d
```

## Checklist

- Query-Engine and Source-Connector point to the same MongoDB and PostgreSQL, with the same `collections`.
- `managementSecret` has the same value in both configurations (or is empty in both: destructive operations disabled).
- The frontend `assets/config.json` has `queryEngineBaseUrl` and `sourceConnectorBaseUrl` reachable **from the
  browser**, and `dashboardBaseURL` registered as redirect url in Keycloak.
- `orion.notificationUrl` is reachable **from Orion** (e.g. `http://host.docker.internal:3000/api/orion/subscribe/<mapID>`).
- The Data Model Mapper has `sessionLocation.filesystem` or `sessionLocation.mongo` set to `true`.
- Run a single Source-Connector instance: the [Orion queue](orion-notifications.html#the-queue) is in memory.
