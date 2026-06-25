---
layout: default
title: Source Connector
---

# Source Connector

The Source Connector is responsible for data ingestion and synchronization.

---

## Responsibilities

- Fetch data from:
  - MinIO
  - Orion
  - External APIs

- Populate:
  - MongoDB (query cache)
  - PostgreSQL (structured cache)

---

## Critical Rule

At least one source must be enabled in `config.js`:

- MinIO
- Orion
- External API

If none are enabled:

- MongoDB stays empty
- PostgreSQL stays empty
- Query Engine returns no results

---

## Data Flow

```mermaid
graph LR
  A[External Sources] --> B[Source Connector]
  B --> C[MongoDB]
  B --> D[PostgreSQL]