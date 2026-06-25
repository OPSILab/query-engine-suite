
---

# Query Engine (`query-engine.md`)

```md
---
layout: default
title: Query Engine
---

# Query Engine

The Query Engine executes queries across multiple data sources and cached databases.

---

## Supported Sources

- MongoDB (cache layer)
- PostgreSQL (cache layer)
- MinIO (direct)
- Orion API (direct)
- External APIs (direct)

---

## Query Types

### GraphQL Queries
Structured queries over cached datasets.

---

### Advanced Search (Key-Value)

- Query nested objects
- Query array elements
- Supports GeoJSON date filters

Executed on:
> MongoDB cache only

---

### Simple Search (Keyword)

- Full-text search
- “Search everywhere” mode

Executed directly on:
> MinIO (currently only supported source)

---

### SQL Queries

Executed on:
> PostgreSQL only

---

## Execution Model

- Advanced + GraphQL → MongoDB
- SQL → PostgreSQL
- Simple Search → Direct source access