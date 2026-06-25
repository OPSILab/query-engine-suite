---
layout: default
title: Architecture
---

# System Architecture

```mermaid
graph TD
  A[Source Connector] --> B[MongoDB Cache]
  A --> C[PostgreSQL Cache]

  B --> D[Query Engine]
  C --> D[Query Engine]

  D --> E[MinIO]
  D --> F[Orion API]
  D --> G[External APIs]