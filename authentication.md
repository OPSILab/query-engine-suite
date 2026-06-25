---
layout: default
title: Authentication
---

# Authentication

The system supports JWT-based authentication.

---

## Supported Providers

- Keycloak
- Any JWT provider

---

## Configuration

Public key must be defined in `config.js`

## Flow

```mermaid
sequenceDiagram
  participant Client
  participant QueryEngine
  participant JWTValidator

  Client->>QueryEngine: Request with JWT
  QueryEngine->>JWTValidator: Validate token
  JWTValidator-->>QueryEngine: OK / Reject
  QueryEngine-->>Client: Response
```