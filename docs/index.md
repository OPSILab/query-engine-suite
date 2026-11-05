---
layout: default
title: Home
---

# Query Engine Suite

The Query Engine Suite collects data from several sources into one **Data Space** and lets users search it from a
single page. It is made of three parts, plus an external service:

| Part | What it does |
|---|---|
| **Source-Connector** | Collects the data: files uploaded to MinIO, Orion datasets (through the Data Model Mapper) and external APIs. Writes them to MongoDB and PostgreSQL, and logs every HTTP request it makes. |
| **Query-Engine** | Answers the queries: Simple search, Advanced search, SQL, GraphQL. Keeps the cache of the GraphQL datapoints queries and serves the status log. |
| **Frontend** | The Angular app: the query page, the status log, and the management pages of the Source-Connector and of the cache. |
| Data Model Mapper (separate repository) | Maps or parses the Orion datasets for the Source-Connector, streaming the result part by part. |

## Pages

- [Architecture](architecture.html): the components and how data flows between them.
- [Query Engine](query-engine.html): the four kinds of query and the endpoints.
  - [Advanced search paging](advanced-search-paging.html): reading every result, page by page, with or without unwrap.
  - [Query cache](cache.html): **versions, backups and deactivation**, three different things.
  - [Status log](status-log.html): the HTTP requests of the Source-Connector, failed and successful.
- [Source Connector](source-connector.html): connectors, collections, rebuild.
  - [Orion notifications](orion-notifications.html): the queue, the streamed mapping with the Data Model Mapper, recovery.
  - [Incremental APIs](incremental-apis.html): date windows, pagination, interrupted polls, reset.
- [Frontend](frontend.html): the pages, the three login modes, `config.json`.
- [Authentication](authentication.html): tokens, 401 / 403, roles, admin token, management password.
- [Configuration](configuration.html): the keys added or changed recently, in one place.
- [Testing](testing.html): the test databases and how to run the tests.
- [Docker](docker.html)

## Key features

- Simple search (a word everywhere), Advanced search (key / value filters, date ranges, unwrap of arrays), SQL, GraphQL.
- Results chosen per collection (`api`, `orion`, `minio`), paged with no limit on the total.
- Versioned cache of the GraphQL datapoints queries, with backups that protect versions from automatic deletion.
- Orion notifications processed one at a time, with a visible queue.
- Incremental APIs polled in date windows, never stored twice, resettable with a log of every reset.
- Status log of every request of the Source-Connector, with credentials hidden from non-administrators.
- Keycloak (OIDC / JWT) authentication, optional on the frontend.
