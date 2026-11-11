---
layout: default
title: Source Connector
---

# Source Connector

The Source-Connector collects the data of the Data Space and writes it to MongoDB and PostgreSQL. Every endpoint is
under `/api`.

## Connectors

Enabled in `sourceConnectors` (at least one, or the Query-Engine finds nothing):

| Connector | Source | Details |
|---|---|---|
| `minioConnector` | JSON, CSV and GeoJSON files in MinIO | synchronised at startup (unless `doNotSyncAtStart`), every `syncInterval` and by `PUT /api/query` |
| `orionConnector` | Orion `DistributionDCAT-AP` entities: each one points to a dataset, mapped by the Data Model Mapper | [Orion notifications](orion-notifications.html) |
| `apiConnector` | the APIs of `apiConnectorConfig.apiUrls`, polled every `pollInterval` | [Incremental APIs](incremental-apis.html) |

Where each connector writes is set by `collections` (same values in the Query-Engine config):

```js
collections: {
  api:   { mongo: "sources",    toMongo: true, postgres: "sources",    toPostgres: true },
  orion: { mongo: "datapoints", toMongo: true, postgres: "datapoints", toPostgres: false },
  minio: { mongo: "minio",      toMongo: true, toPostgres: true }   // one PostgreSQL table per bucket
}
```

`queryOptions.advancedSearch: false` stops every MongoDB write, `queryOptions.SQLQuery: false` every PostgreSQL write,
whatever `toMongo` / `toPostgres` say. Moving from the old single `sources` collection: `scripts/migrateCollections.js`.

## Non incremental APIs

An API without `incrementalParams` returns, at every poll, a list that may repeat what was already stored. Each record
stored gets `datePolled`; the answer is compared with the records already stored for its url (by content: every field
except `_id`, `datePolled` and the flags below, whatever the order of the keys):

1. **By order.** The answer is taken as the continuation of the stored records, like a window sliding over the data,
   oldest first: its first record is looked for among the stored ones; from that occurrence to the end, the stored
   records must be the beginning of the answer (and not longer than it). Then only the rest of the answer is stored,
   flagged `likelyIncremental: true`. An occurrence that does not fit: the next one. This also catches a repeated record
   that went out of the window while an identical one came in.
2. **Otherwise** (records shuffled, replaced, or removed here and there), as configured in `unalignedAnswer` on the
   API (or for every API in `apiConnectorConfig`):

| `unalignedAnswer` | What is stored |
|---|---|
| `"upsert"` | the records of the url replaced by the answer |
| `"append"` | the whole answer appended, flagged `unverifiedAppend: true` (maybe duplicates, interval unknown) |
| not set (default) | by count: of each record, only the copies beyond those already stored (3 stored, 4 in the answer: 1) |

With `upsertRecords: true` (on the API, or in `apiConnectorConfig`) none of this applies: every answer replaces the
records of the url, as it always did. The first answer of a url is stored whole, duplicates included. For a paginated API all the pages are read first, then
compared. An answer that begins with the last stored record counts as a continuation even if they have only that
record in common.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /api/orion/subscribe/:mapID`, `POST /api/orion/subscribe` | Orion notification (queued); **open**, no token |
| `GET /api/queue` | the Orion notifications waiting or being processed |
| `GET /api/config` | what can be shown of the configuration (connectors enabled, poll interval, APIs: url, paginated / batch, incremental, start date, maxWindow); **open**, nothing secret |
| `PUT /api/query` | synchronise MinIO and check the Orion entities now |
| `POST /api/rebuild`, `GET /api/rebuild` | rebuild keys / values / entries or PostgreSQL; progress |
| `POST /api/incremental/reset`, `GET /api/incremental/reset` | reset incremental APIs; progress |
| `GET /api/incremental/reset/log` | every reset done |

All of them except the Orion notifications and `GET /api/config` require a valid token, unless `authConfig.disableAuth` is true (Orion
cannot send one: see [Orion notifications](orion-notifications.html#no-token-on-the-notification-route)). The reset also requires the
[management password](authentication.html#management-password).

## Management page

The frontend page **Source connector** (logged users only) talks to these endpoints directly, using
`sourceConnectorBaseUrl` of its `config.json`.

![Source connector page](assets/img/source-connector.png)

### Rebuild keys, values and entries

The suggestions of the Advanced search (keys, values, key/value entries) are kept up to date while data is written. If
they drift (old data, a change of `datapointsNotIndexed`, an interrupted write), rebuild them:

```
POST /api/rebuild?mode=entries&connector=orion
```

| Parameter | Values |
|---|---|
| `mode` (required) | `entries`: keys / values / entries from the MongoDB collection; `postgres`: the PostgreSQL table becomes a mirror of the MongoDB collection (only `toPostgres` connectors); `all`: both |
| `connector` | `api` or `orion` (default both). MinIO is rebuilt by its synchronisation, not here |
| `origin` | only the records of this url (requires `connector`) |
| `keepLegacy` | `true`: don't remove the old keys / values / entries without connector |

It runs in background (202); `GET /api/rebuild` gives the state and the result. The records themselves are not
changed. Refused (409) while another rebuild, a reset or an API poll is running.

### Reset of the incremental APIs

Deletes every record of the chosen incremental APIs and polls them again from the start. See
[Incremental APIs](incremental-apis.html#reset).

![Reset confirmation](assets/img/source-connector-reset-confirm.png)

### Orion notifications in queue

Shows the notification being processed and the waiting ones, refreshing every 3 seconds while the queue is not empty.
See [Orion notifications](orion-notifications.html#the-queue).

## Status log

Every HTTP request of the Source-Connector is recorded: see [Status log](status-log.html).
