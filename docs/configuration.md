---
layout: default
title: Configuration
---

# Configuration

Each backend reads `config.js` (template: `config.template.js`). Secrets (`authConfig.secret`, `adminToken`,
`managementSecret`, `hideFieldsSecret`, database passwords, MinIO keys) must never be committed. Below, the keys most deployments touch,
and those added recently; the templates comment every key.

## Shared by both backends

These must have the **same value** in the Query-Engine and in the Source-Connector:

| Key | Purpose |
|---|---|
| `collections` | MongoDB collection / PostgreSQL table of each connector, and whether it is written there ([details](source-connector.html#connectors)) |
| `managementSecret` | the [management password](authentication.html#management-password); empty: destructive operations disabled |
| `hideFieldsSecret` | Source-Connector and Query-Engine (same value): the key of the pseudonyms of the [hidden fields](source-connector.html#hidden-fields-personal-data) (`hideFields` on an API); long and random, never committed |
| `apiBackup.mongoDb` / `apiBackup.maxBackups` | Source-Connector: the MongoDB database of the [backups of the API data](source-connector.html#backups-of-the-api-data) (empty: `<main database>_backups`) and the most kept (10; 0: no limit) |
| `mongo`, `postgreConfig` | the same databases |
| `authConfig.clientId`, `publicKey`, `idmHost`, `authRealm` | the same Keycloak client |

The two backends can also read **one `config.js`** (in containers, each one with its own copy, or on the same
machine). Their keys do not clash, except the port: `port` is the port of both (3000), each in its own container.
On the same machine, give each its own: `sourceConnectorPort` and `queryEnginePort` win over `port` (not set:
`port`, as before).

| Key | Default | Purpose |
|---|---|---|
| `port` | `3000` | the port of the backend (of both, with one config.js) |
| `sourceConnectorPort` | `port` | the port of the Source-Connector: its own notification route too (recovery check), and `orion.notificationUrl` must use it |
| `queryEnginePort` | `port` | the port of the Query-Engine |

## Query-Engine

| Key | Default | Purpose |
|---|---|---|
| `authConfig.disableAuth` | `false` | skip the token check ([consequences](authentication.html#backends-with-authentication-disabled)) |
| `authConfig.adminRoles` | `[]` | roles of an administrator; empty: every logged user |
| `authConfig.adminToken` | `""` | with `disableAuth`: the `X-Admin-Token` that opens the cache management |
| `authConfig.userInfoEndpoint` | | returns `{ pilot, username, email }` for the token |
| `queryOptions.defaultCollections` | `["api", "minio"]` | collections searched when the request does not say |
| `queryOptions.advancedSearchMaxResults` | `1000` | highest Advanced search page |
| `queryOptions.suggestionsMaxResults` | `500` | highest suggestions page |
| `queryOptions.mongoMaxTimeMS` | `900000` | MongoDB query time limit (504 beyond) |
| `cache.keepVersions` | `3` | [versions kept per query](cache.html#pruning-keepversions), active included |
| `cache.maxDatapoints` | `5000000` | cache size limit; `0`: none |
| `statusLog.ignoredUrls` / `ignoredPrefixes` | `[]` | [ignored urls](status-log.html#ignored-urls) that cannot be listed again from the frontend |
| `simpleSearchOptions` | | live sources of the Simple search (`minio`, `api`, `orion`), pages, timeouts |

## Source-Connector

| Key | Default | Purpose |
|---|---|---|
| `sourceConnectors` | all `true` | which connectors run |
| `getMapEndpoint`, `mapEndpoint`, `parseEndpoint` | | the Data Model Mapper |
| `sessionEndpoint`, `reportEndpoint`, `feedbackEndpoint` | `…/api/output?`, `…/api/reportSync`, `…/api/feedback` | the [streamed session](orion-notifications.html#flow-of-one-notification) |
| `dmmSessionLostTimeoutMs` | `300000` | give up when the mapper does not know the session for this long |
| `dmmNoProgressTimeoutMs` | `1800000` | give up when the session does not move for this long |
| `mapID` | | default map for Orion notifications (when neither the url nor the entity gives one) |
| `orion.checkSubscriptionInterval` | `0` | ms between [recovery checks](orion-notifications.html#recovery-check-lost-notifications); `0`: only at startup |
| `orion.datapointsNotIndexed` | `["value"]` | datapoint fields whose values are not suggested; after a change rebuild `entries` for `orion` |
| `orion.skippingParams` | | new Orion distributions not synchronised when an attribute matches one of these values: the exact text, `"regex:<pattern>"` or a RegExp (e.g. `accessUrl: ["regex:/sdmx/2\\.1/dataflow/"]`: every Eurostat metadata url) |
| `statusLog.enabled`, `successes`, `successRetentionDays`, `maxDataSize` | `true`, `true`, `30`, `10000` | [recording of the requests](status-log.html#recording-source-connector) |
| `apiConnectorConfig.apiUrls` | | the polled APIs, [incremental](incremental-apis.html) or not |
| `apiConnectorConfig.pollInterval` | 1 day | ms between polls |
| `apiConnectorConfig.upsertRecords` (or on each API) | `false` | non incremental APIs: every answer replaces the stored records |
| on each API: `fhir` / `completeAnswer` | not set | [an answer without every record](incremental-apis.html#fhir-servers-without-paging) (FHIR Bundle: entries < total): an error, nothing stored |
| `apiConnectorConfig.unalignedAnswer` (or on each API) | not set | [non incremental APIs](source-connector.html#non-incremental-apis): `"append"` or `"upsert"` for an answer that does not continue the stored records; not set: by count |

## Frontend

See [Frontend — config.json reference](frontend.html#configjson-reference).

## Data Model Mapper

Used by the Source-Connector for the Orion datasets. Its `sessionLocation.filesystem` (or `.mongo`) must be `true`,
or the streamed sessions return no data. See `backend/docs/` in its repository.
