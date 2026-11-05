---
layout: default
title: Incremental APIs
---

# Incremental APIs

The `apiConnector` polls the APIs of `apiConnectorConfig.apiUrls` every `pollInterval` ms. An API with
`incrementalParams` is **incremental**: each poll asks only for the records since the previous one.

## Configuration

```js
{
  name: "Regrowth (observations)",
  url: "https://api.regrowth.example/observations",
  incrementalParams: {
    startDateParam: "from",          // query param of the start date
    startDateFormat: "YYYY-MM-DD",   // YYYY-MM-DD | millis | RFC 3339
    startDateValue: "2024-01-01",    // first poll: from this date (sent as written)
    startDateLogic: "inclusive",     // does the API include the start date? inclusive | exclusive
    endDateParam: "to",              // optional: query param of the end date
    endDateFormat: "YYYY-MM-DD",
    endDateLogic: "exclusive",
    pollDelay: 0,                    // ms subtracted from both dates (data published late)
    maxWindow: 30 * 24 * 3600 * 1000 // optional: the longest span of one request (needs endDateParam)
  },
  pagination: {                      // optional
    offsetParam: "offset",
    limitParam: "limit",
    limit: 1000,
    condition: response => response.data.length > 0  // keep reading pages while true
  }
}
```

## How a poll chooses its dates

| Situation | Requests |
|---|---|
| no record of this API yet, `startDateValue` set | from `startDateValue` to now |
| no record, no `startDateValue` | one request without start date (everything) |
| records exist | from the date of the last complete poll to now |
| the last poll was today | **nothing**: already polled today |

`startDateLogic` / `endDateLogic` say whether the API includes the bounds, so that two consecutive polls neither
overlap nor leave a gap.

**`maxWindow`** splits a long span (the first poll, or a poll after a long stop) into several requests of at most
`maxWindow` ms each, contiguous like daily polls. With `YYYY-MM-DD` dates it is rounded to whole days (at least one).
Without `endDateParam` it is ignored (warning in the log).

## Pagination

Each request (each window) is read page by page: `offsetParam` starts at `offsetValue.startFrom`, or 0 (the epoch for
`offsetValue.type: "RFC 3339"`), and grows by `limit`, while `condition(response)` is true. With
`offsetValue: { from: "response", value: "<field>" }` the next offset is taken from the first record of the answer
(a cursor); reading stops when it does not change.

## Interrupted polls

Every record stored by a paginated poll is marked **pending** (MongoDB `status` collection, PostgreSQL `status` table,
with the API url and the poll date). When the last page, or the last window, has been stored, the marks of that poll
are removed.

If a poll stops halfway (an error, a restart), its marks stay. The next poll of that API first **deletes those
records** (rollback), then starts again from the last complete poll. So the records of an incomplete poll never count
as polled and are never stored twice.

## Reset

When an incremental API has wrong or duplicated records, reset it: every record of that API is deleted (MongoDB
collection, PostgreSQL table, keys / values / entries, pending marks) and the API is polled again at once, as the first
time, from `startDateValue`, window by window, every page.

From the frontend (Source connector page): tick the APIs, type the management password, confirm. Through the API:

```http
POST /api/incremental/reset?name=Regrowth%20(observations)
X-Management-Secret: <managementSecret>
Authorization: Bearer <token>
```

| Detail | |
|---|---|
| which APIs | `name` (repeatable) or `{ "names": [...] }` in the body; none: every incremental API |
| dry run | `dryRun=true`: only the record counts, nothing deleted, no password needed |
| password | `managementSecret` of the Source-Connector config, in `X-Management-Secret`; not configured: reset disabled (403) |
| refused (409) | while a poll or a rebuild runs, or with `apiConnector` disabled (nothing would poll again) |
| progress | runs in background (202); `GET /api/incremental/reset` |
| APIs with `batch` | not incremental: never reset |
| command line | `node scripts/resetIncrementalApis.js` |

**Not reversible.** Every reset (not the dry runs) is recorded in the `incrementalresetlog` collection: who, when,
which APIs, record counts before and after, outcome. `GET /api/incremental/reset/log?limit=100` reads it, newest first;
the Source connector page shows it under "Log of the resets".
