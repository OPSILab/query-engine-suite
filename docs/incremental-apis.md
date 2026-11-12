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
    maxWindow: 30 * 24 * 3600 * 1000, // optional: the longest span of one request (needs endDateParam)
    alignToDays: false,              // optional: windows of whole UTC days, midnight to midnight (below)
    retryNotFound: false             // optional: ask again, at the next polls, the windows answered 404
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
| a previous poll stored some windows | from the end of the last window stored to now (also on the same day, if that poll was interrupted) |
| no state yet, records exist (APIs polled before the state existed) | from the date of the last poll (`datePolled` of the last record) to now |
| no record of this API yet, `startDateValue` set | from `startDateValue` to now |
| no record, no `startDateValue` | one request without start date (everything) |
| the last poll reached now today | **nothing**: already polled today |

The end of the last window stored is kept per url in the MongoDB collection `incrementalstate` (in MongoDB even when
the API records are only in PostgreSQL).

`startDateLogic` / `endDateLogic` say whether the API includes the bounds, so that two consecutive polls neither
overlap nor leave a gap.

**`maxWindow`** splits a long span (the first poll, or a poll after a long stop) into several requests of at most
`maxWindow` ms each, contiguous like daily polls. With `YYYY-MM-DD` dates it is rounded to whole days (at least one).
Without `endDateParam` it is ignored (warning in the log).

**`alignToDays: true`** makes every window a whole UTC day (or `maxWindow` whole days), from midnight to the next
midnight, whatever the date format, for APIs that aggregate per period (a KPI of a day). Only the days already over
are asked: a poll at 16:00 of the 12th asks the 11th (`2026-11-11T00:00:00.000Z` – `2026-11-12T00:00:00.000Z` with
`RFC 3339` and `endDateLogic: "exclusive"`; `…T23:59:59.999Z` with `"inclusive"`), a second poll the same day asks
nothing, the 12th is asked on the 13th. `pollDelay` moves back the last day asked (one day: on the 12th, the 10th),
not the bounds. The start goes back to its midnight: `startDateValue` and, after switching an API to `alignToDays`, the
end of its last window. The poll interval does not change the windows. To align records already stored,
[reset](#reset) the API after switching.

## Pagination

Each request (each window) is read page by page: `offsetParam` starts at `offsetValue.startFrom`, or 0 (the epoch for
`offsetValue.type: "RFC 3339"`), and grows by `limit`, while `condition(response)` is true. With
`offsetValue: { from: "response", value: "<field>" }` the next offset is taken from the first record of the answer
(a cursor); reading stops when it does not change.

## Request dates on the records

Every record of an incremental poll carries the dates of the request that returned it: the keys `startDateParam` and
`endDateParam`, with the values sent as query params (for a paginated API, those of that page). They are added to the
response when it is an object, or to each object of the array, **only if the key is not already there**: a record
that has its own `date_from` keeps it. A date not sent (e.g. no start date on a first poll without `startDateValue`)
is not added. Example, `startDateParam: "date_from"`, `endDateParam: "date_to"`:

```json
{ "kpi": "visitors", "value": 1250, "date_from": "2026-10-01", "date_to": "2026-10-01" }
```

## Batch APIs

An API with `batch` reads a list first (`batch.from`), then calls `url` once per element, with `{batch}` replaced by
the value of the element (`batch.param`, inside `batch.pick` if given). With `incremental: true`:

```js
{
  name: "Stations readings",
  batch: { from: "https://api.example.org/stations", pick: "items", param: ["station", "code"] },
  url: "https://api.example.org/stations/{batch}/readings",
  incremental: true,
  incrementalParams: { startDateParam: "from", startDateFormat: "YYYY-MM-DD", startDateValue: "2026-01-01",
                       endDateParam: "to", endDateFormat: "YYYY-MM-DD", endDateLogic: "inclusive", maxWindow: 30 * 24 * 3600 * 1000 }
}
```

- The list request is sent as it is, without dates. The dates (and the `maxWindow` windows) go on the request of
  **each element**.
- Every element has **its own last poll**: its records are stored under its own url, and the next poll of that element
  starts from there. An element that appears in the list later starts from `startDateValue`; one already polled today
  is skipped.
- An element whose request fails does **not stop the others**. Its next poll resumes from the window that failed
  (see below).
- The element requests are not paginated.

## Interrupted polls

A window covers dates already past: once stored, its data does not change. So **every window is saved as soon as it
is done**, and is never requested again.

- While a window is read, its records are marked **pending** (MongoDB `status` collection, PostgreSQL `status` table).
  When its last page is stored the marks are removed and the end of the window is saved in `incrementalstate`.
- If a poll stops halfway (an error, a token that cannot be renewed, a restart), the windows already done stay. Only the records of
  the window that was interrupted (its first pages, for a paginated API) are deleted by the next poll (rollback), which
  then starts from that window, at the next poll (`pollInterval`), also on the same day. So a window is never stored
twice, and never skipped. 401 and 404 do not stop the poll (below).

Example, `maxWindow` one day from 2026-10-01, a 401 at 2026-10-15: the first poll stores 2026-10-01…10-14; the next
one, even on the same day, starts from 2026-10-15.

## Expired token (401)

When an API with a token from `authUrl` answers 401, the token is asked again and the same request repeated: if it
succeeds, the poll goes on at once with the next windows (and pages, and elements of a batch API), with the new token.
A second 401, or a token that cannot be renewed (written in the configuration, without `authUrl`), stops the poll as
any other error. This holds for every API request, incremental or not.

## Not found (404)

Some APIs answer 404 instead of an empty list. In an incremental poll a **404 is taken as no records**: the window
counts as done and the poll goes on at once with the next one, in the same poll (for a paginated API, a 404 after the first page ends the pages
of that window). Any other error stops the poll, as above.

By default a window answered 404 is not requested again. With `retryNotFound: true` it is remembered (in
`incrementalstate`) and requested again at every following poll, before the new windows, until it answers. Example:
a 404 on 2026-10-15, on 2026-11-11 the poll asks for 2026-10-15 first, then for 2026-11-10. The polls skipped because
the API was already polled that day do not retry them.

## Reset

When an incremental API has wrong or duplicated records, reset it: every record of that API is deleted (MongoDB
collection, PostgreSQL table, keys / values / entries, pending marks, state in `incrementalstate`) and the API is polled again at once, as the first
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
| batch APIs | the records of every element are deleted (every url matching `url` with any value in place of `{batch}`), and every element is polled again from `startDateValue`; the answer reports how many elements |
| command line | `node scripts/resetIncrementalApis.js` |

**Not reversible.** Every reset (not the dry runs) is recorded in the `incrementalresetlog` collection: who, when,
which APIs, record counts before and after, outcome. `GET /api/incremental/reset/log?limit=100` reads it, newest first;
the Source connector page shows it under "Log of the resets".
