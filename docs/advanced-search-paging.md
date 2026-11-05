---
layout: default
title: Advanced search paging
---

# Advanced search paging

An Advanced search without `page` returns at most `queryOptions.advancedSearchMaxResults` results per collection (1000
by default). To read **all** the results, a client asks for pages and follows the cursor the Query-Engine returns.

## The request

```http
POST /api/query?source=https://api.example.org/observations
Content-Type: application/json
Authorization: Bearer <token>

{
  "mongoQuery": { "source": "https://api.example.org/observations" },
  "collections": ["api", "minio"],
  "page": { "limit": 1000, "skip": 0 }
}
```

- The searched fields go in the query string (and in `mongoQuery`); `page` and `collections` go in the body.
- `limit`: 1 to `advancedSearchMaxResults`. It can be chosen per request; a larger value gets 400.
- `skip`: a number (the same for every collection) or `{ "<collection>": n }`.

## The answer

```json
{
  "results": [ { "_collection": "api", "...": "..." } ],
  "hasMore": true,
  "limit": 1000,
  "skip": 0,
  "next": { "api": 1000 }
}
```

`next` lists only the collections that have more results, with the `skip` to send for them. Collections already read
to the end are not in `next` and must not be asked again.

## The client loop

```js
let page = { limit: 1000, skip: 0 }
let collections = ["api", "minio"]
const all = []
while (true) {
  const answer = await post("/api/query?source=...", { mongoQuery, collections, page })
  all.push(...answer.results)
  if (!answer.hasMore) break
  page = { limit: page.limit, skip: answer.next, skipRows: answer.nextRows } // nextRows: only with unwrap
  collections = Object.keys(answer.next)
}
```

With 20 000 matching documents in `api` and a limit of 5000, this makes four requests: `skip` 0, then
`{ api: 5000 }`, `{ api: 10000 }`, `{ api: 15000 }`; the fourth answer has `hasMore: false`.

## With unwrap

`?unwrap=true` turns every element of the arrays of a document into a result (a row);
`unwrapOnlyArrayOfObjects=true` unwraps only arrays of objects, `unwrapPath=data.items` unwraps at another level than
the top of the documents. Each row carries `unWrappedFrom` (the path of its array).

The limit then applies to the **rows**, not to the documents, and one document can be cut between two pages. So the
answer has one more cursor:

- `next`: `{ "<collection>": documents to skip }`, as before;
- `nextRows`: `{ "<collection>": rows already given of the first of those documents }`.

Send them back as `page.skip` and `page.skipRows`. Example, limit 100, documents of 40 rows each:

| Request | `skip` | `skipRows` | Rows returned | Answer `next` / `nextRows` |
|---|---|---|---|---|
| 1 | 0 | — | doc 0 (40) + doc 1 (40) + 20 rows of doc 2 | `{ api: 2 }` / `{ api: 20 }` |
| 2 | `{ api: 2 }` | `{ api: 20 }` | the last 20 of doc 2 + doc 3 + doc 4 | `{ api: 5 }` / `{}` |

A document is never repeated or skipped, whatever the page size.
