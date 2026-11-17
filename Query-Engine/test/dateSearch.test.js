// Advanced search with "Date" filters (services/dateFilter.js) through the real router, bodyCheck and controller, on a
// real MongoDB (see helpers/db.js: npm run test:db), with and without unwrap.
const { stub, load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { publicKey } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/postgresConnector.js", () => ({ query: () => { } }))

const AGROTOUR = "https://agrotour.example/api/v1/search"
const created = list => ({ source: AGROTOUR, meta: { has_more: false }, data: list.map(([id, at]) => ({ id, record: { created_at: at, name: "Farm " + id } })) })
const API = [
    { name: "Page 1", ...created([["a", "2026-11-14T10:00:00Z"], ["b", "2026-11-16T09:30:00.123Z"], ["c", "2026-11-16T23:30:00+00:00"]]) },
    // one element before the 16th and one after: none on the 16th
    { name: "Page 2", ...created([["d", "2026-11-10T00:00:00Z"], ["e", "2026-11-20T00:00:00Z"]]) },
    // with an offset: 16th, 22:30 UTC (the 17th as text)
    { name: "Page 3", ...created([["f", "2026-11-17T00:30:00+02:00"]]) },
    { name: "Dated", source: "https://api.example/dated", when: new Date("2026-11-16T12:00:00Z") },
    { name: "Day only", source: "https://api.example/dated", when: "2026-11-16" },
    { name: "Not a date", source: "https://api.example/dated", when: "16/11/2026" }
]
const MINIO = [
    { name: "rows.json", record: { bucketName: "public-data" }, json: [{ day: "2026-11-16T05:00:00Z", k: 1 }, { day: "2026-11-01T00:00:00Z", k: 2 }] }
]
const DAY_16 = { $gte: "2026-11-16T00:00:00.000Z", $lte: "2026-11-16T23:59:59.999Z" }

let server, baseUrl

before(async () => {
    await db.setup(__filename)
    const collections = load("api/services/collections.js")
    await collections.collectionModel("api").collection.insertMany(API.map(d => ({ ...d })))
    await collections.collectionModel("minio").collection.insertMany(MINIO.map(d => ({ ...d })))
    const app = express()
    app.use(express.json())
    app.use("/api", load("api/routes/router.js"))
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    baseUrl = `http://127.0.0.1:${server.address().port}/api`
})
after(async () => {
    server?.close()
    await db.teardown()
})
beforeEach(() => {
    resetConfig()
    console.debug = () => { }
    Object.assign(config.authConfig, { disableAuth: true, clientId: "query-engine", publicKey, userInfoEndpoint: "", introspect: false })
})

// as the frontend sends it: the fields in the body and, as JSON text, in the query string
async function search(mongoQuery, { format, page, collections = ["api"], query = {} } = {}) {
    const params = new URLSearchParams({ ...(format ? { format } : {}), ...Object.fromEntries(Object.entries(mongoQuery).map(([k, v]) => [k, typeof v == "string" ? v : JSON.stringify(v)])), ...query })
    const body = { mongoQuery: Object.fromEntries(Object.entries(mongoQuery).map(([k, v]) => [k, typeof v == "string" ? v : JSON.stringify(v)])), collections, ...(page ? { page } : {}) }
    const res = await fetch(`${baseUrl}/query?${params}`, { method: "POST", headers: { "Content-Type": "application/json", visibility: "public" }, body: JSON.stringify(body) })
    return { status: res.status, body: res.status == 200 ? await res.json() : await res.text() }
}
const names = list => list.map(d => d.name).sort()
const ids = list => list.map(row => row.id).sort()

describe("Date filters", () => {
    test("a field inside an array: one element in the range (the same one for both bounds), in any ISO 8601 form", async () => {
        assert.deepEqual(names((await search({ "data.record.created_at": DAY_16 })).body), ["Page 1", "Page 3"])
    })

    test("one bound only (the other null or missing)", async () => {
        assert.deepEqual(names((await search({ "data.record.created_at": { $gte: "2026-11-16T00:00:00.000Z", $lte: null } })).body), ["Page 1", "Page 2", "Page 3"])
        assert.deepEqual(names((await search({ "data.record.created_at": { $lte: "2026-11-12T00:00:00.000Z" } })).body), ["Page 2"])
    })

    test("no bound: no condition", async () => {
        assert.equal((await search({ "data.record.created_at": { $gte: null, $lte: null } })).body.length, API.length)
    })

    test("a BSON Date or a date without time; a text that is not an ISO date never matches", async () => {
        assert.deepEqual(names((await search({ when: DAY_16 })).body), ["Dated", "Day only"])
    })

    test("with other fields", async () => {
        assert.deepEqual(names((await search({ "data.record.created_at": DAY_16, name: "Page 3" })).body), ["Page 3"])
    })

    test("unwrap: only the elements in the range (with or without unwrapPath)", async () => {
        for (const unwrapPath of [undefined, "data"]) {
            const { body } = await search({ "data.record.created_at": DAY_16 }, { query: { unwrap: "true", ...(unwrapPath ? { unwrapPath } : {}) } })
            assert.deepEqual(ids(body), ["b", "c", "f"], unwrapPath)
            assert.ok(body.every(row => row.record?.name && row.unWrappedFrom == "data" && row["../source"] == AGROTOUR))
        }
    })

    test("unwrap with pages: the limit counts the elements kept", async () => {
        const seen = []
        let page = { limit: 2, skip: 0 }
        for (let n = 0; n < 5 && page; n++) {
            const { body } = await search({ "data.record.created_at": DAY_16 }, { page, query: { unwrap: "true" } })
            assert.ok(body.results.length <= 2)
            seen.push(...body.results.map(row => row.id))
            page = body.hasMore ? { limit: 2, skip: body.next, skipRows: body.nextRows } : undefined
        }
        assert.deepEqual(seen.sort(), ["b", "c", "f"])
    })

    test("JSON file type: the field in the rows; with unwrap only the rows in the range", async () => {
        assert.deepEqual(names((await search({ day: DAY_16 }, { format: "JSON", collections: ["minio"] })).body), ["rows.json"])
        assert.deepEqual(names((await search({ day: { $lte: "2026-10-01" } }, { format: "JSON", collections: ["minio"] })).body), [])
        const { body } = await search({ day: DAY_16 }, { format: "JSON", collections: ["minio"], query: { unwrap: "true" } })
        assert.deepEqual(body.map(row => row.k), [1])
    })
})
