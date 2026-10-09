// Advanced search (POST /api/query with mongoQuery) through the real router, bodyCheck and auth, on a real
// MongoDB (see helpers/db.js: npm run test:db). MinIO and PostgreSQL connectors are stubbed (not used here).
const { stub, load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { publicKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/postgresConnector.js", () => ({ query: () => { } }))

const PREFIX = "anna@demetrix.it/data model mapper"
// in their collections: minio (files, with a MinIO record), orion (datapoints), api (apiConnector records)
const DOCS = [
    { name: PREFIX + "/mine.json", record: { bucketName: "pilot" }, city: "Rome", json: [{ city: "Rome" }] },
    { name: "bob@demetrix.it/data model mapper/b.json", record: { bucketName: "pilot" }, city: "Rome" },
    { name: "public-a.json", record: { bucketName: "public-data" }, json: [{ city: "Rome", n: 1 }, { city: "Oslo" }] },
    { name: "public-b.json", record: { bucketName: "public-data" }, city: "Rome", json: [{ city: "Rome" }] },
    { name: "public.csv", record: { bucketName: "public-data" }, csv: [{ city: "Rome" }] },
    { name: "map.geojson", record: { bucketName: "public-data" }, features: [{ properties: { city: "Rome" }, geometry: { type: "Polygon", coordinates: [[[12.5, 41.9], [12.6, 41.9]]] } }] },
    { name: "Datapoint", source: "EUROSTAT", fromUrl: "https://eurostat/nama.xml", survey: "NAMA", dimensions: ["Lovech", "Euro per inhabitant", "2020"], value: 1, kind: "item", n: 100 },
    { name: "Text code", source: "https://api.example.org/codes", code: "007" },
    ...Array.from({ length: 7 }, (_, i) => ({ name: "Item " + i, source: "https://api.example.org/items", kind: "item", n: i }))
]
const collectionOf = d => d.record ? "minio" : d.survey ? "orion" : "api"

let server, baseUrl

before(async () => {
    await db.setup(__filename)
    const collections = load("api/services/collections.js")
    for (const c of ["api", "orion", "minio"])
        await collections.collectionModel(c).collection.insertMany(DOCS.filter(d => collectionOf(d) == c).map(d => ({ ...d })))
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

// as the frontend sends it: the fields in the body and in the query string (with the file type as format)
async function search(mongoQuery, { format, page, collections, visibility = "public", token } = {}) {
    const params = new URLSearchParams({ ...(format ? { format } : {}), ...Object.fromEntries(Object.entries(mongoQuery).map(([k, v]) => [k, typeof v == "string" ? v : JSON.stringify(v)])) })
    const res = await fetch(`${baseUrl}/query?${params}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", visibility, ...(token ? { Authorization: "Bearer " + token } : {}) },
        body: JSON.stringify({ mongoQuery, ...(page ? { page } : {}), ...(collections ? { collections } : {}) })
    })
    const header = res.headers.get("x-query-warnings")
    return { status: res.status, body: res.status == 200 ? await res.json() : await res.text(), warnings: header ? JSON.parse(decodeURIComponent(header)) : [] }
}
const names = list => list.map(d => d.name).sort()
const ALL = { collections: ["api", "orion", "minio"] }

describe("file types", () => {
    test("no type / Object: top-level fields", async () => {
        assert.deepEqual(names((await search({ city: "Rome" })).body), ["bob@demetrix.it/data model mapper/b.json", "anna@demetrix.it/data model mapper/mine.json", "public-b.json"].sort())
    })

    test("JSON: top-level fields or JSON rows, each document once", async () => {
        const { body } = await search({ city: "Rome" }, { format: "JSON" })
        assert.deepEqual(names(body), [PREFIX + "/mine.json", "bob@demetrix.it/data model mapper/b.json", "public-a.json", "public-b.json"].sort())
    })

    test("CSV rows, GeoJSON properties and coordinates", async () => {
        assert.deepEqual(names((await search({ city: "Rome" }, { format: "CSV" })).body), ["public.csv"])
        assert.deepEqual(names((await search({ city: "Rome" }, { format: "GeoJSON" })).body), ["map.geojson"])
        assert.deepEqual(names((await search({ coordinates: "12.6" }, { format: "GeoJSON" })).body), ["map.geojson"])
        assert.deepEqual(names((await search({ coordinates: "99" }, { format: "GeoJSON" })).body), [])
    })

    test("an array field matches any of its elements (dimensions = Lovech)", async () => {
        assert.deepEqual(names((await search({ dimensions: "Lovech" }, ALL)).body), ["Datapoint"])
    })

    test("a number typed in the form matches numbers and text (datapoints' value is a number)", async () => {
        assert.deepEqual(names((await search({ value: "1" }, ALL)).body), ["Datapoint"])
        assert.deepEqual(names((await search({ value: "1.0" }, ALL)).body), ["Datapoint"])
        assert.deepEqual(names((await search({ n: "1" }, { format: "JSON" })).body), ["Item 1", "public-a.json"]) // JSON rows and top-level fields
        assert.deepEqual(names((await search({ code: "007" })).body), ["Text code"])
        assert.deepEqual(names((await search({ value: "1x" })).body), [])
    })

    test("MinIO results carry fileName / path / fileType", async () => {
        const [doc] = (await search({ city: "Rome" }, { format: "CSV" })).body
        assert.deepEqual([doc.fileName, doc.path, doc.fileType], [undefined, "public.csv", "csv"])
    })
})

describe("collections", () => {
    test("without `collections` (older clients): API records and MinIO files only, as before - no datapoints, no _collection", async () => {
        const legacy = (await search({ kind: "item" })).body
        assert.deepEqual([legacy.length, legacy.some(r => r.name == "Datapoint"), legacy.some(r => "_collection" in r)], [7, false, false])
        assert.deepEqual(names((await search({ value: "1" })).body), [])
    })

    test("queryOptions.defaultCollections decides what the requests without `collections` search", async () => {
        config.queryOptions.defaultCollections = ["api", "orion"]
        assert.deepEqual(names((await search({ value: "1" })).body), ["Datapoint"])
        config.queryOptions.defaultCollections = ["orion"]
        assert.deepEqual(names((await search({ kind: "item" })).body), ["Datapoint"])
        for (const invalid of [[], ["ftp"], "api"]) { // invalid: the default (api, minio)
            config.queryOptions.defaultCollections = invalid
            assert.equal((await search({ kind: "item" })).body.length, 7, JSON.stringify(invalid))
        }
    })

    test("only the chosen collections with `collections`; _collection on every result", async () => {
        const all = (await search({ kind: "item" }, ALL)).body
        assert.deepEqual([...new Set(all.map(r => r._collection))].sort(), ["api", "orion"])
        const api = (await search({ kind: "item" }, { collections: ["api"] })).body
        assert.deepEqual([api.length, [...new Set(api.map(r => r._collection))]], [7, ["api"]])
        assert.deepEqual(names((await search({ kind: "item" }, { collections: ["orion", "minio"] })).body), ["Datapoint"])
        assert.deepEqual((await search({ kind: "item" }, { collections: [] })).body, [])
    })

    test("unknown collection: 400; a collection not stored in MongoDB is skipped", async () => {
        assert.equal((await search({ kind: "item" }, { collections: ["ftp"] })).status, 400)
        config.collections.orion.toMongo = false
        assert.deepEqual(names((await search({ kind: "item" }, { collections: ["orion"] })).body), [])
    })

    test("fileInfo only on MinIO files", async () => {
        const [item] = (await search({ name: "Item 1" })).body
        assert.equal(item.fileType, undefined)
    })
})

describe("pages", () => {
    test("{ results, hasMore, skip, next }: per collection, sorted; next = skip of the collections with more", async () => {
        const first = (await search({ kind: "item" }, { page: { limit: 3 }, ...ALL })).body
        assert.deepEqual(first.results.filter(r => r._collection == "api").map(r => r.n), [0, 1, 2])
        assert.deepEqual(first.results.filter(r => r._collection == "orion").map(r => r.n), [100])
        assert.deepEqual([first.hasMore, first.limit, first.skip, first.next], [true, 3, { api: 0, orion: 0, minio: 0 }, { api: 3 }])
        const second = (await search({ kind: "item" }, { page: { limit: 3, skip: first.next }, collections: Object.keys(first.next) })).body
        assert.deepEqual([second.results.map(r => r.n), second.next], [[3, 4, 5], { api: 6 }])
        const last = (await search({ kind: "item" }, { page: { limit: 3, skip: { api: 6 } }, collections: ["api"] })).body
        assert.deepEqual([last.results.map(r => r.n), last.hasMore, last.next], [[6], false, {}])
    })

    test("a number as skip: every collection", async () => {
        const page = (await search({ kind: "item" }, { page: { limit: 3, skip: 6 }, ...ALL })).body
        assert.deepEqual(page.results.map(r => r.n), [6])
    })

    test("pages apply to what the user may see", async () => {
        config.authConfig.disableAuth = false
        const token = makeToken({ azp: "query-engine", email: PREFIX })
        const page = (await search({ city: "Rome" }, { visibility: "private", token, page: { limit: 1 } })).body
        assert.deepEqual([names(page.results), page.hasMore], [[PREFIX + "/mine.json"], false])
    })

    test("invalid page: 400", async () => {
        for (const page of [{ limit: 0 }, { limit: 1001 }, { limit: "x" }, { limit: 5, skip: -1 }, { limit: 5, skip: { ftp: 1 } }, { limit: 5, skip: { api: -1 } }])
            assert.equal((await search({ kind: "item" }, { page })).status, 400, JSON.stringify(page))
    })

    test("without a page: at most advancedSearchMaxResults per collection, a warning for each one with more", async () => {
        config.queryOptions.advancedSearchMaxResults = 5
        const { body, warnings } = await search({ kind: "item" }, ALL)
        assert.equal(body.length, 6) // 5 api + 1 orion
        assert.deepEqual(warnings.map(w => [w.kind, w.code, w.source]), [["runtime", "RESULTS_TRUNCATED", "api"]])
        config.queryOptions.advancedSearchMaxResults = 7
        assert.deepEqual((await search({ kind: "item" }, ALL)).warnings, [])
        config.queryOptions.advancedSearchMaxResults = 5
        assert.deepEqual((await search({ kind: "item" })).body.length, 5) // older clients: the same limit
    })
})

describe("operators", () => {
    test("only read operators: $where, $function, $expr... 400, nothing run", async () => {
        for (const query of [{ $where: "sleep(1000) || true" }, { city: { $function: { body: "return true", args: [], lang: "js" } } }, { $expr: { $eq: [1, 1] } }]) {
            const { status, body } = await search(query)
            assert.equal(status, 400, JSON.stringify(query))
            assert.match(body, /not allowed/)
        }
    })

    test("a query string field with a nested operator ([$where]): 400", async () => {
        const res = await fetch(`${baseUrl}/query?city[$where]=1`, { method: "POST", headers: { "Content-Type": "application/json", visibility: "public" }, body: JSON.stringify({}) })
        assert.equal(res.status, 400)
    })
})

describe("visibility", () => {
    test("with authentication: private, public", async () => {
        config.authConfig.disableAuth = false
        const token = makeToken({ azp: "query-engine", email: PREFIX })
        assert.deepEqual(names((await search({ city: "Rome" }, { visibility: "private", token })).body), [PREFIX + "/mine.json"])
        assert.deepEqual(names((await search({ city: "Rome" }, { format: "JSON", visibility: "public", token })).body), ["public-a.json", "public-b.json"])
        assert.equal((await search({ city: "Rome" })).status, 401)
    })
})
