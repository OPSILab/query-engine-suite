// GET /api/keys, /api/values, /api/entries through the real router and auth, on a real MongoDB
// (see helpers/db.js: npm run test:db). MinIO and PostgreSQL connectors are stubbed (not used here).
const { stub, load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { publicKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/postgresConnector.js", () => ({ query: () => { } }))

let server, baseUrl

before(async () => {
    await db.setup(__filename)
    const Key = load("api/models/Key.js")
    const Value = load("api/models/Value.js")
    const Entries = load("api/models/Entries.js")
    const pub = ["public-data"]
    await Key.collection.insertMany([
        ...Array.from({ length: 12 }, (_, i) => ({ key: "city" + String(i).padStart(2, "0"), visibility: pub })),
        { key: "country", visibility: pub },
        { key: "secret", visibility: ["anna@demetrix.it"] },
        { key: "a.b(c", visibility: pub },
        { key: "apiOnly", visibility: pub, connectors: ["api"] },
        { key: "apiAndOrion", visibility: pub, connectors: ["api", "orion"] },
        { key: "value", visibility: pub, valuesNotIndexed: ["https://eurostat/a.xml"], connectors: ["orion", "api"] },
        { key: "measure", visibility: ["anna@demetrix.it"], valuesNotIndexed: ["https://x"], connectors: ["orion"] },
        { key: "region", visibility: pub, valuesNotIndexed: [] }
    ])
    await Value.collection.insertMany([{ value: "Rome", visibility: pub }, { value: "rovigo", visibility: pub }, { value: "Milan", visibility: pub }])
    await Entries.collection.insertMany([
        { key: "source", value: "https://a", visibility: pub },
        { key: "source", value: "https://b", visibility: pub },
        { key: "sourceId", value: "https://not-this", visibility: pub },
        { key: "Source", value: "https://c", visibility: pub },
        { key: "city", value: "Rome", visibility: pub },
        { key: "dimensions", value: "Lovech", visibility: pub, connectors: ["orion"] }
    ])
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
    config.updateOwner = "never"
})

async function get(path, { visibility = "public", token } = {}) {
    const res = await fetch(baseUrl + path, { headers: { visibility, ...(token ? { Authorization: "Bearer " + token } : {}) } })
    return { status: res.status, body: res.status == 200 ? await res.json() : await res.text() }
}

describe("pages", () => {
    test("keys: sorted pages with hasMore", async () => {
        const first = (await get("/keys?key=cit&limit=5")).body
        assert.deepEqual(first, { items: ["city00", "city01", "city02", "city03", "city04"].map(key => ({ key })), hasMore: true })
        const last = (await get("/keys?key=cit&limit=5&skip=10")).body
        assert.deepEqual(last, { items: [{ key: "city10" }, { key: "city11" }], hasMore: false })
    })

    test("values: prefix is case insensitive", async () => {
        assert.deepEqual((await get("/values?value=ro&limit=10")).body, { items: [{ value: "Rome" }, { value: "rovigo" }], hasMore: false })
    })

    test("entries: prefix, or exact key / value (case insensitive)", async () => {
        const prefix = (await get("/entries?key=source&value=https&limit=10")).body.items.map(e => e.key + " " + e.value)
        assert.deepEqual(prefix, ["Source https://c", "source https://a", "source https://b", "sourceId https://not-this"])
        const exact = (await get("/entries?key=source&value=https&limit=10&exactKey=true")).body.items.map(e => e.value)
        assert.deepEqual(exact, ["https://c", "https://a", "https://b"])
        const exactValue = (await get("/entries?key=&value=rome&limit=10&exactValue=true")).body.items
        assert.deepEqual(exactValue, [{ key: "city", value: "Rome" }])
    })

    test("the search text is literal, not a regex", async () => {
        assert.deepEqual((await get("/keys?key=" + encodeURIComponent("a.b(") + "&limit=10")).body.items, [{ key: "a.b(c" }])
        assert.deepEqual((await get("/keys?key=" + encodeURIComponent(".") + "&limit=10")).body.items, [])
    })

    test("invalid limit / skip: 400", async () => {
        for (const query of ["limit=0", "limit=501", "limit=x", "limit=5&skip=-1"])
            assert.equal((await get("/keys?key=c&" + query)).status, 400, query)
    })

    test("queryOptions.suggestionsMaxResults: the highest page size", async () => {
        config.queryOptions.suggestionsMaxResults = 1000
        assert.equal((await get("/keys?key=c&limit=1000")).status, 200)
        assert.equal((await get("/keys?key=c&limit=1001")).status, 400)
    })
})

describe("without a page (older clients): never the whole collection", () => {
    test("keys / values up to 500, then the 'too many' message", async () => {
        assert.deepEqual((await get("/keys?key=cit")).body.length, 12)
        const Key = load("api/models/Key.js")
        await Key.collection.insertMany(Array.from({ length: 501 }, (_, i) => ({ key: "zz" + i, visibility: ["public-data"] })))
        try {
            assert.deepEqual((await get("/keys?key=zz")).body, ["Too many suggestions. Type some characters in order to reduce them"])
        }
        finally {
            await Key.collection.deleteMany({ key: /^zz/ })
        }
    })

    test("entries: the first 500", async () => {
        assert.equal((await get("/entries?key=&value=")).body.length, 5) // no datapoints for older clients
    })
})

describe("visibility", () => {
    test("with authentication: only what the user may see", async () => {
        config.authConfig.disableAuth = false
        const token = makeToken({ azp: "query-engine", email: "anna@demetrix.it" })
        assert.deepEqual((await get("/keys?key=sec&limit=10", { visibility: "private", token })).body.items, [{ key: "secret" }])
        assert.deepEqual((await get("/keys?key=sec&limit=10", { visibility: "public", token })).body.items, [])
        assert.equal((await get("/keys?key=sec&limit=10")).status, 401)
    })

    test("authentication disabled: everything", async () => {
        assert.deepEqual((await get("/keys?key=sec&limit=10", { visibility: "private" })).body.items, [{ key: "secret" }])
    })
})

describe("GET /keys/notIndexed: keys whose values are not suggested", () => {
    test("only keys with origins in valuesNotIndexed, that the user may see", async () => {
        config.authConfig.disableAuth = false
        const token = makeToken({ azp: "query-engine", email: "anna@demetrix.it/x" })
        const res = await fetch(`${baseUrl}/keys/notIndexed`, { headers: { visibility: "public", Authorization: "Bearer " + token } })
        assert.deepEqual(await res.json(), { keys: ["value"] })
    })

    test("authentication disabled: all of them", async () => {
        const res = await fetch(`${baseUrl}/keys/notIndexed`, { headers: { visibility: "public" } })
        assert.deepEqual(await res.json(), { keys: ["value"] }) // no collections: no Orion-only keys
        const orion = await fetch(`${baseUrl}/keys/notIndexed?collections=orion`, { headers: { visibility: "public" } })
        assert.deepEqual(await orion.json(), { keys: ["measure", "value"] })
    })
})

describe("collections: only the suggestions of the chosen collections", () => {
    const get = async (path, params) => (await fetch(`${baseUrl}/${path}?` + new URLSearchParams(params), { headers: { visibility: "public" } }))

    test("keys, values, entries and notIndexed with ?collections=", async () => {
        const keys = await (await get("keys", { key: "api", limit: 10, collections: "orion" })).json()
        assert.deepEqual(keys.items.map(k => k.key), ["apiAndOrion"])
        const both = await (await get("keys", { key: "api", limit: 10, collections: "api,orion" })).json()
        assert.deepEqual(both.items.map(k => k.key), ["apiAndOrion", "apiOnly"])
        const entries = await (await get("entries", { key: "", value: "", limit: 10, collections: "orion" })).json()
        assert.deepEqual(entries.items, [{ key: "dimensions", value: "Lovech" }])
        assert.deepEqual(await (await get("keys/notIndexed", { collections: "minio" })).json(), { keys: [] })
        // written before the connectors existed (no `connectors`): API / MinIO data
        assert.deepEqual((await (await get("keys", { key: "count", limit: 10, collections: "api" })).json()).items, [{ key: "country" }])
        assert.deepEqual((await (await get("keys", { key: "count", limit: 10, collections: "orion" })).json()).items, [])
        assert.deepEqual(await (await get("keys/notIndexed", { collections: "orion" })).json(), { keys: ["measure", "value"] })
    })

    test("without collections (older clients): api and minio, never datapoints only; unknown collection: 400", async () => {
        const all = await (await get("keys", { key: "api", limit: 10 })).json()
        assert.deepEqual(all.items.map(k => k.key), ["apiAndOrion", "apiOnly"])
        const entries = await (await get("entries", { key: "dimensions", value: "", limit: 10 })).json()
        assert.deepEqual(entries.items, []) // dimensions = Lovech comes from Orion only
        config.queryOptions.defaultCollections = ["api", "orion", "minio"]
        const withOrion = await (await get("entries", { key: "dimensions", value: "", limit: 10 })).json()
        assert.deepEqual(withOrion.items, [{ key: "dimensions", value: "Lovech" }])
        assert.equal((await get("values", { value: "", limit: 10, collections: "ftp" })).status, 400)
    })
})
