// Live simple search on APIs and Orion (simpleSearch.js) against a local fake of the external sources.
const { load, config, resetConfig } = require("./helpers/env")
const { startFakeSources } = require("./helpers/fakeSources")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

const simpleSearch = load("api/services/simpleSearch.js")
const { objectFilter } = load("api/services/visibility.js")

let fake

before(async () => { fake = await startFakeSources() })
after(() => fake.close())
beforeEach(() => {
    resetConfig()
    simpleSearch._reset()
    fake.calls.length = 0
})

const oauth = () => ({
    Authorization: {
        type: "bearerToken",
        authProfile: "OAuth 2.0 Client Credentials Grant",
        credentials: { client_id: "cid", client_secret: "secret" },
        authUrl: { value: fake.url + "/oauth/token", requestType: "POST" }
    }
})
const useApis = (...apiUrls) => { config.apiConnectorConfig.apiUrls = apiUrls }
const codes = warnings => warnings.map(w => w.code)

describe("limits (configuration)", () => {
    test("default: Orion is not searched", () => {
        useApis({ name: "A", url: "x" })
        assert.deepEqual(simpleSearch.limits(), [{ kind: "config", code: "ORION_DISABLED", message: "Orion sources are not searched (simpleSearchOptions.orion = false)", collection: "orion" }])
    })

    test("APIs excluded one by one or all together, MinIO off", () => {
        useApis({ name: "A", url: "x" }, { name: "B", url: "y", simpleSearch: false })
        assert.deepEqual(simpleSearch.limits().map(w => [w.code, w.source]), [["API_EXCLUDED", "B"], ["ORION_DISABLED", undefined]])
        config.simpleSearchOptions = { api: false, minio: false, orion: true }
        assert.deepEqual(codes(simpleSearch.limits()), ["MINIO_DISABLED", "API_DISABLED"])
        assert.deepEqual(simpleSearch.limits().map(w => w.collection), ["minio", "api"]) // the frontend shows those of the selected collections
    })

    test("no API configured: nothing to warn about the APIs", () => {
        config.simpleSearchOptions.api = false
        assert.deepEqual(codes(simpleSearch.limits()), ["ORION_DISABLED"])
    })

    test("old config.js without simpleSearchOptions: built-in defaults", () => {
        delete config.simpleSearchOptions
        assert.equal(simpleSearch.options().orion, false)
        assert.equal(simpleSearch.options().maxPages, 20)
    })
})

describe("APIs", () => {
    test("plain GET with an OAuth token: matching records, shaped as public API records", async () => {
        useApis({ name: "Plain", url: fake.url + "/plain", headers: oauth() })
        const warnings = []
        const results = await simpleSearch.searchLive("Rome", warnings)
        assert.deepEqual(results, [{ raw: { city: "Rome", id: 1 }, name: "Plain", source: fake.url + "/plain", record: { from: fake.url + "/plain", api: "Plain" } }])
        assert.deepEqual(warnings, [])
        assert.ok(objectFilter(results[0], "someone@x.it", "pilot", "public"))
        assert.equal(objectFilter(results[0], "someone@x.it", "pilot", "private"), false)
    })

    test("the token is reused until it expires", async () => {
        useApis({ name: "Plain", url: fake.url + "/plain", headers: oauth() })
        await simpleSearch.searchLive("Rome")
        await simpleSearch.searchLive("Milan")
        assert.equal(fake.callsTo("/oauth/token").length, 1)
        assert.equal(fake.callsTo("/plain").length, 2)
    })

    test("no value: every record", async () => {
        useApis({ name: "Plain", url: fake.url + "/plain", headers: oauth() })
        assert.equal((await simpleSearch.searchLive("")).length, 2)
    })

    test("a single object response, a POST with body, a static header", async () => {
        useApis(
            { name: "Single", url: fake.url + "/single" },
            { name: "Post", url: fake.url + "/search", method: "POST", body: { q: "anything" } },
            { name: "Key", url: fake.url + "/static-key", headers: { "x-api-key": "k" } }
        )
        const results = await simpleSearch.searchLive("Rome")
        assert.deepEqual(results.map(r => r.name).sort(), ["Key", "Post", "Single"])
        assert.deepEqual(fake.callsTo("/search")[0].body, { q: "anything" })
    })

    test("batch: one request per batch value, the origin is the batch url", async () => {
        useApis({ name: "Batch", url: fake.url + "/batch/{batch}", batch: { from: fake.url + "/batch-list", pick: "items", param: ["ref", "id"] } })
        const results = await simpleSearch.searchLive("Rome")
        assert.deepEqual(fake.callsTo("/batch/a").length + fake.callsTo("/batch/b").length, 2)
        assert.deepEqual(results.map(r => r.record.from), [fake.url + "/batch/a"])
    })

    test("pagination: every page from the configured offset, until the condition is false", async () => {
        useApis({ name: "Paged", url: fake.url + "/paged", pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, offset: 0, condition: response => response.data.length > 0 } })
        const warnings = []
        const results = await simpleSearch.searchLive("Rome", warnings)
        assert.deepEqual(fake.callsTo("/paged").map(c => c.query.offset), ["0", "2", "4", "6"])
        assert.deepEqual(results.map(r => r.raw.n), [4])
        assert.deepEqual(warnings, [])
    })

    test("pagination without condition stops at the first empty page", async () => {
        useApis({ name: "Paged", url: fake.url + "/paged?x=1", pagination: { offsetParam: "offset", limitParam: "limit", limit: 3 } })
        await simpleSearch.searchLive("Rome")
        assert.deepEqual(fake.callsTo("/paged").map(c => c.query), [{ x: "1", limit: "3", offset: "0" }, { x: "1", limit: "3", offset: "3" }, { x: "1", limit: "3", offset: "6" }])
    })

    test("pagination beyond maxPages: stops and warns that the results are incomplete", async () => {
        config.simpleSearchOptions.maxPages = 2
        useApis({ name: "Paged", url: fake.url + "/paged", pagination: { offsetParam: "offset", limitParam: "limit", limit: 2, condition: r => r.data.length > 0 } })
        const warnings = []
        const results = await simpleSearch.searchLive("Rome", warnings)
        assert.equal(fake.callsTo("/paged").length, 2)
        assert.deepEqual(results, [])
        assert.deepEqual(warnings.map(w => [w.kind, w.code, w.source]), [["runtime", "API_TRUNCATED", "Paged"]])
    })

    test("incremental: from the beginning - end date only, no start date", async () => {
        useApis({
            name: "Incr", url: fake.url + "/incremental", incremental: true, queryParams: { country: "IT" },
            incrementalParams: { startDateParam: "from", startDateFormat: "YYYY-MM-DD", endDateParam: "to", endDateFormat: "YYYY-MM-DD", endDateLogic: "inclusive" }
        })
        await simpleSearch.searchLive("Rome")
        const [call] = fake.callsTo("/incremental")
        assert.deepEqual(Object.keys(call.query).sort(), ["country", "to"])
        assert.equal(call.query.to, new Date().toISOString().split("T")[0])
    })

    test("a failing API is reported, the others are still searched", async () => {
        useApis({ name: "Broken", url: fake.url + "/fail" }, { name: "Single", url: fake.url + "/single" })
        const warnings = []
        const results = await simpleSearch.searchLive("Rome", warnings)
        assert.deepEqual(results.map(r => r.name), ["Single"])
        assert.deepEqual(warnings.map(w => [w.code, w.source]), [["API_ERROR", "Broken"]])
        assert.match(warnings[0].message, /HTTP 500/)
    })

    test("excluded APIs (simpleSearch: false) and api: false are not called", async () => {
        useApis({ name: "Plain", url: fake.url + "/plain", headers: oauth(), simpleSearch: false }, { name: "Single", url: fake.url + "/single" })
        assert.deepEqual((await simpleSearch.searchLive("Rome")).map(r => r.name), ["Single"])
        config.simpleSearchOptions.api = false
        assert.deepEqual(await simpleSearch.searchLive("Rome"), [])
        assert.equal(fake.callsTo("/plain").length, 0)
    })

    test("maxResults", async () => {
        config.simpleSearchOptions.maxResults = 1
        useApis({ name: "Plain", url: fake.url + "/plain", headers: oauth() })
        const warnings = []
        assert.equal((await simpleSearch.searchLive("", warnings)).length, 1)
        assert.deepEqual(codes(warnings), ["MAX_RESULTS"])
    })
})

describe("Orion", () => {
    beforeEach(() => {
        useApis()
        config.orion.orionBaseUrl = fake.url
        config.orion.subscribeType = "DistributionDCAT-AP"
        config.orion.attrWithUrl = "datasetUrl"
        Object.assign(config.authConfig, { idmHost: fake.url, authRealm: "test", clientId: "qe", username: "mapper-user", password: "pw" })
        config.getMapEndpoint = fake.url + "/api/map"
        config.mapEndpoint = fake.url + "/api/map/transform"
        config.parseEndpoint = fake.url + "/api/parse"
        config.sessionEndpoint = fake.url + "/api/output?"
        config.mapID = ""
    })

    test("off by default: Orion is never called", async () => {
        await simpleSearch.searchLive("Rome")
        assert.equal(fake.callsTo("/ngsi-ld/v1/entities").length, 0)
    })

    test("on: datasets downloaded as they are, or through the mapper when the entity has a mapID", async () => {
        config.simpleSearchOptions.orion = true
        const warnings = []
        const results = await simpleSearch.searchLive("Rome", warnings)
        const by = id => results.filter(r => r.record.orion == id).map(r => r.raw)
        assert.deepEqual(by("urn:e1"), [{ city: "Rome", value: 1 }])
        assert.deepEqual(by("urn:e2"), []) // CSV: not one of the entities the Source-Connector ingests
        assert.deepEqual(by("urn:e3"), [{ _id: "d1", region: "Rome", value: 10 }, { _id: "d3", region: "Rome", value: 12 }])
        assert.deepEqual(by("urn:e4"), [{ region: "Rome", value: 4 }]) // { data: { datapoints } } payload
        assert.deepEqual(warnings, [])
        assert.equal(results.find(r => r.record.orion == "urn:e1").record.from, fake.url + "/dataset/1")

        // mapper: no map (404) -> parse, then the output chunks until an empty one
        assert.equal(fake.callsTo("/api/parse")[0].body.sourceDataURL, fake.url + "/dataset/3")
        assert.deepEqual(fake.callsTo("/api/output").map(c => c.query.index), ["0", "1", "2"])
        assert.equal(fake.callsTo("/api/output")[1].query.lastId, "d2")
        assert.ok(results.every(r => objectFilter(r, "x@y.it", "pilot", "public")))
    })

    test("maxOrionEntities", async () => {
        Object.assign(config.simpleSearchOptions, { orion: true, maxOrionEntities: 1 })
        const warnings = []
        const results = await simpleSearch.searchLive("Rome", warnings)
        assert.deepEqual([...new Set(results.map(r => r.record.orion))], ["urn:e1"])
        assert.deepEqual(codes(warnings), ["ORION_TRUNCATED"])
    })

    test("Orion unreachable: reported", async () => {
        config.simpleSearchOptions.orion = true
        config.orion.orionBaseUrl = "http://127.0.0.1:1"
        const warnings = []
        assert.deepEqual(await simpleSearch.searchLive("Rome", warnings), [])
        assert.deepEqual(codes(warnings), ["ORION_ERROR"])
    })
})
