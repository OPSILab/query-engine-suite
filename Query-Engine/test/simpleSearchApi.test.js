// GET /api/query (Simple search: isRawQuery) and GET /api/query/simple/limits through the real router and auth
// middleware. MinIO is stubbed, the APIs / Orion are the local fake sources.
const { stub, load, config, resetConfig } = require("./helpers/env")
const { startFakeSources } = require("./helpers/fakeSources")
const { publicKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

const minioFiles = {
    "public-data": [{ name: "rome.json", size: 10, isLatest: true, content: { city: "Rome" } }, { name: "oslo.json", size: 10, isLatest: true, content: { city: "Oslo" } }],
    pilot: [{ name: "anna@demetrix.it/data model mapper/mine.json", size: 5, isLatest: true, content: { city: "Rome", mine: true } }]
}
stub("inputConnectors/minioConnector.js", {
    listObjects: async bucket => (minioFiles[bucket] || []).map(({ content, ...obj }) => obj),
    getObject: async (bucket, name) => minioFiles[bucket].find(f => f.name == name).content
})
stub("inputConnectors/postgresConnector.js", () => ({ query: () => { } }))

let fake, server, baseUrl

before(async () => {
    fake = await startFakeSources()
    const router = load("api/routes/router.js")
    const app = express()
    app.use(express.json())
    app.use("/api", router)
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    baseUrl = `http://127.0.0.1:${server.address().port}/api`
})
after(async () => {
    server?.close()
    await fake?.close()
})
beforeEach(() => {
    resetConfig()
    console.debug = () => { }
    Object.assign(config.authConfig, { disableAuth: true, clientId: "query-engine", publicKey, userInfoEndpoint: "", introspect: false })
    config.minioConfig.defaultBucket = "pilot"
    config.apiConnectorConfig.apiUrls = [{ name: "Single", url: fake.url + "/single" }, { name: "Broken", url: fake.url + "/fail" }]
})

async function simpleSearch(value, { visibility = "public", token, collections } = {}) {
    const res = await fetch(`${baseUrl}/query?value=${encodeURIComponent(value)}` + (collections !== undefined ? "&collections=" + collections : ""), {
        headers: { isRawQuery: "yes", visibility, ...(token ? { Authorization: "Bearer " + token } : {}) }
    })
    const header = res.headers.get("x-query-warnings")
    return { status: res.status, body: res.status == 200 ? await res.json() : undefined, warnings: header ? JSON.parse(decodeURIComponent(header)) : [] }
}

describe("GET /api/query (simple search)", () => {
    test("public: MinIO public-data files and live API records; warnings in X-Query-Warnings", async () => {
        const { status, body, warnings } = await simpleSearch("Rome")
        assert.equal(status, 200)
        assert.deepEqual(body.map(r => r.name), ["rome.json", "Single"])
        assert.deepEqual(body[1].record, { from: fake.url + "/single", api: "Single" })
        assert.deepEqual(warnings.map(w => [w.kind, w.code]), [["config", "ORION_DISABLED"], ["runtime", "API_ERROR"]])
    })

    test("no value (Find all): every visible MinIO file and every API record, no text filter", async () => {
        config.apiConnectorConfig.apiUrls = [{ name: "Single", url: fake.url + "/single" }, { name: "Paged", url: fake.url + "/paged", pagination: { offsetParam: "offset", limitParam: "limit", limit: 2 } }]
        const { body } = await simpleSearch("")
        assert.deepEqual(body.map(r => r.name).sort(), ["Paged", "Paged", "Paged", "Paged", "Paged", "Single", "oslo.json", "rome.json"])
    })

    test("with authentication: private searches the user's MinIO files only, no live sources", async () => {
        config.authConfig.disableAuth = false
        const token = makeToken({ azp: "query-engine", email: "anna@demetrix.it/data model mapper" })
        const { body, warnings } = await simpleSearch("Rome", { visibility: "private", token })
        assert.deepEqual(body.map(r => r.name), ["anna@demetrix.it/data model mapper/mine.json"])
        assert.deepEqual(warnings, [])
        assert.equal(fake.callsTo("/single").length, 2) // only the previous tests' calls
    })

    test("with authentication: public includes the live sources", async () => {
        config.authConfig.disableAuth = false
        const token = makeToken({ azp: "query-engine", email: "anna@demetrix.it" })
        const { body } = await simpleSearch("Rome", { token })
        assert.deepEqual(body.map(r => r.name), ["rome.json", "Single"])
    })

    test("MinIO turned off: only the live sources, and it is reported", async () => {
        config.simpleSearchOptions.minio = false
        config.apiConnectorConfig.apiUrls = [{ name: "Single", url: fake.url + "/single" }]
        const { body, warnings } = await simpleSearch("Rome")
        assert.deepEqual(body.map(r => r.name), ["Single"])
        assert.deepEqual(warnings.map(w => w.code), ["MINIO_DISABLED", "ORION_DISABLED"])
    })

    test("no warnings: no header", async () => {
        config.simpleSearchOptions.orion = true
        config.orion.orionBaseUrl = fake.url
        config.apiConnectorConfig.apiUrls = []
        config.authConfig.disableAuth = false
        const token = makeToken({ azp: "query-engine", email: "anna@demetrix.it/data model mapper" })
        const res = await fetch(`${baseUrl}/query?value=Rome`, { headers: { isRawQuery: "yes", visibility: "private", Authorization: "Bearer " + token } })
        assert.equal(res.headers.get("x-query-warnings"), null)
    })
})

describe("GET /api/query/simple/limits", () => {
    test("what the configuration leaves out", async () => {
        config.apiConnectorConfig.apiUrls.push({ name: "Huge", url: "x", simpleSearch: false })
        const res = await fetch(baseUrl + "/query/simple/limits")
        assert.deepEqual((await res.json()).warnings.map(w => [w.code, w.source]), [["API_EXCLUDED", "Huge"], ["ORION_DISABLED", undefined]])
    })

    test("requires authentication when it is enabled", async () => {
        config.authConfig.disableAuth = false
        assert.equal((await fetch(baseUrl + "/query/simple/limits")).status, 401)
    })
})

describe("collections", () => {
    test("simple search on the chosen collections only (minio: the files, api / orion: live)", async () => {
        const files = await simpleSearch("Rome", { collections: "minio" })
        assert.deepEqual([files.body.map(r => r.name), files.warnings], [["rome.json"], []])
        const api = await simpleSearch("Rome", { collections: "api" })
        assert.deepEqual(api.body.map(r => r.name), ["Single"])
        assert.deepEqual(api.warnings.map(w => w.code), ["API_ERROR"]) // ORION_DISABLED: Orion not chosen
        assert.equal((await simpleSearch("Rome", { collections: "ftp" })).status, 400)
    })

    test("GET /api/collections: what each collection offers", async () => {
        config.collections.orion.toMongo = false
        config.simpleSearchOptions = { orion: true, api: false }
        const res = await fetch(baseUrl + "/collections")
        assert.deepEqual(await res.json(), { collections: [
            { id: "api", default: true, advancedSearch: true, simpleSearch: false },
            { id: "orion", default: false, advancedSearch: false, simpleSearch: true },
            { id: "minio", default: true, advancedSearch: true, simpleSearch: true }
        ], limits: { advancedSearch: 1000, suggestions: 500 } })
        config.queryOptions.defaultCollections = ["orion"]
        config.queryOptions.advancedSearchMaxResults = 5000
        config.queryOptions.suggestionsMaxResults = 200
        const custom = await (await fetch(baseUrl + "/collections")).json()
        assert.deepEqual(custom.collections.map(c => [c.id, c.default]), [["api", false], ["orion", true], ["minio", false]])
        assert.deepEqual(custom.limits, { advancedSearch: 5000, suggestions: 200 })
    })
})
