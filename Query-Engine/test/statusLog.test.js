// GET /api/statuslog (services/statusLog.js) through the real router and auth, on a real MongoDB (see helpers/db.js):
// the failed requests of the Source-Connector, filtered by date / url, the header values hidden unless admin.
const { stub, load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { publicKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/postgresConnector.js", () => ({ query: () => { } }))

// newest last here; the endpoint answers newest first
const DOCS = [
    { createdAt: new Date("2026-10-01T10:00:00Z"), api: "Records", method: "GET", url: "https://api.example.org/records", fullUrl: "https://api.example.org/records?from=2026-09-01", params: { from: "2026-09-01" }, headers: { Authorization: "Bearer secret", "X-Api-Key": "k" }, status: 500, data: "{\"message\":\"boom\"}" },
    { createdAt: new Date("2026-10-15T12:00:00Z"), host: "orion.example", method: "GET", url: "https://orion.example/ngsi-ld/v1/entities", fullUrl: "https://orion.example/ngsi-ld/v1/entities", headers: { Accept: "application/json" }, error: "connect ECONNREFUSED", code: "ECONNREFUSED" },
    // headers saved as JSON text (MongoDB refused their field names)
    { createdAt: new Date("2026-10-31T23:00:00Z"), api: "Records", method: "POST", url: "https://API.example.org/records/7", fullUrl: "https://API.example.org/records/7", headers: "{\"Authorization\":\"Bearer other\"}", body: "{\"id\":7}", status: 404 }
]

let server, baseUrl, StatusLog

before(async () => {
    await db.setup(__filename)
    StatusLog = load("api/models/StatusLog.js")
    await StatusLog.collection.insertMany(DOCS.map(doc => ({ ...doc })))
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
    Object.assign(config.authConfig, { disableAuth: false, clientId: "query-engine", publicKey, userInfoEndpoint: "", introspect: false })
})

async function statusLog(query = {}, token) {
    const res = await fetch(`${baseUrl}/statuslog?${new URLSearchParams(query)}`, { headers: token ? { Authorization: "Bearer " + token } : {} })
    return { status: res.status, body: res.status == 200 ? await res.json() : await res.text() }
}
const userToken = (extra = {}) => makeToken({ azp: "query-engine", email: "anna@demetrix.it", ...extra })
const urls = body => body.items.map(item => item.url)

describe("who sees the header values", () => {
    test("authentication disabled: every record, newest first, header values hidden (keys kept)", async () => {
        config.authConfig.disableAuth = true
        const { status, body } = await statusLog()
        assert.equal(status, 200)
        assert.deepEqual(urls(body), ["https://API.example.org/records/7", "https://orion.example/ngsi-ld/v1/entities", "https://api.example.org/records"])
        assert.equal(body.total, 3)
        assert.equal(body.masked, true)
        assert.deepEqual(body.items.map(item => item.headers), [
            { Authorization: "<masked>" },
            { Accept: "<masked>" },
            { Authorization: "<masked>", "X-Api-Key": "<masked>" }
        ])
        // the rest as saved
        assert.deepEqual([body.items[2].status, body.items[2].data, body.items[2].params], [500, "{\"message\":\"boom\"}", { from: "2026-09-01" }])
        assert.equal(body.items[0].body, "{\"id\":7}")
    })

    test("authentication on, no token: readable, values hidden", async () => {
        const { status, body } = await statusLog()
        assert.equal(status, 200)
        assert.equal(body.masked, true)
        assert.equal(body.items[2].headers.Authorization, "<masked>")
    })

    test("a valid token, adminRoles empty: the values in clear", async () => {
        const { body } = await statusLog({}, userToken())
        assert.equal(body.masked, false)
        assert.deepEqual(body.items[2].headers, { Authorization: "Bearer secret", "X-Api-Key": "k" })
        assert.equal(body.items[0].headers, "{\"Authorization\":\"Bearer other\"}") // as saved
    })

    test("adminRoles: in clear only with one of them", async () => {
        config.authConfig.adminRoles = ["qe-admin"]
        assert.equal((await statusLog({}, userToken({ realm_access: { roles: ["user"] } }))).body.masked, true)
        assert.equal((await statusLog({}, userToken({ realm_access: { roles: ["qe-admin"] } }))).body.masked, false)
        assert.equal((await statusLog({}, userToken({ resource_access: { "query-engine": { roles: ["qe-admin"] } } }))).body.masked, false)
    })

    test("an invalid token: 403", async () => {
        assert.equal((await statusLog({}, "not-a-token")).status, 403)
    })

    test("the records in the database keep their values", async () => {
        config.authConfig.disableAuth = true
        await statusLog()
        const saved = await StatusLog.collection.find({ url: "https://api.example.org/records" }).toArray()
        assert.equal(saved[0].headers.Authorization, "Bearer secret")
    })
})

describe("filters and pages", () => {
    beforeEach(() => { config.authConfig.disableAuth = true })

    test("from / to: ISO dates; a date alone as `to` is that whole day", async () => {
        assert.deepEqual(urls((await statusLog({ from: "2026-10-10" })).body), ["https://API.example.org/records/7", "https://orion.example/ngsi-ld/v1/entities"])
        assert.deepEqual(urls((await statusLog({ to: "2026-10-15" })).body), ["https://orion.example/ngsi-ld/v1/entities", "https://api.example.org/records"])
        assert.deepEqual(urls((await statusLog({ from: "2026-10-15T00:00:00Z", to: "2026-10-15T11:59:59Z" })).body), [])
        const { body } = await statusLog({ from: "2026-10-01T10:00:00Z", to: "2026-10-01T10:00:00Z" })
        assert.deepEqual([urls(body), body.total], [["https://api.example.org/records"], 1])
    })

    test("url: part of url or fullUrl, case insensitive, taken literally", async () => {
        assert.deepEqual(urls((await statusLog({ url: "api.example.org/records" })).body), ["https://API.example.org/records/7", "https://api.example.org/records"])
        assert.deepEqual(urls((await statusLog({ url: "records?from=" })).body), ["https://api.example.org/records"]) // ? not a regex
        assert.deepEqual(urls((await statusLog({ url: "orion", from: "2026-10-20" })).body), [])
    })

    test("limit / skip: a page, total of every match", async () => {
        const first = (await statusLog({ limit: 2 })).body
        assert.deepEqual([first.items.length, first.total], [2, 3])
        const second = (await statusLog({ limit: 2, skip: 2 })).body
        assert.deepEqual(urls(second), ["https://api.example.org/records"])
    })

    test("invalid filters: 400", async () => {
        for (const query of [{ from: "yesterday" }, { to: "2026-13-45" }, { limit: 0 }, { limit: 1001 }, { limit: 5, skip: -1 }])
            assert.equal((await statusLog(query)).status, 400, JSON.stringify(query))
    })
})

describe("credentials in params, urls, body and response", () => {
    const TOKEN_REQUEST = {
        createdAt: new Date("2026-10-20T08:00:00Z"), api: "Records", method: "POST", url: "https://auth.example/token",
        fullUrl: "https://auth.example/token?realm=demo&api_key=K1", params: { realm: "demo", api_key: "K1" },
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "client_id=query-engine&client_secret=S3CRET&grant_type=client_credentials",
        status: 401, data: JSON.stringify({ error: "invalid_client", access_token: "T0KEN", expires_in: 300 })
    }
    const CUT = { createdAt: new Date("2026-10-20T09:00:00Z"), url: "https://api.example.org/login", body: "{\"user\":\"anna\",\"password\":\"pw\"}", data: "{\"refresh_token\":\"eyJhbG", dataTruncated: true, status: 500 }
    before(async () => { await StatusLog.collection.insertMany([{ ...TOKEN_REQUEST }, { ...CUT }]) })
    after(async () => { await StatusLog.collection.deleteMany({ createdAt: { $in: [TOKEN_REQUEST.createdAt, CUT.createdAt] } }) })
    const find = (body, url) => body.items.find(item => item.url == url)

    test("not an admin: the values of the sensitive fields hidden, the rest kept", async () => {
        config.authConfig.disableAuth = true
        const { body } = await statusLog({ from: "2026-10-20", to: "2026-10-20" })
        const token = find(body, "https://auth.example/token")
        assert.deepEqual(token.params, { realm: "demo", api_key: "<masked>" })
        assert.equal(token.fullUrl, "https://auth.example/token?realm=demo&api_key=<masked>")
        assert.equal(token.body, "client_id=query-engine&client_secret=<masked>&grant_type=client_credentials")
        assert.deepEqual(JSON.parse(token.data), { error: "invalid_client", access_token: "<masked>", expires_in: 300 })
        const cut = find(body, "https://api.example.org/login")
        assert.deepEqual(JSON.parse(cut.body), { user: "anna", password: "<masked>" })
        assert.equal(cut.data, "{\"refresh_token\":\"<masked>\"") // a cut JSON text: by pattern
    })

    test("an admin: everything as saved", async () => {
        const { body } = await statusLog({ from: "2026-10-20", to: "2026-10-20" }, userToken())
        const token = find(body, "https://auth.example/token")
        assert.deepEqual([token.params, token.fullUrl, token.body, token.data], [TOKEN_REQUEST.params, TOKEN_REQUEST.fullUrl, TOKEN_REQUEST.body, TOKEN_REQUEST.data])
        assert.equal(find(body, "https://api.example.org/login").data, CUT.data)
    })
})

describe("ignored urls", () => {
    const IGNORED = "https://api.example.org/records"
    const ignore = (url, token, headers = {}) => fetch(`${baseUrl}/statuslog/ignored`, {
        method: "POST", headers: { "Content-Type": "application/json", ...(token ? { Authorization: "Bearer " + token } : {}), ...headers }, body: JSON.stringify({ url })
    })
    const restore = (url, token, headers = {}) => fetch(`${baseUrl}/statuslog/ignored?${new URLSearchParams({ url })}`, {
        method: "DELETE", headers: { ...(token ? { Authorization: "Bearer " + token } : {}), ...headers }
    })
    const ignoredList = async token => (await fetch(`${baseUrl}/statuslog/ignored`, { headers: token ? { Authorization: "Bearer " + token } : {} })).json()
    const StatusLogIgnored = () => load("api/models/StatusLogIgnored.js")
    after(async () => { await StatusLogIgnored().collection.deleteMany({}) })

    test("only admins change them: 401 without a token, 403 without the role, 400 without url", async () => {
        config.authConfig.adminRoles = ["qe-admin"]
        assert.equal((await ignore(IGNORED)).status, 401)
        assert.equal((await ignore(IGNORED, userToken({ realm_access: { roles: ["user"] } }))).status, 403)
        assert.equal((await restore(IGNORED, userToken({ realm_access: { roles: ["user"] } }))).status, 403)
        assert.equal((await ignore("", userToken({ realm_access: { roles: ["qe-admin"] } }))).status, 400)
        assert.deepEqual((await ignoredList()).urls, [])
    })

    test("authentication disabled: only with the X-Admin-Token", async () => {
        Object.assign(config.authConfig, { disableAuth: true, adminToken: "s3cret" })
        assert.equal((await ignore(IGNORED)).status, 403)
        assert.equal((await ignore(IGNORED, undefined, { "X-Admin-Token": "s3cret" })).status, 200)
        assert.equal((await restore(IGNORED, undefined, { "X-Admin-Token": "s3cret" })).status, 200)
        assert.deepEqual((await ignoredList()).urls, [])
    })

    test("an ignored url: left out without a url filter (dates included), listed with one", async () => {
        const res = await ignore(IGNORED, userToken())
        assert.equal(res.status, 200)
        assert.deepEqual((await res.json()).urls.map(doc => [doc.url, doc.by]), [[IGNORED, "anna@demetrix.it"]])
        assert.equal((await ignore(IGNORED, userToken())).status, 200) // twice: still once
        assert.equal((await ignoredList(userToken())).urls.length, 1)

        config.authConfig.disableAuth = true
        let { body } = await statusLog()
        assert.deepEqual(urls(body), ["https://API.example.org/records/7", "https://orion.example/ngsi-ld/v1/entities"]) // not "records/7": another url
        assert.deepEqual([body.total, body.ignoredExcluded], [2, 1])
        assert.deepEqual(urls((await statusLog({ to: "2026-10-05" })).body), []) // a date filter does not bring it back
        body = (await statusLog({ url: "example.org/records" })).body
        assert.deepEqual(body.items.map(item => [item.url, item.ignored === true]), [["https://API.example.org/records/7", false], [IGNORED, true]])
        assert.equal(body.ignoredExcluded, 0)
        // non admins: the list with the requester hidden
        assert.deepEqual((await ignoredList()).urls.map(doc => [doc.url, doc.by]), [[IGNORED, "<masked>"]])
    })

    test("statusLog.ignoredUrls of the config: left out too, listed first, not restored from the API", async () => {
        const ORION = "https://orion.example/ngsi-ld/v1/entities"
        config.statusLog.ignoredUrls = [ORION, ORION, "", 7]
        const list = (await ignoredList(userToken())).urls
        assert.deepEqual(list.map(doc => [doc.url, doc.fromConfig === true]), [[ORION, true], [IGNORED, false]])
        assert.equal((await restore(ORION, userToken())).status, 409)
        assert.equal((await ignore(ORION, userToken())).status, 200) // already ignored: nothing saved
        assert.equal(await StatusLogIgnored().collection.countDocuments({ url: ORION }), 0)
        config.authConfig.disableAuth = true
        const { body } = await statusLog()
        assert.deepEqual([urls(body), body.ignoredExcluded], [["https://API.example.org/records/7"], 2])
        assert.deepEqual(urls((await statusLog({ url: "orion" })).body), [ORION])
    })

    test("restored: listed again", async () => {
        const res = await restore(IGNORED, userToken())
        assert.deepEqual([res.status, (await res.json()).urls], [200, []])
        config.authConfig.disableAuth = true
        const { body } = await statusLog()
        assert.deepEqual([body.total, body.ignoredExcluded], [3, 0])
    })
})
