// Cache management endpoints (reset / backup / restore): auth + adminOnly, through the real router. The service is
// stubbed: only who gets through is tested here.
const { stub, load, config, resetConfig } = require("./helpers/env")
const { publicKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

const calls = []
const done = name => async () => { calls.push(name); return name + " done" }
stub("api/services/service.js", { resetCache: done("resetCache"), backupCache: done("backupCache"), restoreCache: done("restoreCache"), resetBackup: done("resetBackup"), listCache: done("listCache") })
stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/postgresConnector.js", () => ({ query: () => { } }))

let server, baseUrl

before(async () => {
    const app = express()
    app.use(express.json())
    app.use("/api", load("api/routes/router.js"))
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    baseUrl = `http://127.0.0.1:${server.address().port}/api`
})
after(() => server?.close())
beforeEach(() => {
    resetConfig()
    calls.length = 0
    console.debug = () => { }
    Object.assign(config.authConfig, { disableAuth: false, clientId: "query-engine", publicKey, userInfoEndpoint: "", introspect: false })
})

const ENDPOINTS = [["POST", "/minio/resetCache"], ["GET", "/backupCache"], ["GET", "/restoreCache"], ["POST", "/resetBackup"], ["GET", "/listCache"]]
const call = (method, path, token) => fetch(baseUrl + path, { method, headers: token ? { Authorization: "Bearer " + token } : {} })
const userToken = (extra = {}) => makeToken({ azp: "query-engine", email: "anna@demetrix.it", ...extra })

describe("cache endpoints", () => {
    test("authentication on: 401 without a token, reached with a valid one (adminRoles empty)", async () => {
        for (const [method, path] of ENDPOINTS) {
            assert.equal((await call(method, path)).status, 401, path)
            assert.equal((await call(method, path, userToken())).status, 200, path)
        }
        assert.deepEqual(calls, ["resetCache", "backupCache", "restoreCache", "resetBackup", "listCache"])
    })

    test("adminRoles: only tokens with one of them (realm or client roles)", async () => {
        config.authConfig.adminRoles = ["qe-admin"]
        const [method, path] = ENDPOINTS[0]
        const denied = await call(method, path, userToken({ realm_access: { roles: ["user"] } }))
        assert.equal(denied.status, 403)
        assert.match(await denied.text(), /qe-admin/)
        assert.equal((await call(method, path, userToken({ realm_access: { roles: ["qe-admin"] } }))).status, 200)
        assert.equal((await call(method, path, userToken({ resource_access: { "query-engine": { roles: ["qe-admin"] } } }))).status, 200)
        assert.equal((await call(method, path, userToken({ resource_access: { other: { roles: ["qe-admin"] } } }))).status, 403)
        assert.deepEqual(calls, ["resetCache", "resetCache"])
    })

    test("authentication disabled, no adminToken: closed (403), nothing called", async () => {
        Object.assign(config.authConfig, { disableAuth: true, adminRoles: ["qe-admin"] })
        for (const [method, path] of ENDPOINTS) {
            const res = await call(method, path)
            assert.equal(res.status, 403, path)
            assert.match(await res.text(), /adminToken/)
        }
        assert.deepEqual(calls, [])
    })

    test("authentication disabled with adminToken: only with the right X-Admin-Token (adminRoles not checked)", async () => {
        Object.assign(config.authConfig, { disableAuth: true, adminRoles: ["qe-admin"], adminToken: "s3cret" })
        const withHeader = (method, path, value) => fetch(baseUrl + path, { method, headers: { "X-Admin-Token": value } })
        for (const [method, path] of ENDPOINTS) {
            assert.equal((await call(method, path)).status, 403, path)
            assert.equal((await withHeader(method, path, "wrong")).status, 403, path)
            assert.equal((await withHeader(method, path, "s3cret")).status, 200, path)
        }
        assert.deepEqual(calls, ["resetCache", "backupCache", "restoreCache", "resetBackup", "listCache"])
    })
})
