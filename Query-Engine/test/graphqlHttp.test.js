// /graphql over HTTP as index.js mounts it (api/graphql/mount.js): Apollo Server 5 on express, the auth middleware,
// CORS, the CSRF prevention. On a real MongoDB (see helpers/db.js: npm run test:db).
const { stub, load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { publicKey, makeToken } = require("./helpers/jwt")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")
const express = require("express")

stub("api/services/translationService.js", { translateDataPointsBatch: async dps => dps })

let server, apollo, baseUrl

before(async () => {
    await db.setup(__filename)
    await load("api/services/collections.js").collectionModel("api").collection.insertMany([
        { name: "Item One", source: "https://api.example.org/items" }
    ])
    const { ApolloServer } = require("@apollo/server")
    apollo = new ApolloServer({ typeDefs: load("api/graphql/typeDefs.js"), resolvers: load("api/graphql/resolvers.js") })
    await apollo.start()
    const app = express()
    load("api/graphql/mount.js").mountGraphql(app, apollo, { auth: load("api/middlewares/auth.js").auth, corsOptions: { exposedHeaders: ["X-Query-Warnings"] } })
    await new Promise(resolve => { server = app.listen(0, "127.0.0.1", resolve) })
    baseUrl = `http://127.0.0.1:${server.address().port}/graphql`
})
after(async () => {
    server?.close()
    await apollo?.stop()
    await db.teardown()
})
beforeEach(() => {
    resetConfig()
    Object.assign(config.authConfig, { disableAuth: true, clientId: "query-engine", publicKey, userInfoEndpoint: "", introspect: false })
})

const post = (body, headers = {}) => fetch(baseUrl, { method: "POST", headers: { "Content-Type": "application/json", visibility: "public", ...headers }, body: JSON.stringify(body) })

describe("/graphql over HTTP", () => {
    test("a POST JSON query, as the frontend sends it: the data", async () => {
        const res = await post({ query: "{ sources(collections: [\"api\"]) { name } }" })
        assert.equal(res.status, 200)
        assert.deepEqual((await res.json()).data.sources.map(s => s.name), ["Item One"])
        assert.equal(res.headers.get("access-control-allow-origin"), "*")
    })

    test("CSRF prevention: a GET without a preflight header is refused; with apollo-require-preflight it answers", async () => {
        const url = baseUrl + "?query=" + encodeURIComponent("{ sourcesCount }")
        assert.equal((await fetch(url)).status, 400)
        const res = await fetch(url, { headers: { "apollo-require-preflight": "true", visibility: "public" } })
        assert.equal(res.status, 200)
        assert.equal(typeof (await res.json()).data.sourcesCount, "number")
    })

    test("a wrong argument: 200 with the error, code BAD_USER_INPUT (as UserInputError of Apollo Server 3)", async () => {
        const body = await (await post({ query: "{ sources(limit: 0) { name } }" })).json()
        assert.match(body.errors[0].message, /limit/)
        assert.equal(body.errors[0].extensions.code, "BAD_USER_INPUT")
    })

    test("authentication enabled: no token 401, a valid token 200", async () => {
        config.authConfig.disableAuth = false
        assert.equal((await post({ query: "{ sourcesCount }" })).status, 401)
        const token = makeToken({ azp: "query-engine", email: "anna@demetrix.it", pilot: "pilot" })
        assert.equal((await post({ query: "{ sourcesCount }" }, { Authorization: "Bearer " + token })).status, 200)
    })
})
