// auth middleware, called directly with fake req/res (no network: userInfoEndpoint and introspect are off).
const { load, config, resetConfig } = require("./helpers/env")
const { publicKey, otherPrivateKey, makeToken } = require("./helpers/jwt")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

const { auth } = load("api/middlewares/auth.js")

const CLIENT = "query-engine"

function fakeRes() {
    const res = { statusCode: undefined, body: undefined, headers: {} }
    res.set = (name, value) => { res.headers[name.toLowerCase()] = value; return res }
    res.status = code => { res.statusCode = code; return res }
    res.send = body => { res.body = body; return res }
    res.sendStatus = code => { res.statusCode = code; return res }
    return res
}

async function run({ headers = {}, body, isGraphql } = {}) {
    const req = { headers: { ...headers }, body, isGraphql }
    const res = fakeRes()
    let nextCalled = false
    await auth(req, res, () => { nextCalled = true })
    return { req, res, next: nextCalled }
}

const bearer = token => ({ authorization: "Bearer " + token })

beforeEach(() => {
    resetConfig()
    Object.assign(config.authConfig, { clientId: CLIENT, publicKey, userInfoEndpoint: "", introspect: false, disableAuth: false })
    delete config.authConfig.publicKeys
    config.enableQueryControl = false
    console.debug = () => { } // auth.js logs every request's headers
})

describe("authentication disabled", () => {
    beforeEach(() => { config.authConfig.disableAuth = true })

    test("lets everything through with the default bucket and folder", async () => {
        const { req, next } = await run()
        assert.equal(next, true)
        assert.equal(req.body.bucketName, config.minioConfig.defaultBucket)
        assert.equal(req.body.prefix, config.minioConfig.defaultInputFolderName)
        assert.equal(req.headers.visibility, "private") // default visibility
    })

    test("GraphQL requests too", async () => {
        const { next } = await run({ isGraphql: true, body: { query: "{ sources { name } }" } })
        assert.equal(next, true)
    })
})

describe("authentication enabled", () => {
    test("401 without a token, with WWW-Authenticate: Bearer", async () => {
        const { res, next } = await run()
        assert.equal(next, false)
        assert.equal(res.statusCode, 401)
        assert.equal(res.headers["www-authenticate"], "Bearer")
    })

    test("401 for GraphQL without a token", async () => {
        const { res, next } = await run({ isGraphql: true, body: { query: "{ sources { name } }" } })
        assert.equal(next, false)
        assert.equal(res.statusCode, 401)
    })

    test("a valid token sets the user's prefix and bucket", async () => {
        const { req, next } = await run({ headers: bearer(makeToken({ azp: CLIENT, email: "anna@demetrix.it" })) })
        assert.equal(next, true)
        assert.equal(req.body.prefix, "anna@demetrix.it")
        assert.equal(req.body.bucketName, config.minioConfig.defaultBucket)
    })

    test("the Bearer prefix is optional", async () => {
        const { next } = await run({ headers: { authorization: makeToken({ azp: CLIENT, email: "a@b.it" }) } })
        assert.equal(next, true)
    })

    test("401 invalid_token (RFC 6750): expired, other client, malformed, signed with another key", async () => {
        for (const [headers, reason] of [
            [bearer(makeToken({ azp: CLIENT }, { expiresIn: -60 })), /jwt expired/],
            [bearer(makeToken({ azp: "another-client" })), /not issued for this client/],
            [bearer("not.a.jwt"), /invalid token|jwt malformed/],
            [bearer(makeToken({ azp: CLIENT }, { key: otherPrivateKey })), /invalid signature/]
        ]) {
            const { res, next } = await run({ headers })
            assert.equal(next, false)
            assert.equal(res.statusCode, 401, String(reason))
            assert.match(res.headers["www-authenticate"], /^Bearer error="invalid_token", error_description="/)
            assert.match(res.headers["www-authenticate"], reason)
        }
    })

    test("publicKeys: an expired token is 401 at once; a token no key validates is 401", async () => {
        config.authConfig.publicKeys = ["-----BEGIN PUBLIC KEY-----\nnot a key\n-----END PUBLIC KEY-----", publicKey]
        let { res } = await run({ headers: bearer(makeToken({ azp: CLIENT }, { expiresIn: -60 })) })
        assert.equal(res.statusCode, 401)
        config.authConfig.publicKeys = [publicKey]
        res = (await run({ headers: bearer(makeToken({ azp: CLIENT }, { key: otherPrivateKey })) })).res
        assert.equal(res.statusCode, 401)
        assert.match(res.headers["www-authenticate"], /invalid signature/)
    })

    test("publicKeys: any of the configured keys validates the token", async () => {
        config.authConfig.publicKeys = ["-----BEGIN PUBLIC KEY-----\nnot a key\n-----END PUBLIC KEY-----", publicKey]
        const { next } = await run({ headers: bearer(makeToken({ azp: CLIENT, email: "a@b.it" })) })
        assert.equal(next, true)
    })
})

describe("SQL checks (REST only)", () => {
    test("public visibility requires a query on public-data", async () => {
        config.authConfig.disableAuth = true
        const { res, next } = await run({ headers: { visibility: "public" }, body: { query: "SELECT * FROM pilot" } })
        assert.equal(next, false)
        assert.equal(res.statusCode, 400)
    })

    test("SELECT * FROM public-data is rewritten to the publicdata table", async () => {
        config.authConfig.disableAuth = true
        const { req } = await run({ headers: { visibility: "public" }, body: { query: "SELECT * FROM public-data WHERE a = 1" } })
        assert.equal(req.body.query, "SELECT * FROM publicdata WHERE a = 1")
    })

    test("not applied to GraphQL documents", async () => {
        config.authConfig.disableAuth = true
        const query = "{ sources { name } }"
        const { req, next } = await run({ isGraphql: true, headers: { visibility: "public" }, body: { query } })
        assert.equal(next, true)
        assert.equal(req.body.query, query)
    })

    test("enableQueryControl: other buckets / other users' objects are denied, own ones allowed", async () => {
        config.enableQueryControl = true
        const token = bearer(makeToken({ azp: CLIENT, email: "anna@demetrix.it" }))
        const bucket = config.minioConfig.defaultBucket

        const denied = await run({ headers: token, body: { query: "SELECT * FROM otherbucket WHERE name = 'x'" } })
        assert.equal(denied.res.statusCode, 403)

        const otherUser = await run({ headers: token, body: { query: `SELECT * FROM ${bucket} WHERE name = 'bob@demetrix.it/f.json'` } })
        assert.equal(otherUser.res.statusCode, 403)

        const own = await run({ headers: token, body: { query: `SELECT * FROM ${bucket} WHERE name = 'anna@demetrix.it/f.json'` } })
        assert.equal(own.next, true)

        const graphql = await run({ headers: token, isGraphql: true, body: { query: "{ sources { name } }" } })
        assert.equal(graphql.next, true)
    })
})
