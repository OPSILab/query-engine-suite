const { load } = require("./helpers/env")
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")

const common = load("utils/common.js")

function runBodyCheck(body, query = {}) {
    const req = { body, query }
    let nextCalled = false
    return common.bodyCheck(req, {}, () => { nextCalled = true }).then(() => ({ req, nextCalled }))
}

describe("bodyCheck", () => {
    test("parses JSON-encoded values of mongoQuery (and of the query string)", async () => {
        const { req, nextCalled } = await runBodyCheck(
            { mongoQuery: { age: '{"$gte":18}', name: "Anna", count: "3" } },
            { limit: "10" }
        )
        assert.equal(nextCalled, true)
        assert.deepEqual(req.body.mongoQuery, { age: { $gte: 18 }, name: "Anna", count: 3 })
        assert.deepEqual(req.query, { limit: 10 })
    })

    test("drops the empty range produced by the form", async () => {
        const { req } = await runBodyCheck({ mongoQuery: { "": '{"$gte":null,"$lte":null}', k: '"v"' } })
        assert.deepEqual(req.body.mongoQuery, { k: "v" })
    })

    test("leaves SQL requests (body.query) alone", async () => {
        const { req } = await runBodyCheck({ query: "SELECT 1", mongoQuery: { a: "1" } })
        assert.deepEqual(req.body.mongoQuery, { a: "1" })
    })
})

describe("checkConfig", () => {
    test("fills missing keys from the template, keeps the existing ones", () => {
        assert.deepEqual(
            common.checkConfig({ a: 1, n: { x: 1 } }, { a: 2, b: 3, n: { x: 2, y: 2 } }),
            { a: 1, b: 3, n: { x: 1, y: 2 } }
        )
    })
})

describe("json2csv / parseJwt / urlEncode", () => {
    test("helpers", () => {
        assert.equal(common.json2csv({ a: 1 }), '[{"a":1}]')
        assert.equal(common.urlEncode("public-data"), "publicdata")
        const payload = Buffer.from(JSON.stringify({ azp: "c" })).toString("base64")
        assert.deepEqual(common.parseJwt(`h.${payload}.s`), { azp: "c" })
    })
})
