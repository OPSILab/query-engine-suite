// Guards of the MongoDB queries built from what the user sends (api/services/queryGuards.js)
const { load, config, resetConfig } = require("./helpers/env")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

const guards = load("api/services/queryGuards.js")
beforeEach(resetConfig)

describe("checkFilter", () => {
    test("read operators, nested, in arrays: accepted", () => {
        guards.checkFilter({ city: "Rome", n: { $gte: 1, $lt: 5 }, $or: [{ a: { $in: [1, 2] } }, { b: { $regex: "x", $options: "i" } }], c: { $elemMatch: { d: { $exists: true } } } })
        guards.checkFilter("text")
        guards.checkFilter(null)
    })

    test("code / other collections / writes: 400 with the operator", () => {
        for (const [filter, op] of [[{ $where: "1" }, "$where"], [{ a: { $function: {} } }, "$function"], [{ $expr: {} }, "$expr"],
            [{ $or: [{ a: 1 }, { b: { $accumulator: {} } }] }, "$accumulator"], [{ a: { $lookup: {} } }, "$lookup"], [[{ $set: { a: 1 } }], "$set"]])
            assert.throws(() => guards.checkFilter(filter, 0, "query"), e => e.status == 400 && e.message.includes(op) && e.message.startsWith("query"))
    })

    test("too deep: 400", () => {
        let deep = { a: 1 }
        for (let i = 0; i < guards.MAX_DEPTH + 2; i++)
            deep = { $and: [deep] }
        assert.throws(() => guards.checkFilter(deep), e => e.status == 400 && /deeply/.test(e.message))
    })
})

describe("maxTimeMS", () => {
    test("default 15 minutes; configured; 0 = no limit", () => {
        delete config.queryOptions.mongoMaxTimeMS
        assert.deepEqual(guards.timeLimit(), { maxTimeMS: 900000 })
        config.queryOptions.mongoMaxTimeMS = 600000
        assert.deepEqual(guards.timeLimit(), { maxTimeMS: 600000 })
        config.queryOptions.mongoMaxTimeMS = 0
        assert.deepEqual(guards.timeLimit(), {})
    })

    test("httpStatus: 400 of the guards, 504 when MongoDB stopped the query, else 500", () => {
        assert.equal(guards.httpStatus(Object.assign(new Error(), { status: 400 })), 400)
        assert.equal(guards.httpStatus(Object.assign(new Error(), { code: 50, codeName: "MaxTimeMSExpired" })), 504)
        assert.equal(guards.httpStatus(new Error("x")), 500)
    })
})
