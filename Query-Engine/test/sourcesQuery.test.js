// sourcesQuery.js: filter validation and limits (no database).
const { load, config, resetConfig } = require("./helpers/env")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

const { parseFilter, limits, sourcesQuery, visibilityQuery } = load("api/graphql/sourcesQuery.js")

beforeEach(() => resetConfig())

describe("parseFilter", () => {
    test("JSON string or object; empty means no filter", () => {
        assert.deepEqual(parseFilter('{"a": {"$in": [1, 2]}}'), { a: { $in: [1, 2] } })
        assert.deepEqual(parseFilter({ a: 1 }), { a: 1 })
        for (const empty of [undefined, null, ""])
            assert.deepEqual(parseFilter(empty), {})
    })

    test("operators are checked at any depth, also inside arrays", () => {
        assert.throws(() => parseFilter({ $or: [{ a: 1 }, { $where: "x" }] }), /\$where is not allowed/)
        assert.throws(() => parseFilter({ a: { $elemMatch: { b: { $function: {} } } } }), /\$function is not allowed/)
        assert.throws(() => parseFilter({ $lookup: {} }), /not allowed/)
        assert.doesNotThrow(() => parseFilter({ $and: [{ a: { $regex: "^x", $options: "i" } }, { b: { $not: { $size: 2 } } }] }))
    })

    test("too deep, not an object, invalid JSON", () => {
        let deep = { a: 1 }
        for (let i = 0; i < 20; i++)
            deep = { $and: [deep] }
        assert.throws(() => parseFilter(deep), /too deeply nested/)
        assert.throws(() => parseFilter("[1]"), /must be a JSON object/)
        assert.throws(() => parseFilter("42"), /must be a JSON object/)
        assert.throws(() => parseFilter("{"), /invalid JSON/)
    })
})

describe("limits", () => {
    test("defaults and caps from config", () => {
        assert.deepEqual(limits({}), { limit: 100, skip: 0 })
        assert.deepEqual(limits({ limit: 5000, skip: 10 }), { limit: 1000, skip: 10 })
        config.queryOptions.graphQLDefaultLimit = 20
        config.queryOptions.graphQLMaxLimit = 10
        assert.deepEqual(limits({}), { limit: 10, skip: 0 }) // default never above the max
    })

    test("missing config keys (old config.js): built-in defaults", () => {
        delete config.queryOptions.graphQLDefaultLimit
        delete config.queryOptions.graphQLMaxLimit
        assert.deepEqual(limits({}), { limit: 100, skip: 0 })
    })

    test("invalid values", () => {
        assert.throws(() => limits({ limit: 0 }), /limit/)
        assert.throws(() => limits({ limit: 1.5 }), /limit/)
        assert.throws(() => limits({ skip: -1 }), /skip/)
    })
})

describe("sourcesQuery", () => {
    test("combines filter, visibility, name and source", () => {
        const req = { headers: { visibility: "public" }, body: {} }
        const query = sourcesQuery({ filter: '{"a": 1}', name: "x.y", source: "https://s" }, req)
        assert.deepEqual(query.$and[0], { a: 1 })
        assert.deepEqual(query.$and[1], visibilityQuery(req))
        assert.deepEqual(query.$and[2], { name: { $regex: "x\\.y", $options: "i" } })
        assert.deepEqual(query.$and[3], { source: "https://s" })
    })

    test("authentication disabled and no arguments: empty query", () => {
        config.authConfig.disableAuth = true
        assert.deepEqual(sourcesQuery({}, { headers: {}, body: {} }), {})
    })

    test("no prefix / bucket / visibility: matches nothing", () => {
        for (const req of [{ headers: { visibility: "private" }, body: {} }, { headers: { visibility: "shared" }, body: {} }, { headers: {}, body: {} }])
            assert.deepEqual(visibilityQuery(req), { _id: { $in: [] } })
    })
})
