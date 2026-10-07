// Suggestions (keys / values / entries) and mongoQuery scoping of service.js (models and connectors stubbed).
const { stub, load, config, resetConfig } = require("./helpers/env")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

// Model stub: records the filters passed to find() and returns `results`
function model() {
    const m = { filters: [], results: [] }
    m.find = async (filter, projection) => {
        m.filters.push({ filter, projection })
        return m.results
    }
    return m
}
const Key = stub("api/models/Key.js", model())
const Value = stub("api/models/Value.js", model())
const Entries = stub("api/models/Entries.js", model())
const Source = stub("api/models/Source.js", model())
for (const name of ["QueriesMap", "QueriesMapBackup", "QueryCache", "QueryCacheBackup"])
    stub(`api/models/${name}.js`, {})
stub("inputConnectors/minioConnector.js", {})
stub("inputConnectors/postgresConnector.js", () => ({ query: () => { } }))

const service = load("api/services/service.js")

const PREFIX = "anna@demetrix.it/data model mapper"

beforeEach(() => {
    resetConfig()
    for (const m of [Key, Value, Entries, Source]) {
        m.filters = []
        m.results = []
    }
    console.debug = () => { }
})

describe("suggestions visibility filter", () => {
    test("private: the user's email", async () => {
        await service.getKeys(PREFIX, "pilot", "private", "ci")
        assert.deepEqual(Key.filters[0].filter, { key: { $regex: "^ci", $options: "i" }, visibility: "anna@demetrix.it" })
    })

    test("shared: the pilot's SHARED Data", async () => {
        await service.getValues(PREFIX, "pilot", "shared", "Ro")
        assert.deepEqual(Value.filters[0].filter, { value: { $regex: "^Ro", $options: "i" }, visibility: "PILOT SHARED Data" })
    })

    test("public (and anything else): public-data", async () => {
        await service.getEntries(PREFIX, "pilot", "public", "ci", "Ro")
        assert.deepEqual(Entries.filters[0].filter, {
            key: { $regex: "^ci", $options: "i" },
            value: { $regex: "^Ro", $options: "i" },
            visibility: "public-data"
        })
        assert.deepEqual(Entries.filters[0].projection, { key: 1, value: 1, _id: 0 })
    })

    test("authentication disabled: no visibility filter", async () => {
        config.authConfig.disableAuth = true
        await service.getKeys(undefined, undefined, "private", "")
        await service.getValues(undefined, undefined, "shared", "")
        await service.getEntries(undefined, undefined, "public", "", "")
        assert.equal("visibility" in Key.filters[0].filter, false)
        assert.equal("visibility" in Value.filters[0].filter, false)
        assert.equal("visibility" in Entries.filters[0].filter, false)
    })
})

describe("too many suggestions", () => {
    test("more than 500 keys/values: a message instead of the list", async () => {
        Key.results = Array.from({ length: 501 }, (_, i) => ({ key: "k" + i }))
        assert.deepEqual(await service.getKeys(PREFIX, "pilot", "public", ""), ["Too many suggestions. Type some characters in order to reduce them"])
        Value.results = Array.from({ length: 501 }, (_, i) => ({ value: "v" + i }))
        assert.match((await service.getValues(PREFIX, "pilot", "public", ""))[0], /Too many suggestions/)
    })

    test("up to 500: the list", async () => {
        Key.results = Array.from({ length: 500 }, (_, i) => ({ key: "k" + i }))
        assert.equal((await service.getKeys(PREFIX, "pilot", "public", "")).length, 500)
    })
})

describe("mongoQuery scoping", () => {
    beforeEach(() => {
        Source.results = [
            { name: PREFIX + "/own.json", record: { bucketName: "pilot" } },
            { name: "bob@demetrix.it/data model mapper/b.json", record: { bucketName: "pilot" } },
            { name: "p.json", record: { bucketName: "public-data" } },
            { name: "item", source: "https://api.example.org/items" }
        ]
    })

    test("auth enabled: only what the visibility allows", async () => {
        const own = await service.mongoQuery({ color: "red" }, PREFIX, "pilot", "private")
        assert.deepEqual(own.map(o => o.name), [PREFIX + "/own.json"])
        const pub = await service.mongoQuery({ color: "red", format: "object" }, PREFIX, "pilot", "public")
        assert.deepEqual(pub.map(o => o.name), ["p.json", "item"])
    })

    test("the format field is not part of the Mongo filter", async () => {
        await service.mongoQuery({ color: "red", format: "Object" }, PREFIX, "pilot", "public")
        assert.deepEqual(Source.filters[0].filter, { color: "red" })
    })

    test("auth disabled: everything", async () => {
        config.authConfig.disableAuth = true
        assert.equal((await service.mongoQuery({}, undefined, undefined, "private")).length, 4)
    })

    test("simpleQuery adds fileName, path and fileType", async () => {
        Source.results = [{ name: "a@b.it/folder/data.csv" }]
        const [obj] = await service.simpleQuery({})
        assert.deepEqual(obj, { name: "a@b.it/folder/data.csv", fileName: "folder", path: "a@b.it/folder/data.csv", fileType: "csv" })
    })
})
