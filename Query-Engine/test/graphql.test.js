// GraphQL schema + resolvers through Apollo's executeOperation, on a real MongoDB (see helpers/db.js:
// npm run test:db). Only the translation service is stubbed (it opens a Redis connection when required).
const { stub, load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

stub("api/services/translationService.js", { translateDataPointsBatch: async dps => dps })

const PREFIX = "anna@demetrix.it/data model mapper"
// one document per kind found in `sources`
const DOCS = [
    { name: PREFIX + "/own.json", record: { bucketName: "pilot", name: PREFIX + "/own.json" }, json: [{ a: 1 }], year: 2021 },
    { name: "bob@demetrix.it/data model mapper/b.json", record: { bucketName: "pilot" }, year: 2022 },
    { name: "PILOT SHARED Data/s.json", record: { bucketName: "pilot" }, year: 2019 },
    { name: "OTHER SHARED Data/s.json", record: { s3: { bucket: { name: "other" } } } },
    { name: "public.json", record: { s3: { bucket: { name: "public-data" } } }, year: 2023 },
    { name: "Item One", source: "https://api.example.org/items", sourceId: 42, color: "red", year: 2024 },
    { name: "Item Two", source: "https://api.example.org/other", sourceId: 43, color: "blue" },
    { name: "pg-style", record: { from: "https://api.example.org/items" } },
    { name: "weird", source: { nested: true }, record: { bucketName: "public-data" } },
    { name: "minio with a source field", source: "a string field of the file", record: { bucketName: "pilot" } }
]

let server, Source, objectFilter, typeDefs

before(async () => {
    await db.setup(__filename)
    Source = load("api/models/Source.js")
    objectFilter = load("api/services/visibility.js").objectFilter
    typeDefs = load("api/graphql/typeDefs.js")
    const resolvers = load("api/graphql/resolvers.js")
    const { ApolloServer } = require("apollo-server-express")
    server = new ApolloServer({ typeDefs, resolvers, context: ({ req }) => ({ req }) })
    await server.start()
    await Source.collection.insertMany(DOCS.map(d => ({ ...d })))
})
after(db.teardown)
beforeEach(() => resetConfig())

// req as left by the auth middleware
const userReq = (visibility, prefix = PREFIX, bucketName = "pilot") => ({ headers: { visibility }, body: { prefix, bucketName } })
const anyReq = { headers: {}, body: {} }

async function exec(query, req = anyReq, variables) {
    const result = await server.executeOperation({ query, variables }, { req })
    return JSON.parse(JSON.stringify(result)) // graphql-js returns null-prototype objects
}
const names = result => result.data.sources.map(s => s.name).sort()

describe("schema", () => {
    test("has no mutations", async () => {
        const result = await exec('mutation { createSource(name: "x") { id } }')
        assert.match(result.errors[0].message, /not configured to execute mutation/i)
        assert.equal(/type\s+Mutation/.test(typeDefs.loc.source.body), false)
    })
})

describe("visibility, authentication enabled", () => {
    // the visibility is translated into the MongoDB query: it must select exactly what objectFilter allows
    for (const visibility of ["private", "shared", "public", "nonsense"])
        test(`${visibility}: same documents as visibility.objectFilter`, async () => {
            const expected = DOCS.filter(d => objectFilter(d, PREFIX, "pilot", visibility)).map(d => d.name).sort()
            assert.deepEqual(names(await exec("{ sources { name } }", userReq(visibility))), expected)
            assert.equal((await exec("{ sourcesCount }", userReq(visibility))).data.sourcesCount, expected.length)
        })

    test("private: only the user's files", async () => {
        assert.deepEqual(names(await exec("{ sources { name } }", userReq("private"))), [PREFIX + "/own.json"])
    })

    test("public: public-data and API records", async () => {
        assert.deepEqual(names(await exec("{ sources { name } }", userReq("public"))),
            ["Item One", "Item Two", "pg-style", "public.json", "weird"])
    })

    test("a filter can't reach beyond the visibility", async () => {
        const result = await exec('{ sources(filter: """{"record.bucketName": "public-data"}""") { name } }', userReq("private"))
        assert.deepEqual(names(result), [])
    })

    test("source(id) returns null for a document the user may not see", async () => {
        const hidden = await Source.collection.findOne({ name: "bob@demetrix.it/data model mapper/b.json" })
        const own = await Source.collection.findOne({ name: PREFIX + "/own.json" })
        assert.equal((await exec(`{ source(id: "${hidden._id}") { name } }`, userReq("private"))).data.source, null)
        assert.equal((await exec(`{ source(id: "${own._id}") { name } }`, userReq("private"))).data.source.name, PREFIX + "/own.json")
    })
})

describe("authentication disabled", () => {
    test("everything is visible", async () => {
        config.authConfig.disableAuth = true
        assert.equal((await exec("{ sources { name } }")).data.sources.length, DOCS.length)
    })
})

describe("arguments", () => {
    beforeEach(() => { config.authConfig.disableAuth = true })

    test("filter with operators as a block string", async () => {
        const result = await exec('{ sources(filter: """{"year": {"$gte": 2022}, "color": {"$exists": false}}""") { name } }')
        assert.deepEqual(names(result), ["bob@demetrix.it/data model mapper/b.json", "public.json"])
    })

    test("filter as an object through variables", async () => {
        const result = await exec("query ($f: JSON) { sources(filter: $f) { name } }", anyReq, { f: { "record.bucketName": "pilot", year: { $in: [2019, 2021] } } })
        assert.deepEqual(names(result), ["PILOT SHARED Data/s.json", PREFIX + "/own.json"])
    })

    test("name: contains, case insensitive, regex characters are literal", async () => {
        assert.deepEqual(names(await exec('{ sources(name: "item") { name } }')), ["Item One", "Item Two"])
        assert.deepEqual(names(await exec('{ sources(name: "(") { name } }')), [])
    })

    test("source: exact origin", async () => {
        assert.deepEqual(names(await exec('{ sources(source: "https://api.example.org/items") { name } }')), ["Item One"])
    })

    test("refuses operators that run code or read elsewhere, invalid JSON, non-objects", async () => {
        for (const filter of ['"""{"$where": "sleep(1000)"}"""', '"""{"a": {"$function": {}}}"""', '"""{"$expr": {"$eq": [1, 1]}}"""'])
            assert.match((await exec(`{ sources(filter: ${filter}) { name } }`)).errors[0].message, /not allowed/)
        assert.match((await exec('{ sources(filter: "{not json") { name } }')).errors[0].message, /invalid JSON/)
        assert.match((await exec('{ sources(filter: """[1, 2]""") { name } }')).errors[0].message, /must be a JSON object/)
    })

    test("limit defaults to graphQLDefaultLimit and is capped at graphQLMaxLimit; skip", async () => {
        config.queryOptions.graphQLDefaultLimit = 3
        config.queryOptions.graphQLMaxLimit = 5
        assert.equal((await exec("{ sources { name } }")).data.sources.length, 3)
        assert.equal((await exec("{ sources(limit: 100) { name } }")).data.sources.length, 5)
        const all = (await exec("{ sources(limit: 5) { name } }")).data.sources.map(s => s.name)
        const skipped = (await exec("{ sources(limit: 5, skip: 2) { name } }")).data.sources.map(s => s.name)
        assert.deepEqual(skipped.slice(0, 3), all.slice(2))
        assert.match((await exec("{ sources(limit: 0) { name } }")).errors[0].message, /limit/)
        assert.match((await exec("{ sources(skip: -1) { name } }")).errors[0].message, /skip/)
        assert.equal((await exec("{ sourcesCount }")).data.sourcesCount, DOCS.length) // count ignores limit
    })
})

describe("Source fields", () => {
    beforeEach(() => { config.authConfig.disableAuth = true })

    test("scalars, non-scalar source as null, doc(fields)", async () => {
        const result = await exec('{ sources(name: "item one") { name source sourceId doc(fields: ["color", "missing"]) } }')
        assert.deepEqual(result.data.sources, [{ name: "Item One", source: "https://api.example.org/items", sourceId: "42", doc: { color: "red" } }])
        const weird = await exec('{ sources(name: "weird") { source } }')
        assert.equal(weird.errors, undefined)
        assert.equal(weird.data.sources[0].source, null) // object `source`: null instead of a serialization error
    })

    test("doc without fields returns the whole document", async () => {
        const result = await exec('{ sources(name: "own.json") { doc } }')
        assert.deepEqual(result.data.sources[0].doc.json, [{ a: 1 }])
    })
})
