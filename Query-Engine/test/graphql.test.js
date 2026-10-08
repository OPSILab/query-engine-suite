// GraphQL schema + resolvers through Apollo's executeOperation, on a real MongoDB (see helpers/db.js:
// npm run test:db). Only the translation service is stubbed (it opens a Redis connection when required).
const { stub, load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

stub("api/services/translationService.js", { translateDataPointsBatch: async dps => dps })

const PREFIX = "anna@demetrix.it/data model mapper"
// one document per kind, in its collection (api: apiConnector records, minio: MinIO files)
const DOCS = [
    { name: PREFIX + "/own.json", record: { bucketName: "pilot", name: PREFIX + "/own.json" }, json: [{ a: 1 }], year: 2021 },
    { name: "bob@demetrix.it/data model mapper/b.json", record: { bucketName: "pilot" }, year: 2022 },
    { name: "PILOT SHARED Data/s.json", record: { bucketName: "pilot" }, year: 2019 },
    { name: "OTHER SHARED Data/s.json", record: { s3: { bucket: { name: "other" } } } },
    { name: "public.json", record: { s3: { bucket: { name: "public-data" } } }, year: 2023 },
    { name: "Item One", source: "https://api.example.org/items", sourceId: 42, color: "red", year: 2024, collection: "api" },
    { name: "Item Two", source: "https://api.example.org/other", sourceId: 43, color: "blue", collection: "api" },
    { name: "pg-style", record: { from: "https://api.example.org/items" }, collection: "api" },
    { name: "weird", source: { nested: true }, record: { bucketName: "public-data" } },
    { name: "minio with a source field", source: "a string field of the file", record: { bucketName: "pilot" } }
]

let server, Api, Minio, Orion, objectFilter, typeDefs
const collectionOf = d => d.collection || "minio"
const stored = ({ collection, ...d }) => d

before(async () => {
    await db.setup(__filename)
    const collections = load("api/services/collections.js")
    Api = collections.collectionModel("api")
    Minio = collections.collectionModel("minio")
    Orion = collections.collectionModel("orion")
    objectFilter = load("api/services/visibility.js").objectFilter
    typeDefs = load("api/graphql/typeDefs.js")
    const resolvers = load("api/graphql/resolvers.js")
    const { ApolloServer } = require("apollo-server-express")
    server = new ApolloServer({ typeDefs, resolvers, context: ({ req }) => ({ req }) })
    await server.start()
    await Api.collection.insertMany(DOCS.filter(d => collectionOf(d) == "api").map(stored))
    await Minio.collection.insertMany(DOCS.filter(d => collectionOf(d) == "minio").map(stored))
    await load("api/models/Dimensions.js").collection.insertMany([
        { survey: "NAMA_10R_3GDP", dimensions: { geo: true } },
        { survey: "DEMO_R_GIND3", dimensions: { geo: true } },
        { dimensions: { broken: true } }
    ])
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
            const expected = DOCS.filter(d => objectFilter(stored(d), PREFIX, "pilot", visibility)).map(d => d.name).sort()
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
        const hidden = await Minio.collection.findOne({ name: "bob@demetrix.it/data model mapper/b.json" })
        const own = await Minio.collection.findOne({ name: PREFIX + "/own.json" })
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
        const minio = 'collections: ["minio"]'
        assert.equal((await exec(`{ sources(${minio}) { name } }`)).data.sources.length, 3)
        assert.equal((await exec(`{ sources(${minio}, limit: 100) { name } }`)).data.sources.length, 5)
        const all = (await exec(`{ sources(${minio}, limit: 5) { name } }`)).data.sources.map(s => s.name)
        const skipped = (await exec(`{ sources(${minio}, limit: 5, skip: 2) { name } }`)).data.sources.map(s => s.name)
        assert.deepEqual(skipped.slice(0, 3), all.slice(2))
        assert.match((await exec("{ sources(limit: 0) { name } }")).errors[0].message, /limit/)
        assert.match((await exec("{ sources(skip: -1) { name } }")).errors[0].message, /skip/)
        assert.equal((await exec("{ sourcesCount }")).data.sourcesCount, DOCS.length) // count ignores limit
    })
})

describe("collections", () => {
    beforeEach(() => { config.authConfig.disableAuth = true })

    test("default api + minio; collections: [...] restricts; collection of each document", async () => {
        const all = await exec("{ sources(limit: 100) { name collection } }")
        assert.equal(all.data.sources.length, DOCS.length)
        assert.deepEqual(all.data.sources.filter(s => s.collection == "api").map(s => s.name).sort(), ["Item One", "Item Two", "pg-style"])
        const api = await exec('{ sources(collections: ["api"]) { name } sourcesCount(collections: ["api"]) }')
        assert.deepEqual([names(api), api.data.sourcesCount], [["Item One", "Item Two", "pg-style"], 3])
        assert.match((await exec('{ sources(collections: ["ftp"]) { name } }')).errors[0].message, /Unknown collections/)
    })

    test("queryOptions.defaultCollections: the collections of sources without `collections`", async () => {
        config.queryOptions.defaultCollections = ["api"]
        assert.deepEqual(names(await exec("{ sources { name } }")), ["Item One", "Item Two", "pg-style"])
    })

    test("limit and skip apply to each collection", async () => {
        const result = await exec('{ sources(limit: 1) { collection } }')
        assert.deepEqual(result.data.sources.map(s => s.collection).sort(), ["api", "minio"])
    })

    test("orion: the Orion records, source = their dataset (fromUrl)", async () => {
        await Orion.collection.insertOne({ name: "orion record", fromUrl: "https://ds/x", source: "EUROSTAT" })
        const result = await exec('{ sources(collections: ["orion"], source: "https://ds/x") { name collection } }')
        assert.deepEqual(result.data.sources, [{ name: "orion record", collection: "orion" }])
        await Orion.collection.deleteMany({})
    })

    test("a collection not stored in MongoDB is not searched", async () => {
        config.collections.api.toMongo = false
        assert.deepEqual(names(await exec('{ sources(collections: ["api"]) { name } }')), [])
    })

    test("source(id) finds the document in any collection", async () => {
        const item = await Api.collection.findOne({ name: "Item One" })
        assert.deepEqual((await exec(`{ source(id: "${item._id}") { name collection } }`)).data.source, { name: "Item One", collection: "api" })
        assert.equal((await exec('{ source(id: "not-an-id") { name } }')).data.source, null)
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

describe("surveys", () => {
    test("surveys of the dimensions collection, sorted, limited", async () => {
        assert.deepEqual((await exec("{ surveys }", userReq("private"))).data.surveys, ["DEMO_R_GIND3", "NAMA_10R_3GDP"])
        assert.deepEqual((await exec("{ surveys(limit: 1) }")).data.surveys, ["DEMO_R_GIND3"])
    })
})

describe("datapoints (the Orion collection)", () => {
    // as the Source-Connector stores them (utils/datapoints.js, legacy shape): provider in source, dataset in fromUrl
    const URL_A = "https://ec.europa.eu/eurostat/api/nama_10r_3gdp.xml"
    const dp = (region, year, value) => ({
        source: "EUROSTAT", survey: "NAMA_10R3GDP", region, fromUrl: URL_A,
        dimensions: [region, "Euro per inhabitant", String(year)], value
    })

    before(async () => {
        await Orion.collection.insertMany([
            dp("LOVECH", 2020, 1), dp("LOVECH", 2021, 2), dp("TRENTO", 2021, 3),
            { fromUrl: URL_A, survey: "NOT A DATAPOINT", name: "no dimensions" }
        ])
    })
    after(() => Orion.collection.deleteMany({}))

    test("by survey, provider in `source`", async () => {
        const result = await exec('{ datapoints(survey: "nama_10r3gdp", sortBy: ["year", "region"], sortOrder: ["asc", "asc"], limit: 10) { survey region dimensions value source fromUrl } }')
        assert.equal(result.errors, undefined)
        assert.deepEqual(result.data.datapoints.map(d => [d.region, d.value, d.source]), [["LOVECH", 1, "EUROSTAT"], ["LOVECH", 2, "EUROSTAT"], ["TRENTO", 3, "EUROSTAT"]])
        assert.equal(result.data.datapoints[0].fromUrl, URL_A)
    })

    test("by provider (source) and dimensions; other sources documents never match", async () => {
        const result = await exec('{ datapoints(source: "EUROSTAT", dimensions: ["LOVECH", "2021"]) { region value } }')
        assert.deepEqual(result.data.datapoints, [{ region: "LOVECH", value: 2 }])
        const none = await exec('{ datapoints(survey: "NOT A DATAPOINT") { region } }')
        assert.deepEqual(none.data.datapoints, [])
    })

    test("the same query again is read from the cache (one version), in the same order", async () => {
        const query = '{ datapoints(survey: "NAMA_10R3GDP", sortBy: ["year", "region"], sortOrder: ["desc", "desc"]) { region value } }'
        const first = await exec(query)
        assert.deepEqual(first.data.datapoints.map(d => d.value), [3, 2, 1])
        await Orion.collection.insertOne(dp("SOFIA", 2022, 4))
        try {
            const second = await exec(query)
            assert.equal(second.errors, undefined)
            assert.deepEqual(second.data, first.data)
        }
        finally {
            await Orion.collection.deleteMany({ region: "SOFIA" })
        }
        const versions = await load("api/models/QueriesMap.js").collection.find({ query: { $regex: "NAMA_10R3GDP" }, state: "active" }).toArray()
        assert.ok(versions.some(r => r.count == 3 && r.survey == "NAMA_10R3GDP"))
    })
})
