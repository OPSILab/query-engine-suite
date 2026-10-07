// GraphQL schema + resolvers through Apollo's executeOperation (models stubbed, no MongoDB).
const { stub, load, config, resetConfig } = require("./helpers/env")
const { test, describe, before, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

// mongoose-like documents: the resolvers read them through toObject()
const doc = data => ({ _id: data.id, id: data.id, toObject: () => ({ ...data }) })
const stored = [
    doc({ id: "1", name: "anna@demetrix.it/data model mapper/own.json", record: { bucketName: "pilot" }, json: [{ a: 1 }] }),
    doc({ id: "2", name: "bob@demetrix.it/data model mapper/b.json", record: { bucketName: "pilot" } }),
    doc({ id: "3", name: "PILOT SHARED Data/s.json", record: { bucketName: "pilot" } }),
    doc({ id: "4", name: "p.json", record: { bucketName: "public-data" } }),
    doc({ id: "5", name: "item", source: "https://api.example.org/items", sourceId: 42, color: "red" }),
    doc({ id: "6", name: "weird", source: { nested: true }, record: { bucketName: "public-data" } })
]

stub("api/models/Source.js", {
    find: async () => stored,
    findById: async id => stored.find(d => d.id == id) || null
})
for (const model of ["Datapoint", "Dimensions", "QueryCache", "QueriesMap"])
    stub(`api/models/${model}.js`, {})
stub("api/services/translationService.js", { translateDataPointsBatch: async dps => dps })

const { ApolloServer } = require("apollo-server-express")
const typeDefs = load("api/graphql/typeDefs.js")
const resolvers = load("api/graphql/resolvers.js")

let server
before(async () => {
    server = new ApolloServer({ typeDefs, resolvers, context: ({ req }) => ({ req }) })
    await server.start()
})

// req as left by the auth middleware
const userReq = (visibility, prefix = "anna@demetrix.it/data model mapper", bucketName = "pilot") =>
    ({ headers: { visibility }, body: { prefix, bucketName } })

async function exec(query, req, variables) {
    const result = await server.executeOperation({ query, variables }, { req })
    return result
}

const plain = value => JSON.parse(JSON.stringify(value)) // graphql-js returns null-prototype objects
const ids = result => result.data.sources.map(s => s.id)

beforeEach(() => resetConfig())

describe("schema", () => {
    test("has no mutations", async () => {
        const result = await exec('mutation { createSource(name: "x") { id } }', userReq("public"))
        assert.ok(result.errors?.length)
        assert.match(result.errors[0].message, /not configured to execute mutation/i)
        assert.equal(/type\s+Mutation/.test(typeDefs.loc.source.body), false)
    })
})

describe("sources, authentication enabled", () => {
    test("private: only the user's files", async () => {
        assert.deepEqual(ids(await exec("{ sources { id } }", userReq("private"))), ["1"])
    })

    test("shared: the pilot's SHARED Data folder", async () => {
        assert.deepEqual(ids(await exec("{ sources { id } }", userReq("shared"))), ["3"])
    })

    test("public: public-data and API records", async () => {
        assert.deepEqual(ids(await exec("{ sources { id } }", userReq("public"))), ["4", "5", "6"])
    })

    test("source(id) returns null for a document the user may not see", async () => {
        const hidden = await exec('{ source(id: "2") { id name } }', userReq("private"))
        assert.equal(hidden.data.source, null)
        const own = await exec('{ source(id: "1") { id name } }', userReq("private"))
        assert.equal(own.data.source.name, "anna@demetrix.it/data model mapper/own.json")
    })
})

describe("sources, authentication disabled", () => {
    test("everything is visible", async () => {
        config.authConfig.disableAuth = true
        assert.deepEqual(ids(await exec("{ sources { id } }", { headers: {}, body: {} })), ["1", "2", "3", "4", "5", "6"])
    })
})

describe("Source fields", () => {
    test("scalars, non-scalar source as null, doc(fields)", async () => {
        config.authConfig.disableAuth = true
        const result = await exec('{ sources { id source sourceId doc(fields: ["color", "missing"]) } }', { headers: {}, body: {} })
        assert.equal(result.errors, undefined)
        const api = plain(result.data.sources.find(s => s.id == "5"))
        assert.deepEqual(api, { id: "5", source: "https://api.example.org/items", sourceId: "42", doc: { color: "red" } })
        const weird = result.data.sources.find(s => s.id == "6")
        assert.equal(weird.source, null) // object `source`: null instead of a serialization error
    })

    test("doc without fields returns the whole document", async () => {
        config.authConfig.disableAuth = true
        const result = await exec('{ source(id: "1") { doc } }', { headers: {}, body: {} })
        assert.deepEqual(result.data.source.doc.json, [{ a: 1 }])
    })
})
