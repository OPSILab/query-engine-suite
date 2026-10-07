// The MongoDB collections of the Source-Connector, one per connector (config.collections, same names as in the
// Source-Connector config; only mongo / toMongo are used here):
//
//   api    apiConnector records ("sources")     public data
//   orion  Orion datasets, datapoints ("datapoints")   public data
//   minio  MinIO files ("minio")                 private / shared / public by bucket and path
//
// The queries (Advanced search, Simple search, suggestions, GraphQL sources) can be restricted to some of them:
// `collections` = list of ids. A collection with toMongo: false is never searched in MongoDB.

const mongoose = require("mongoose")
const config = require("../../config")

const CONNECTORS = ["api", "orion", "minio"]
const DEFAULTS = {
    api: { mongo: "sources", toMongo: true },
    orion: { mongo: "datapoints", toMongo: true },
    minio: { mongo: "minio", toMongo: true }
}
// the field holding the origin url of a record (GraphQL sources(source: ...))
const ORIGIN_FIELD = { api: "source", orion: "fromUrl", minio: "source" }

function collectionSettings(connector) {
    if (!CONNECTORS.includes(connector))
        throw new Error(`Unknown collection "${connector}" (${CONNECTORS.join(" | ")})`)
    const s = { ...DEFAULTS[connector], ...(config.collections?.[connector] || {}) }
    return { connector, mongo: s.mongo, toMongo: s.toMongo !== false, originField: ORIGIN_FIELD[connector] }
}

const storedCollections = () => CONNECTORS.filter(c => collectionSettings(c).toMongo)

// Schemaless model on the collection named in the config (one per name, created at the first use)
const schema = new mongoose.Schema({}, { strict: false, versionKey: false })
function collectionModel(connector) {
    const { mongo } = collectionSettings(connector)
    const name = "collection:" + mongo
    return mongoose.models[name] || mongoose.model(name, schema, mongo)
}

// `collections` of a request: an array or a comma separated string of ids. undefined / null / "" -> undefined
// (every collection); an unknown id -> error with status 400.
function parseCollections(value) {
    if (value === undefined || value === null || value === "")
        return undefined
    const ids = Array.isArray(value) ? value : String(value).split(",")
    const list = [...new Set(ids.map(id => String(id).trim()).filter(Boolean))]
    const unknown = list.filter(id => !CONNECTORS.includes(id))
    if (unknown.length)
        throw Object.assign(new Error(`Unknown collections: ${unknown.join(", ")} (${CONNECTORS.join(" | ")})`), { status: 400 })
    return list
}

module.exports = { CONNECTORS, DEFAULTS, collectionSettings, storedCollections, collectionModel, parseCollections }
