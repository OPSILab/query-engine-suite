// The MongoDB collections of the Source-Connector, one per connector (config.collections, same names as in the
// Source-Connector config; only mongo / toMongo / toPostgres are used here - toPostgres only to tell the frontend
// which data the SQL queries can find):
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
// Requests without `collections` (clients written before the collections existed): queryOptions.defaultCollections.
// Fallback: what the single `sources` collection held, API records and MinIO files - not the datapoints, which
// were in their own collection.
const DEFAULT_COLLECTIONS = ["api", "minio"]

let warnedDefault
function defaultCollections() {
    const configured = config.queryOptions?.defaultCollections
    if (configured === undefined || configured === null)
        return DEFAULT_COLLECTIONS
    const valid = Array.isArray(configured) && configured.length > 0 && configured.every(id => CONNECTORS.includes(id))
    if (valid)
        return [...new Set(configured)]
    if (!warnedDefault) {
        warnedDefault = true
        require("percocologger").warn(`queryOptions.defaultCollections must be a non-empty list of ${CONNECTORS.join(" / ")}: using ${DEFAULT_COLLECTIONS.join(", ")}`)
    }
    return DEFAULT_COLLECTIONS
}
// same defaults as the Source-Connector's
const DEFAULTS = {
    api: { mongo: "sources", toMongo: true, toPostgres: true },
    orion: { mongo: "datapoints", toMongo: true, toPostgres: false },
    minio: { mongo: "minio", toMongo: true, toPostgres: true }
}
// the field holding the origin url of a record (GraphQL sources(source: ...))
const ORIGIN_FIELD = { api: "source", orion: "fromUrl", minio: "source" }

function collectionSettings(connector) {
    if (!CONNECTORS.includes(connector))
        throw new Error(`Unknown collection "${connector}" (${CONNECTORS.join(" | ")})`)
    const s = { ...DEFAULTS[connector], ...(config.collections?.[connector] || {}) }
    return {
        connector,
        mongo: s.mongo,
        toMongo: s.toMongo !== false,
        // copied to PostgreSQL by the Source-Connector (queryOptions.SQLQuery false: no SQL at all)
        toPostgres: s.toPostgres === true && config.queryOptions?.SQLQuery !== false,
        originField: ORIGIN_FIELD[connector]
    }
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

module.exports = { CONNECTORS, DEFAULT_COLLECTIONS, defaultCollections, DEFAULTS, collectionSettings, storedCollections, collectionModel, parseCollections }
