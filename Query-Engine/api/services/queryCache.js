// Cache of the GraphQL datapoints queries, versioned, in one collection (config.cache.collection, "querycache").
//
//   queriesmap   one row per VERSION of a query: { query, version, state, createdAt, count, backups, ... }
//                query is the stringified GraphQL query (same key as before), state:
//                  building   being written (never read)
//                  active     the version returned for the query (at most one per query)
//                  previous   an older version, kept as history (restoreCache brings it back)
//                  deleting   being deleted (its datapoints first, then the row)
//   querycache   the datapoints of every version, copied: { ...datapoint, cacheId, cacheSeq, datapointId }
//                cacheId = _id of the queriesmap row, cacheSeq = position in the result, datapointId = the _id of
//                the datapoint in the datapoints collection (returned as _id). Indexed by { cacheId, cacheSeq }.
//
// A new version never overwrites the active one: it is written in full, then it becomes active and the old active
// one becomes previous. If writing fails, the active version is untouched. Versions are deleted only by the
// pruning: per query, config.cache.keepVersions versions (the active one included) are kept, plus every version
// with a backup timestamp (backupCache), which only resetBackup releases.
//
// The rows written before (legacy: { query, coll }, one collection per query) have no state and are ignored here:
// scripts/migrateCache.js imports them.

const mongoose = require("mongoose")
const logger = require("percocologger")
const config = require("../../config")
const QueriesMap = require("../models/QueriesMap")

const STATES = ["building", "active", "previous", "deleting"]
const WRITE_BATCH = 1000

function cacheSettings() {
    const c = config.cache || {}
    const keep = Number(c.keepVersions)
    return {
        collection: typeof c.collection === "string" && c.collection ? c.collection : "querycache",
        keepVersions: Number.isInteger(keep) && keep >= 1 ? keep : 3
    }
}

const rows = () => QueriesMap.collection
const data = () => mongoose.connection.db.collection(cacheSettings().collection)

// ---- Indexes (once per process)
let indexesReady
function ensureIndexes() {
    indexesReady ||= (async () => {
        // at most one active and one building version per query (partial: the legacy rows have no state)
        await rows().createIndex({ query: 1, state: 1 }, { name: "query_active_unique", unique: true, partialFilterExpression: { state: "active" } })
        await rows().createIndex({ state: 1, query: 1 }, { name: "query_building_unique", unique: true, partialFilterExpression: { state: "building" } })
        await rows().createIndex({ query: 1, version: -1 }, { name: "query_version" })
        await data().createIndex({ cacheId: 1, cacheSeq: 1 }, { name: "cacheId_seq" })
    })().catch(error => {
        indexesReady = undefined
        throw error
    })
    return indexesReady
}

// ---- Read / write (GraphQL datapoints)

// The datapoints of the active version of the query, in their order; null if there is none (or it is incomplete)
async function readCache(query) {
    await ensureIndexes()
    const row = await rows().findOne({ query, state: "active" })
    if (!row)
        return null
    const docs = await data().find({ cacheId: row._id }, { projection: { _id: 0, cacheId: 0, cacheSeq: 0 } }).sort({ cacheSeq: 1 }).toArray()
    if (typeof row.count === "number" && docs.length != row.count) {
        logger.warn(`Cache of version ${row.version} of ${query}: ${docs.length} datapoints instead of ${row.count}, ignored`)
        return null
    }
    return docs.map(({ datapointId, ...d }) => datapointId === undefined ? d : { _id: datapointId, ...d })
}

function toCached(datapoint, cacheId, cacheSeq) {
    const { _id, ...d } = datapoint
    return _id === undefined ? { ...d, cacheId, cacheSeq } : { ...d, cacheId, cacheSeq, datapointId: _id }
}

// Writes a new version of the query and makes it the active one. meta: informative fields of the row (survey, ...).
// Returns the version, or null when another request is already writing one for the same query.
async function writeCache(query, datapoints, meta = {}) {
    await ensureIndexes()
    const last = await rows().findOne({ query, state: { $in: STATES } }, { sort: { version: -1 }, projection: { version: 1 } })
    const version = (last?.version || 0) + 1
    let _id
    try {
        const info = Object.fromEntries(Object.entries(meta).filter(([, v]) => v !== undefined))
        _id = (await rows().insertOne({ ...info, query, version, state: "building", createdAt: new Date() })).insertedId
    }
    catch (error) {
        if (error?.code == 11000)
            return null // building already (unique index)
        throw error
    }
    try {
        for (let start = 0; start < datapoints.length; start += WRITE_BATCH)
            await data().insertMany(datapoints.slice(start, start + WRITE_BATCH).map((d, i) => toCached(d, _id, start + i)), { ordered: true })
        await rows().updateMany({ query, state: "active" }, { $set: { state: "previous", replacedAt: new Date() } })
        await rows().updateOne({ _id }, { $set: { state: "active", count: datapoints.length, activatedAt: new Date() } })
    }
    catch (error) {
        await rows().updateOne({ _id }, { $set: { state: "deleting" } }).catch(() => { })
        schedulePurge()
        throw error
    }
    logger.info(`Cache version ${version} of ${query}: ${datapoints.length} datapoints`)
    schedulePrune([query])
    return version
}

// ---- Pruning and deletion (in background: deleting a large version takes time)

let background = Promise.resolve()
function inBackground(job) {
    background = background.then(job).catch(error => logger.error("Query cache cleanup failed", error))
    return background
}

// Per query: keepVersions versions, the active one included; versions with a backup timestamp are always kept
async function prune(queries) {
    const { keepVersions } = cacheSettings()
    const filter = { state: "previous", $or: [{ backups: { $exists: false } }, { backups: { $size: 0 } }] }
    const list = queries || await rows().distinct("query", filter)
    let marked = 0
    for (const query of list) {
        const old = await rows().find({ ...filter, query }, { projection: { _id: 1 } }).sort({ version: -1 }).toArray()
        // no active version (after resetCache): its slot is kept for the previous ones until the next version
        const hasActive = !!(await rows().findOne({ query, state: "active" }, { projection: { _id: 1 } }))
        const extra = old.slice(hasActive ? keepVersions - 1 : keepVersions).map(r => r._id)
        if (extra.length)
            marked += (await rows().updateMany({ _id: { $in: extra }, state: "previous" }, { $set: { state: "deleting" } })).modifiedCount
    }
    return marked
}

// Deletes the datapoints of the versions being deleted, then their rows
async function purge() {
    let deleted = 0
    for (const row of await rows().find({ state: "deleting" }, { projection: { _id: 1, query: 1, version: 1 } }).toArray()) {
        const { deletedCount } = await data().deleteMany({ cacheId: row._id })
        await rows().deleteOne({ _id: row._id, state: "deleting" })
        logger.info(`Cache version ${row.version} of ${row.query} deleted (${deletedCount} datapoints)`)
        deleted++
    }
    return deleted
}

function schedulePrune(queries) {
    return inBackground(async () => {
        await prune(queries)
        await purge()
    })
}
const schedulePurge = () => inBackground(purge)

// At startup: a version still building was interrupted (the Query-Engine stopped while writing it)
async function recover() {
    await ensureIndexes()
    const { modifiedCount } = await rows().updateMany({ state: "building" }, { $set: { state: "deleting" } })
    if (modifiedCount)
        logger.warn(`Query cache: ${modifiedCount} interrupted versions to delete`)
    return schedulePrune()
}

// ---- Management endpoints (filters: every string must be in the query, case insensitive, as before)

function asList(value) {
    if (value === undefined || value === null || value === "")
        return []
    return (Array.isArray(value) ? value : [value]).filter(v => typeof v === "string" && v)
}

function rowsFilter(queriesMapFilter, cacheFilter) {
    const words = asList(queriesMapFilter).concat(asList(cacheFilter))
    const escape = s => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    return words.length ? { $and: words.map(w => ({ query: { $regex: escape(w), $options: "i" } })) } : {}
}

// The active versions get the backup timestamp: kept until resetBackup, restoreCache(timestamp) brings them back
async function backupCache(queriesMapFilter, cacheFilter) {
    await ensureIndexes()
    const timestamp = Date.now()
    const { modifiedCount } = await rows().updateMany({ ...rowsFilter(queriesMapFilter, cacheFilter), state: "active" }, { $addToSet: { backups: timestamp } })
    return { timestamp, queries: modifiedCount }
}

async function activate(row) {
    if (row.state == "active")
        return false
    await rows().updateMany({ query: row.query, state: "active" }, { $set: { state: "previous", replacedAt: new Date() } })
    const { modifiedCount } = await rows().updateOne({ _id: row._id, state: "previous" }, { $set: { state: "active", restoredAt: new Date() } })
    return modifiedCount == 1
}

// timestamp: the versions of that backup. version = "previous": per query, the newest version older than the
// active one (or the newest one, if there is no active version).
async function restoreCache(queriesMapFilter, cacheFilter, timestamp, version) {
    await ensureIndexes()
    const filter = rowsFilter(queriesMapFilter, cacheFilter)
    let chosen = []
    if (timestamp !== undefined && timestamp !== null && timestamp !== "") {
        const ts = Number(timestamp)
        if (!Number.isFinite(ts))
            throw Object.assign(new Error("timestamp must be a number (returned by backupCache)"), { status: 400 })
        chosen = await rows().find({ ...filter, backups: ts, state: { $in: ["active", "previous"] } }).toArray()
    }
    else if (version == "previous") {
        const byQuery = new Map()
        for (const row of await rows().find({ ...filter, state: { $in: ["active", "previous"] } }).sort({ version: -1 }).toArray()) {
            const q = byQuery.get(row.query) || byQuery.set(row.query, []).get(row.query)
            q.push(row)
        }
        for (const versions of byQuery.values()) {
            const active = versions.find(r => r.state == "active")
            const candidate = versions.find(r => r.state == "previous" && (!active || r.version < active.version))
            if (candidate)
                chosen.push(candidate)
        }
    }
    else
        throw Object.assign(new Error("Missing timestamp (or version=previous)"), { status: 400 })
    let restored = 0
    for (const row of chosen)
        if (await activate(row))
            restored++
    return { restored, alreadyActive: chosen.length - restored }
}

// The active versions become previous (kept as history, within keepVersions): the next request writes a new one
async function resetCache(queriesMapFilter, cacheFilter) {
    await ensureIndexes()
    const filter = { ...rowsFilter(queriesMapFilter, cacheFilter), state: "active" }
    const queries = await rows().distinct("query", filter)
    const { modifiedCount } = await rows().updateMany(filter, { $set: { state: "previous", replacedAt: new Date() } })
    schedulePrune(queries)
    return { reset: modifiedCount }
}

// Removes the backup timestamp (one, or all) from the versions: the ones beyond keepVersions are then deleted
async function resetBackup(queriesMapFilter, cacheFilter, timestamp) {
    await ensureIndexes()
    const filter = rowsFilter(queriesMapFilter, cacheFilter)
    let update
    if (timestamp !== undefined && timestamp !== null && timestamp !== "") {
        const ts = Number(timestamp)
        if (!Number.isFinite(ts))
            throw Object.assign(new Error("timestamp must be a number (returned by backupCache)"), { status: 400 })
        filter.backups = ts
        update = { $pull: { backups: ts } }
    }
    else {
        filter.backups = { $exists: true }
        update = { $unset: { backups: "" } }
    }
    filter.state = { $in: STATES }
    const queries = await rows().distinct("query", filter)
    const { modifiedCount } = await rows().updateMany(filter, update)
    schedulePrune(queries)
    return { released: modifiedCount }
}

// Versions of the queries (no datapoints): to choose what to restore
async function listCache(queriesMapFilter, cacheFilter) {
    return rows().find({ ...rowsFilter(queriesMapFilter, cacheFilter), state: { $in: STATES } },
        { projection: { query: 1, version: 1, state: 1, count: 1, backups: 1, createdAt: 1, activatedAt: 1, replacedAt: 1, survey: 1, source: 1, lang: 1 } })
        .sort({ query: 1, version: -1 }).toArray()
}

module.exports = {
    STATES,
    cacheSettings,
    ensureIndexes,
    readCache,
    writeCache,
    prune,
    purge,
    recover,
    backupCache,
    restoreCache,
    resetCache,
    resetBackup,
    listCache,
    // tests: resolves when the background cleanup has finished
    idle: () => background
}
