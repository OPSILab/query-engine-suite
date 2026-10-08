// Import of the old datapoints cache (one collection per query) into the versioned cache (api/services/queryCache.js).
// Used by scripts/migrateCache.js.
//
// Before: queriesmap { query, coll } + collection "cached<...> : <row id>" (lowercased by mongoose, sometimes with a
// trailing "s"), and the backups: queriesmapbackup (copies of the rows) + collections "<collection>_<timestamp>_backup".
// After: every old collection is one version of its query, in querycache:
//   - the backups are previous versions with backups: [timestamp] (restoreCache(timestamp) as before);
//   - the current one is the active version (previous, if the query already has an active version).
// Per query, the imported versions come before the versions already written by the new cache (renumbered after them).
//
// Nothing is deleted unless dropOld: then the imported collections are dropped, and the old rows (queriesmap rows
// without state, queriesmapbackup) are deleted when no old collection still needs them.
// Re-runnable: an old collection already imported (migratedFrom) is skipped, an interrupted import is redone.

const mongoose = require("mongoose")
const logger = require("percocologger")
const QueriesMap = require("../api/models/QueriesMap")
const { cacheSettings, ensureIndexes, purge, STATES } = require("../api/services/queryCache")

const OLD_COLLECTION = /^cached.* : ([0-9a-f]{24})s?(?:_(\d+)_backup)?$/i
const BATCH = 1000

function parseOldCollection(name) {
    const match = OLD_COLLECTION.exec(name)
    return match ? { name, id: match[1].toLowerCase(), timestamp: match[2] ? Number(match[2]) : undefined } : undefined
}

function createdAtOf(id) {
    try {
        return new mongoose.Types.ObjectId(id).getTimestamp()
    }
    catch {
        return new Date(0)
    }
}

async function migrateCache({ dryRun = false, dropOld = false } = {}) {
    const db = mongoose.connection.db
    const rows = QueriesMap.collection
    const backupRows = db.collection(mongoose.pluralize()("queriesmapbackup")) // the old QueriesMapBackup model
    const data = db.collection(cacheSettings().collection)
    const names = (await db.listCollections({}, { nameOnly: true }).toArray()).map(c => c.name)
    const old = names.map(parseOldCollection).filter(Boolean)

    const legacy = new Map((await rows.find({ state: { $exists: false } }).toArray()).map(r => [String(r._id).toLowerCase(), r]))
    const backups = new Map((await backupRows.find({}).toArray()).map(r => [String(r._id).toLowerCase(), r]))
    // imported already (a version interrupted while importing is still building: imported again)
    const migrated = new Set(await rows.distinct("migratedFrom", { migratedFrom: { $exists: true }, state: { $in: ["active", "previous"] } }))

    const stats = { oldCollections: old.length, alreadyMigrated: 0, orphans: [], toMigrate: 0, documents: 0, migrated: 0, dropped: 0, rowsDeleted: 0 }
    const plan = []
    for (const o of old) {
        const row = o.timestamp === undefined ? legacy.get(o.id) : backups.get(o.id)
        if (migrated.has(o.name)) {
            stats.alreadyMigrated++
            continue
        }
        if (!row || typeof row.query !== "string") {
            stats.orphans.push(o.name)
            continue
        }
        plan.push({ ...o, query: row.query, createdAt: createdAtOf(o.id), documents: await db.collection(o.name).estimatedDocumentCount() })
    }
    stats.toMigrate = plan.length
    stats.documents = plan.reduce((sum, p) => sum + p.documents, 0)
    if (dryRun)
        return stats

    await ensureIndexes()
    // versions left building by an interrupted import
    await rows.updateMany({ migratedFrom: { $exists: true }, state: "building" }, { $set: { state: "deleting" } })
    await purge()
    const byQuery = new Map()
    for (const p of plan)
        (byQuery.get(p.query) || byQuery.set(p.query, []).get(p.query)).push(p)

    for (const [query, versions] of byQuery) {
        // oldest first: the backups (by timestamp), then the current collection(s) (by creation)
        versions.sort((a, b) => (a.timestamp === undefined) - (b.timestamp === undefined) || (a.timestamp ?? 0) - (b.timestamp ?? 0) || a.createdAt - b.createdAt)
        // the versions written by the new cache come after the imported ones
        await rows.updateMany({ query, state: { $in: STATES } }, { $inc: { version: versions.length } })
        const hasActive = !!(await rows.findOne({ query, state: "active" }))
        const current = versions.filter(v => v.timestamp === undefined).pop()
        let version = 0
        for (const v of versions) {
            version++
            const { insertedId: _id } = await rows.insertOne({ query, version, state: "building", createdAt: v.createdAt, migratedFrom: v.name })
            let count = 0
            let batch = []
            const flush = async () => {
                if (batch.length)
                    await data.insertMany(batch, { ordered: true })
                batch = []
            }
            for await (const doc of db.collection(v.name).find({}).sort({ $natural: 1 })) {
                const { _id: datapointId, ...d } = doc
                batch.push({ ...d, cacheId: _id, cacheSeq: count++, datapointId })
                if (batch.length >= BATCH)
                    await flush()
            }
            await flush()
            const active = v === current && !hasActive
            await rows.updateOne({ _id }, {
                $set: { state: active ? "active" : "previous", count, ...(active ? { activatedAt: new Date() } : {}) },
                ...(v.timestamp !== undefined ? { $addToSet: { backups: v.timestamp } } : {})
            })
            stats.migrated++
            migrated.add(v.name)
            logger.info(`Cache ${v.name}: version ${version} (${active ? "active" : "previous"}) of ${query}, ${count} datapoints`)
        }
    }

    if (dropOld) {
        for (const o of old)
            if (migrated.has(o.name)) {
                await db.collection(o.name).drop().catch(() => { })
                stats.dropped++
            }
        // old rows no longer needed: no old collection left for them
        const left = (await db.listCollections({}, { nameOnly: true }).toArray()).map(c => parseOldCollection(c.name)).filter(Boolean)
        const needed = { legacy: new Set(left.filter(o => o.timestamp === undefined).map(o => o.id)), backup: new Set(left.filter(o => o.timestamp !== undefined).map(o => o.id)) }
        const legacyIds = [...legacy.entries()].filter(([id]) => !needed.legacy.has(id)).map(([, r]) => r._id)
        const backupIds = [...backups.entries()].filter(([id]) => !needed.backup.has(id)).map(([, r]) => r._id)
        stats.rowsDeleted += (await rows.deleteMany({ _id: { $in: legacyIds }, state: { $exists: false } })).deletedCount
        stats.rowsDeleted += (await backupRows.deleteMany({ _id: { $in: backupIds } })).deletedCount
    }
    return stats
}

module.exports = { OLD_COLLECTION, parseOldCollection, migrateCache }
