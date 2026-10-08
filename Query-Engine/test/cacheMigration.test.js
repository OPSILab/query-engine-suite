// Import of the old datapoints cache (one collection per query) into the versioned one (utils/cacheMigration.js),
// on real MongoDB (npm run test:db).
const { load } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let migration, cache, mongoose, rows, backupRows, data

before(async () => {
    await db.setup(__filename)
    mongoose = require("mongoose")
    migration = load("utils/cacheMigration.js")
    cache = load("api/services/queryCache.js")
    rows = () => load("api/models/QueriesMap.js").collection
    backupRows = () => mongoose.connection.db.collection("queriesmapbackups")
    data = () => mongoose.connection.db.collection("querycache")
})
after(db.teardown)
beforeEach(async () => {
    await cache.idle()
    await mongoose.connection.dropDatabase()
})

const QA = JSON.stringify({ args: { survey: "NAMA_10R3GDP" } })
const QB = JSON.stringify({ args: { survey: "DEMO_R_GIND3" } })
const ID_A = "66a1b2c3d4e5f60718293a4b" // the old collection names: lowercased by mongoose, + "s" after a letter
const ID_B = "66a1b2c3d4e5f60718293a40"
const ID_ORPHAN = "66a1b2c3d4e5f60718293a41"
const OLD_A = `cachednama_10r3gdp : ${ID_A}s`
const OLD_A_BACKUP = `${OLD_A}_1712345678901_backup`
const OLD_B = `cacheddemo_r_gind3 : ${ID_B}`
const ORPHAN = `cachedx : ${ID_ORPHAN}`
const dps = (tag, n) => Array.from({ length: n }, (_, i) => ({ _id: new mongoose.Types.ObjectId().toString(), value: i, tag }))

async function oldCache() {
    const coll = name => mongoose.connection.db.collection(name)
    const oid = id => new mongoose.Types.ObjectId(id)
    await rows().insertMany([{ _id: oid(ID_A), query: QA, coll: "CachedNAMA_10R3GDP : " + ID_A }, { _id: oid(ID_B), query: QB, coll: "x" }])
    await backupRows().insertMany([{ _id: oid(ID_A), query: QA }])
    await coll(OLD_A).insertMany(dps("current", 3))
    await coll(OLD_A_BACKUP).insertMany(dps("backup", 2))
    await coll(OLD_B).insertMany(dps("b", 1))
    await coll(ORPHAN).insertMany(dps("orphan", 1))
    await coll("datapoints").insertMany(dps("not a cache", 1))
}
const names = async () => (await mongoose.connection.db.listCollections().toArray()).map(c => c.name).sort()

describe("migrateCache", () => {
    test("parseOldCollection", () => {
        assert.deepEqual(migration.parseOldCollection(OLD_A_BACKUP), { name: OLD_A_BACKUP, id: ID_A, timestamp: 1712345678901 })
        assert.deepEqual(migration.parseOldCollection(OLD_B), { name: OLD_B, id: ID_B, timestamp: undefined })
        assert.equal(migration.parseOldCollection("datapoints"), undefined)
        assert.equal(migration.parseOldCollection("querycache"), undefined)
    })

    test("dry run: counts only", async () => {
        await oldCache()
        const stats = await migration.migrateCache({ dryRun: true })
        assert.deepEqual([stats.oldCollections, stats.toMigrate, stats.documents, stats.orphans], [4, 3, 6, [ORPHAN]])
        assert.equal(await data().countDocuments({}), 0)
    })

    test("each old collection becomes a version: backups previous with their timestamp, the current one active", async () => {
        await oldCache()
        const stats = await migration.migrateCache()
        assert.equal(stats.migrated, 3)
        const a = await rows().find({ query: QA, state: { $exists: true } }).sort({ version: 1 }).toArray()
        assert.deepEqual(a.map(r => [r.version, r.state, r.count, r.backups, r.migratedFrom]),
            [[1, "previous", 2, [1712345678901], OLD_A_BACKUP], [2, "active", 3, undefined, OLD_A]])
        assert.ok((await cache.readCache(QA)).every(d => d.tag == "current"))
        assert.equal((await cache.readCache(QB)).length, 1)
        // restore of the old backup, as before
        await cache.restoreCache(undefined, undefined, "1712345678901")
        assert.ok((await cache.readCache(QA)).every(d => d.tag == "backup"))
        // nothing deleted without --drop-old
        assert.ok((await names()).includes(OLD_A))
        assert.equal(await rows().countDocuments({ state: { $exists: false } }), 2)
        // re-run: nothing imported twice
        assert.equal((await migration.migrateCache()).migrated, 0)
        assert.equal(await data().countDocuments({}), 6)
    })

    test("a query already cached by the new code: the old versions before it, the new one stays active", async () => {
        await oldCache()
        await cache.writeCache(QA, dps("new", 4))
        await migration.migrateCache()
        const a = await rows().find({ query: QA, state: { $exists: true } }).sort({ version: 1 }).toArray()
        assert.deepEqual(a.map(r => [r.version, r.state, r.migratedFrom]), [[1, "previous", OLD_A_BACKUP], [2, "previous", OLD_A], [3, "active", undefined]])
        assert.ok((await cache.readCache(QA)).every(d => d.tag == "new"))
    })

    test("--drop-old: imported collections dropped, their old rows deleted; orphans and other collections untouched", async () => {
        await oldCache()
        const stats = await migration.migrateCache({ dropOld: true })
        assert.deepEqual([stats.migrated, stats.dropped], [3, 3])
        const left = await names()
        assert.ok(!left.includes(OLD_A) && !left.includes(OLD_A_BACKUP) && !left.includes(OLD_B))
        assert.ok(left.includes(ORPHAN) && left.includes("datapoints"))
        assert.equal(await rows().countDocuments({ state: { $exists: false } }), 0)
        assert.equal(await backupRows().countDocuments({}), 0)
        assert.equal((await cache.readCache(QA)).length, 3)
    })
})
