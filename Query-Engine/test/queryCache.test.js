// Versioned datapoints cache in one collection (api/services/queryCache.js), on real MongoDB (npm run test:db).
const { load, config, resetConfig } = require("./helpers/env")
const db = require("./helpers/db")
const { test, describe, before, after, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

let cache, rows, data, mongoose

before(async () => {
    await db.setup(__filename)
    mongoose = require("mongoose")
    cache = load("api/services/queryCache.js")
    rows = () => load("api/models/QueriesMap.js").collection
    data = () => mongoose.connection.db.collection("querycache")
})
after(db.teardown)
beforeEach(async () => {
    await cache.idle()
    resetConfig()
    await rows().deleteMany({})
    await data().deleteMany({})
})

const Q = JSON.stringify({ args: { survey: "NAMA_10R3GDP", limit: 10 } })
const Q2 = JSON.stringify({ args: { survey: "DEMO_R_GIND3" } })
// as the aggregation returns them: with the _id of the datapoints collection
const points = (n, tag = "v1") => Array.from({ length: n }, (_, i) => ({
    _id: new mongoose.Types.ObjectId().toString(), survey: "NAMA_10R3GDP", dimensions: ["LOVECH", String(2000 + i)], value: i, tag
}))
const versions = async (query = Q) => (await rows().find({ query }).sort({ version: 1 }).toArray()).map(r => [r.version, r.state])
const stored = async () => (await data().find({}).toArray()).length

describe("readCache / writeCache", () => {
    test("nothing cached: null", async () => {
        assert.equal(await cache.readCache(Q), null)
    })

    test("the datapoints come back in their order, with their _id, without the cache fields", async () => {
        const dps = points(25)
        assert.equal(await cache.writeCache(Q, dps, { survey: "NAMA_10R3GDP", lang: undefined }), 1)
        const read = await cache.readCache(Q)
        assert.deepEqual(read.map(d => [String(d._id), d.value]), dps.map(d => [String(d._id), d.value]))
        assert.ok(read.every(d => !("cacheId" in d) && !("cacheSeq" in d) && !("datapointId" in d)))
        const [row] = await rows().find({ query: Q }).toArray()
        assert.deepEqual([row.version, row.state, row.count, row.survey, "lang" in row], [1, "active", 25, "NAMA_10R3GDP", false])
    })

    test("an empty result is cached too", async () => {
        await cache.writeCache(Q, [])
        assert.deepEqual(await cache.readCache(Q), [])
    })

    test("two versions of the same query: 2 rows, 20 + 20 datapoints, the new one active", async () => {
        await cache.writeCache(Q, points(20, "v1"))
        await cache.writeCache(Q, points(20, "v2"))
        await cache.idle()
        assert.deepEqual(await versions(), [[1, "previous"], [2, "active"]])
        assert.equal(await stored(), 40)
        assert.ok((await cache.readCache(Q)).every(d => d.tag == "v2"))
    })

    test("queries are independent", async () => {
        await cache.writeCache(Q, points(2, "q1"))
        await cache.writeCache(Q2, points(3, "q2"))
        assert.equal((await cache.readCache(Q)).length, 2)
        assert.equal((await cache.readCache(Q2)).length, 3)
    })

    test("a version that fails while being written is deleted, the active one is untouched", async () => {
        await cache.writeCache(Q, points(3, "v1"))
        const proto = Object.getPrototypeOf(data())
        const insertMany = proto.insertMany
        proto.insertMany = async function () { throw new Error("disk full") }
        try {
            await assert.rejects(cache.writeCache(Q, points(3, "v2")), /disk full/)
        }
        finally {
            proto.insertMany = insertMany
        }
        await cache.idle()
        assert.deepEqual(await versions(), [[1, "active"]])
        assert.ok((await cache.readCache(Q)).every(d => d.tag == "v1"))
    })

    test("another request writing the same query: no second version (null)", async () => {
        await cache.ensureIndexes()
        await rows().insertOne({ query: Q, version: 1, state: "building", createdAt: new Date() })
        assert.equal(await cache.writeCache(Q, points(2)), null)
        assert.equal(await cache.readCache(Q), null)
    })

    test("an incomplete version (count differs) is not returned", async () => {
        await cache.writeCache(Q, points(3))
        await data().deleteOne({})
        assert.equal(await cache.readCache(Q), null)
    })

    test("the old rows (no state, one collection per query) are ignored", async () => {
        await rows().insertOne({ query: Q, coll: "CachedNAMA_10R3GDP : x" })
        assert.equal(await cache.readCache(Q), null)
        assert.equal(await cache.writeCache(Q, points(1)), 1)
    })

    test("recover: the versions interrupted while building are deleted with their datapoints", async () => {
        await cache.writeCache(Q, points(2, "v1"))
        const { insertedId } = await rows().insertOne({ query: Q, version: 2, state: "building", createdAt: new Date() })
        await data().insertOne({ value: 1, cacheId: insertedId, cacheSeq: 0 })
        await cache.recover()
        assert.deepEqual(await versions(), [[1, "active"]])
        assert.equal(await stored(), 2)
    })
})

describe("cache.maxDatapoints", () => {
    test("a result that would go beyond it is not cached (null); 0 = no limit", async () => {
        config.cache.maxDatapoints = 10
        assert.equal(await cache.writeCache(Q, points(6, "v1")), 1)
        assert.equal(await cache.writeCache(Q2, points(5)), null)
        assert.equal(await cache.readCache(Q2), null)
        assert.equal(await cache.writeCache(Q2, points(4)), 1) // 6 + 4 = 10: fits
        assert.equal(await cache.writeCache(Q, points(1, "v2")), null)
        assert.ok((await cache.readCache(Q)).every(d => d.tag == "v1"))
        config.cache.maxDatapoints = 0
        assert.equal(await cache.writeCache(Q, points(100, "v2")), 2)
    })

    test("default: 5 million", async () => {
        delete config.cache.maxDatapoints
        assert.equal(cache.cacheSettings().maxDatapoints, 5000000)
    })
})

describe("versions kept (config.cache.keepVersions)", () => {
    test("per query: keepVersions versions, the active one included; the older ones deleted with their datapoints", async () => {
        config.cache.keepVersions = 2
        for (const tag of ["v1", "v2", "v3", "v4"])
            await cache.writeCache(Q, points(5, tag))
        await cache.writeCache(Q2, points(5))
        await cache.idle()
        assert.deepEqual(await versions(), [[3, "previous"], [4, "active"]])
        assert.equal(await stored(), 15)
    })

    test("after resetCache (no active version) keepVersions previous versions are kept, until the next version", async () => {
        config.cache.keepVersions = 2
        for (const tag of ["v1", "v2"])
            await cache.writeCache(Q, points(1, tag))
        await cache.resetCache()
        await cache.idle()
        assert.deepEqual(await versions(), [[1, "previous"], [2, "previous"]])
        await cache.writeCache(Q, points(1, "v3"))
        await cache.idle()
        assert.deepEqual(await versions(), [[2, "previous"], [3, "active"]])
    })

    test("the versions with a backup timestamp are always kept", async () => {
        config.cache.keepVersions = 1
        await cache.writeCache(Q, points(5, "v1"))
        const { timestamp, queries } = await cache.backupCache()
        assert.equal(queries, 1)
        await cache.writeCache(Q, points(5, "v2"))
        await cache.writeCache(Q, points(5, "v3"))
        await cache.idle()
        assert.deepEqual(await versions(), [[1, "previous"], [3, "active"]])
        assert.deepEqual((await rows().findOne({ query: Q, version: 1 })).backups, [timestamp])
        assert.equal(await stored(), 10)
    })
})

describe("management endpoints", () => {
    test("backupCache + restoreCache(timestamp): the versions of that backup are active again, no copy", async () => {
        await cache.writeCache(Q, points(4, "good"))
        await cache.writeCache(Q2, points(2, "other"))
        const { timestamp } = await cache.backupCache("nama")
        await cache.writeCache(Q, points(4, "bad"))
        await cache.idle()
        const before = await stored()
        assert.deepEqual(await cache.restoreCache(undefined, undefined, String(timestamp)), { restored: 1, alreadyActive: 0 })
        assert.ok((await cache.readCache(Q)).every(d => d.tag == "good"))
        assert.deepEqual(await versions(), [[1, "active"], [2, "previous"]])
        assert.equal(await stored(), before)
        // Q2 was not in the backup (filter)
        assert.equal((await rows().findOne({ query: Q2 })).backups, undefined)
    })

    test("restoreCache(version=previous): per query, the version before the active one", async () => {
        await cache.writeCache(Q, points(2, "v1"))
        await cache.writeCache(Q, points(2, "v2"))
        await cache.writeCache(Q, points(2, "v3"))
        assert.deepEqual(await cache.restoreCache("NAMA", undefined, undefined, "previous"), { restored: 1, alreadyActive: 0 })
        assert.ok((await cache.readCache(Q)).every(d => d.tag == "v2"))
        await cache.restoreCache("NAMA", undefined, undefined, "previous")
        assert.ok((await cache.readCache(Q)).every(d => d.tag == "v1"))
    })

    test("restoreCache: timestamp or version=previous required (400), timestamp a number", async () => {
        await assert.rejects(cache.restoreCache(), e => e.status == 400)
        await assert.rejects(cache.restoreCache(undefined, undefined, "abc"), e => e.status == 400)
    })

    test("resetCache: the active versions become previous (kept), the next request writes a new version", async () => {
        await cache.writeCache(Q, points(3, "v1"))
        await cache.writeCache(Q2, points(3))
        assert.deepEqual(await cache.resetCache(["NAMA"], "10R3"), { reset: 1 })
        await cache.idle()
        assert.equal(await cache.readCache(Q), null)
        assert.equal((await cache.readCache(Q2)).length, 3)
        assert.deepEqual(await versions(), [[1, "previous"]])
        assert.equal(await cache.writeCache(Q, points(3, "v2")), 2)
        // and the reset version can still be brought back
        await cache.restoreCache("NAMA", undefined, undefined, "previous")
        assert.ok((await cache.readCache(Q)).every(d => d.tag == "v1"))
    })

    test("resetBackup: releases the backup (one timestamp, or all), the versions beyond keepVersions are deleted", async () => {
        config.cache.keepVersions = 1
        await cache.writeCache(Q, points(2, "v1"))
        const first = await cache.backupCache()
        await cache.writeCache(Q, points(2, "v2"))
        const second = await cache.backupCache()
        await cache.writeCache(Q, points(2, "v3"))
        await cache.idle()
        assert.deepEqual(await versions(), [[1, "previous"], [2, "previous"], [3, "active"]])
        assert.deepEqual(await cache.resetBackup(undefined, undefined, String(first.timestamp)), { released: 1 })
        await cache.idle()
        assert.deepEqual(await versions(), [[2, "previous"], [3, "active"]])
        assert.deepEqual(await cache.resetBackup(), { released: 1 })
        await cache.idle()
        assert.deepEqual(await versions(), [[3, "active"]])
        assert.equal(await stored(), 2)
        assert.ok(second.timestamp >= first.timestamp)
    })

    test("filters: every string in the query, case insensitive, taken literally", async () => {
        const Q3 = JSON.stringify({ args: { survey: "A.B(C)" } })
        await cache.writeCache(Q3, points(1))
        await cache.writeCache(Q, points(1))
        assert.equal((await cache.resetCache("a.b(c)")).reset, 1)
        assert.equal((await cache.resetCache("A.B")).reset, 0) // already reset
        assert.equal((await cache.resetCache(["nama", "missing"])).reset, 0)
        assert.equal((await cache.resetCache(["nama", "limit"])).reset, 1)
    })

    test("listCache: the versions, without datapoints", async () => {
        await cache.writeCache(Q, points(2))
        await cache.writeCache(Q, points(2))
        const list = await cache.listCache("nama")
        assert.deepEqual(list.map(r => [r.version, r.state, r.count]), [[2, "active", 2], [1, "previous", 2]])
    })
})
