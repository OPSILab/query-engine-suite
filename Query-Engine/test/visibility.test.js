const { load, config, resetConfig } = require("./helpers/env")
const { test, describe, beforeEach } = require("node:test")
const assert = require("node:assert/strict")

const { objectFilter, bucketIs, isApiRecord } = load("api/services/visibility.js")

const PREFIX = "anna@demetrix.it/data model mapper"
const BUCKET = "pilot"

// the kinds of documents found in `sources` (Mongo) and in the PostgreSQL `sources` table
const docs = {
    ownFile: { name: PREFIX + "/own.json", record: { bucketName: BUCKET, name: PREFIX + "/own.json" } },
    otherUserFile: { name: "bob@demetrix.it/data model mapper/b.json", record: { bucketName: BUCKET, name: "bob@demetrix.it/data model mapper/b.json" } },
    sharedFile: { name: "PILOT SHARED Data/s.json", record: { s3: { bucket: { name: BUCKET } } } },
    sharedOtherPilot: { name: "OTHER SHARED Data/s.json", record: { bucketName: "other" } },
    publicFile: { name: "public.json", record: { bucketName: "public-data" } },
    apiRecord: { source: "https://api.example.org/items", sourceId: 1, color: "red" },
    apiPgRow: { name: "item", data: {}, record: { from: "https://api.example.org/items" } },
    minioWithStringSource: { source: "a string field of the uploaded file", record: { bucketName: BUCKET, name: "x/f.json" } }
}

function visible(visibility, prefix = PREFIX, bucket = BUCKET) {
    return Object.keys(docs).filter(name => objectFilter(docs[name], prefix, bucket, visibility))
}

beforeEach(() => resetConfig())

describe("objectFilter with authentication enabled", () => {
    test("private: only the user's own files", () => {
        assert.deepEqual(visible("private"), ["ownFile"])
    })

    test("shared: only the SHARED Data folder of the user's bucket", () => {
        assert.deepEqual(visible("shared"), ["sharedFile"])
    })

    test("public: public-data bucket and API/Orion records (Mongo and PostgreSQL)", () => {
        assert.deepEqual(visible("public"), ["publicFile", "apiRecord", "apiPgRow"])
    })

    test("no prefix (e.g. no user): private shows nothing", () => {
        for (const prefix of [undefined, null, ""])
            assert.deepEqual(Object.keys(docs).filter(name => objectFilter(docs[name], prefix, BUCKET, "private")), [])
    })

    test("unknown or missing visibility shows nothing", () => {
        assert.deepEqual(visible(undefined), [])
        assert.deepEqual(visible("everything"), [])
    })

    test("works on documents without name/record", () => {
        assert.equal(objectFilter({}, PREFIX, BUCKET, "private"), false)
        assert.equal(objectFilter(undefined, PREFIX, BUCKET, "public"), false)
    })
})

describe("objectFilter with authentication disabled", () => {
    test("everything is visible, whatever the visibility (passe-partout)", () => {
        config.authConfig.disableAuth = true
        for (const visibility of ["private", "shared", "public", undefined])
            assert.deepEqual(Object.keys(docs).filter(name => objectFilter(docs[name], undefined, undefined, visibility)), Object.keys(docs))
    })
})

describe("helpers", () => {
    test("bucketIs reads both record shapes", () => {
        assert.equal(bucketIs({ bucketName: "a" }, "a"), true)
        assert.equal(bucketIs({ s3: { bucket: { name: "a" } } }, "a"), true)
        assert.equal(bucketIs({ bucketName: "b" }, "a"), false)
        assert.equal(bucketIs(undefined, "a"), false)
    })

    test("isApiRecord", () => {
        assert.equal(isApiRecord(docs.apiRecord), true)
        assert.equal(isApiRecord(docs.apiPgRow), true)
        assert.equal(isApiRecord(docs.minioWithStringSource), false)
        assert.equal(isApiRecord({ source: { nested: 1 } }), false)
    })
})
