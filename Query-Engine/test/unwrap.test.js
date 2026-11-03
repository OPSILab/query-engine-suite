// services/unwrap.js: the arrays of the Advanced search results, one result per element (?unwrap=true)
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")
const { unwrapDocument, unwrapDocuments, unwrapAnswer } = require("../api/services/unwrap")

describe("unwrap", () => {
    test("the example: every array of objects, its elements with unWrappedFrom and the other fields as ../<field>", () => {
        const docs = [
            { unArray: [{ chiave: "valore" }], unAltroArray: [{ chiave2: "valore2" }], unachiave: "valore3" },
            { unArrayUlteriore: [{ chiave4: "valore4" }], chiave5: "valore5" }
        ]
        assert.deepEqual(unwrapDocuments(docs), [
            { chiave: "valore", unWrappedFrom: "unArray", "../unachiave": "valore3" },
            { chiave2: "valore2", unWrappedFrom: "unAltroArray", "../unachiave": "valore3" },
            { chiave4: "valore4", unWrappedFrom: "unArrayUlteriore", "../chiave5": "valore5" }
        ])
    })

    test("arrays of values: { value }; with onlyArrayOfObjects they stay fields of the document", () => {
        const doc = { name: "Datapoint", dimensions: ["Lovech", "2020"], rows: [{ n: 1 }] }
        assert.deepEqual(unwrapDocument(doc), [
            { value: "Lovech", unWrappedFrom: "dimensions", "../name": "Datapoint" },
            { value: "2020", unWrappedFrom: "dimensions", "../name": "Datapoint" },
            { n: 1, unWrappedFrom: "rows", "../name": "Datapoint" }
        ])
        assert.deepEqual(unwrapDocument(doc, { onlyArrayOfObjects: true }), [
            { n: 1, unWrappedFrom: "rows", "../name": "Datapoint", "../dimensions": ["Lovech", "2020"] }
        ])
        // mixed: not only objects
        assert.deepEqual(unwrapDocument({ mixed: [{ a: 1 }, 2] }, { onlyArrayOfObjects: true }), [{ mixed: [{ a: 1 }, 2] }])
        assert.deepEqual(unwrapDocument({ mixed: [{ a: 1 }, 2] }), [{ a: 1, unWrappedFrom: "mixed" }, { value: 2, unWrappedFrom: "mixed" }])
    })

    test("first level only; empty arrays and documents without arrays as they are; _collection kept", () => {
        assert.deepEqual(unwrapDocument({ a: [{ inner: [{ x: 1 }] }], _collection: "api" }), [{ inner: [{ x: 1 }], unWrappedFrom: "a", _collection: "api" }])
        assert.deepEqual(unwrapDocument({ empty: [], k: 1 }), [{ empty: [], k: 1 }])
        assert.deepEqual(unwrapDocument({ k: 1, o: { a: [1] } }), [{ k: 1, o: { a: [1] } }])
        // objects that are not plain (a Date, an ObjectId...) are values
        const date = new Date("2026-11-03T00:00:00Z")
        assert.deepEqual(unwrapDocument({ dates: [date] }), [{ value: date, unWrappedFrom: "dates" }])
    })

    test("a page: its results unwrapped, the rest as it was", () => {
        assert.deepEqual(unwrapAnswer({ results: [{ a: [{ k: 1 }, { k: 2 }] }], hasMore: true, next: { api: 1 } }),
            { results: [{ k: 1, unWrappedFrom: "a" }, { k: 2, unWrappedFrom: "a" }], hasMore: true, next: { api: 1 } })
    })
})
