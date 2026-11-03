// services/unwrap.js: the arrays of the Advanced search results, one result per element (?unwrap=true)
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")
const { unwrapDocument, unwrapDocuments, unwrapAnswer, unwrapPage, unwrapCapped } = require("../api/services/unwrap")

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

    test("a level (path): its arrays, the context relative like a path, unWrappedFrom the whole path", () => {
        const doc = { _collection: "api", name: "Doc", data: { unarray: [{ a: 1 }, { a: 2 }], other: [{ b: 1 }], k: "kv", tags: ["x"] }, list: [{ z: 1 }] }
        assert.deepEqual(unwrapDocument(doc, { path: "data", onlyArrayOfObjects: true }), [
            { a: 1, unWrappedFrom: "data.unarray", "../k": "kv", "../tags": ["x"], "../../name": "Doc", "../../list": [{ z: 1 }], _collection: "api" },
            { a: 2, unWrappedFrom: "data.unarray", "../k": "kv", "../tags": ["x"], "../../name": "Doc", "../../list": [{ z: 1 }], _collection: "api" },
            { b: 1, unWrappedFrom: "data.other", "../k": "kv", "../tags": ["x"], "../../name": "Doc", "../../list": [{ z: 1 }], _collection: "api" }
        ])
        // an array: only that one, its siblings are context
        assert.deepEqual(unwrapDocument(doc, { path: "data.unarray" }).map(r => [r.a, r.unWrappedFrom, r["../other"], r["../../name"]]),
            [[1, "data.unarray", [{ b: 1 }], "Doc"], [2, "data.unarray", [{ b: 1 }], "Doc"]])
    })

    test("deeper levels: one more ../ per level", () => {
        const doc = { r: 0, a: { x: 1, b: { y: 2, items: [{ v: 1 }] } } }
        assert.deepEqual(unwrapDocument(doc, { path: "a.b" }), [{ v: 1, unWrappedFrom: "a.b.items", "../y": 2, "../../x": 1, "../../../r": 0 }])
        assert.deepEqual(unwrapDocument(doc, { path: "a.b.items" }), [{ v: 1, unWrappedFrom: "a.b.items", "../y": 2, "../../x": 1, "../../../r": 0 }])
    })

    test("a level the document does not have (or not an object / array there, or nothing to unwrap): as it is", () => {
        const doc = { data: { k: 1, values: [1, 2] }, s: "x" }
        for (const path of ["nope", "data.nope", "s", "s.deeper", "data.k"])
            assert.deepEqual(unwrapDocument(doc, { path }), [doc], path)
        assert.deepEqual(unwrapDocument(doc, { path: "data", onlyArrayOfObjects: true }), [doc])
        assert.deepEqual(unwrapDocument(doc, { path: "data.values", onlyArrayOfObjects: true }), [doc])
        assert.deepEqual(unwrapDocument(doc, { path: "data.values" }).map(r => r.value), [1, 2])
    })

    test("a bad path: an error with status 400", () => {
        for (const path of ["data.", ".data", "a..b"])
            assert.throws(() => unwrapDocument({}, { path }), error => error.status == 400)
    })

    test("pages count the rows: a document cut between two pages, next / nextRows to go on", () => {
        const doc = (c, id, n) => ({ _collection: c, id, items: Array.from({ length: n }, (_, i) => ({ i })) })
        // documents 0..2 of api (3, 1, 2 rows), more after them; one document of orion (2 rows)
        const answer = { results: [doc("api", "a", 3), doc("api", "b", 1), doc("api", "c", 2), doc("orion", "o", 2)], hasMore: true, next: { api: 3 } }
        const rows = page => page.results.map(r => `${r._collection}:${r["../id"]}${r.i}`)
        let page = unwrapPage(answer, { limit: 2, skip: 0 }, ["api", "orion"])
        assert.deepEqual(rows(page), ["api:a0", "api:a1", "orion:o0", "orion:o1"])
        assert.deepEqual([page.next, page.nextRows, page.hasMore], [{ api: 0 }, { api: 2 }, true])
        // the next page of api: from document 0, its third row
        const second = { results: [doc("api", "a", 3), doc("api", "b", 1)], next: { api: 2 } }
        page = unwrapPage(second, { limit: 2, skip: { api: 0 }, skipRows: { api: 2 } }, ["api"])
        assert.deepEqual(rows(page), ["api:a2", "api:b0"])
        assert.deepEqual([page.next, page.nextRows], [{ api: 2 }, {}])
        // the last documents, nothing more
        page = unwrapPage({ results: [doc("api", "c", 2)] }, { limit: 5, skip: { api: 2 } }, ["api"])
        assert.deepEqual([rows(page), page.hasMore, page.next], [["api:c0", "api:c1"], false, {}])
        // the limit reached at the end of a document, more documents in this answer: the next one
        page = unwrapPage({ results: [doc("api", "a", 2), doc("api", "b", 1)] }, { limit: 2, skip: 4 }, ["api"])
        assert.deepEqual([rows(page), page.next, page.nextRows], [["api:a0", "api:a1"], { api: 5 }, {}])
    })

    test("without a page: at most max rows per collection, a warning for each one cut (not twice)", () => {
        const docs = [{ _collection: "api", items: [{ i: 0 }, { i: 1 }, { i: 2 }] }, { _collection: "minio", k: 1 }]
        const warnings = []
        assert.deepEqual(unwrapCapped(docs, 2, ["api", "minio"], {}, warnings).map(r => r.i ?? r.k), [0, 1, 1])
        assert.deepEqual(warnings.map(w => [w.code, w.source]), [["RESULTS_TRUNCATED", "api"]])
        const already = [{ kind: "runtime", code: "RESULTS_TRUNCATED", source: "api", message: "documents" }]
        unwrapCapped(docs, 2, ["api"], {}, already)
        assert.equal(already.length, 1)
    })
})
