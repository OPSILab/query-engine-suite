// services/dateFilter.js: the "Date" filters of the Advanced search ({ $gte, $lte } with dates as bounds)
const { test, describe } = require("node:test")
const assert = require("node:assert/strict")
const { splitDateFilters, documentMatches, rowMatches, dateBounds, valuesAt } = require("../api/services/dateFilter")

const DAY_16 = { $gte: "2026-11-16T00:00:00.000Z", $lte: "2026-11-16T23:59:59.999Z" }

describe("dateFilter", () => {
    test("a date filter: $gte / $gt / $lte / $lt with dates; empty bounds left out; anything else is a field as before", () => {
        assert.deepEqual(dateBounds(DAY_16), DAY_16)
        assert.deepEqual(dateBounds({ $gte: "2026-11-16", $lte: null }), { $gte: "2026-11-16" })
        assert.deepEqual(dateBounds({ $gte: null, $lte: null }), {})
        assert.equal(dateBounds({ $gte: 5 }), undefined)
        assert.equal(dateBounds({ $gte: "abc" }), undefined)
        assert.equal(dateBounds({ $in: ["2026-11-16"] }), undefined)
        assert.equal(dateBounds("2026-11-16"), undefined)
        assert.equal(dateBounds({}), undefined)
    })

    test("split: the date filters out of the fields, a condition one day wider in their place (as text or as BSON Date)", () => {
        const { fields, dates } = splitDateFilters({ city: "Rome", when: DAY_16, empty: { $gte: null, $lte: null } })
        assert.deepEqual(dates, [{ key: "when", bounds: DAY_16 }])
        assert.equal(fields.city, "Rome")
        assert.equal("empty" in fields || "when" in fields, false)
        assert.deepEqual(fields.$and, [{
            $or: [
                { when: { $gte: "2026-11-15", $lte: "2026-11-17￿" } },
                { when: { $gte: new Date(DAY_16.$gte), $lte: new Date(DAY_16.$lte) } }
            ]
        }])
    })

    test("split, GeoJSON: the bounds sent as they are (null ones dropped), checked by MongoDB in the features", () => {
        const { fields, dates } = splitDateFilters({ when: { $gte: "2026-11-16", $lte: null } }, "geojson")
        assert.deepEqual(fields, { when: { $gte: "2026-11-16" } })
        assert.deepEqual(dates, [])
    })

    test("values at a path, through arrays", () => {
        const doc = { data: [{ record: { at: 1 } }, { record: { at: [2, 3] } }, { other: 4 }] }
        assert.deepEqual(valuesAt(doc, ["data", "record", "at"]), [1, 2, 3])
        assert.deepEqual(valuesAt(doc, ["data", "1", "record", "at"]), [2, 3])
    })

    test("a document: one value in the range, read as a date in any ISO 8601 form or as a BSON Date", () => {
        const dates = [{ key: "when", bounds: DAY_16 }]
        for (const when of ["2026-11-16", "2026-11-16T12:00:00Z", "2026-11-16T12:00:00.123Z", "2026-11-17T00:30:00+02:00", new Date("2026-11-16T23:00:00Z")])
            assert.equal(documentMatches({ when }, dates), true, String(when))
        for (const when of ["2026-11-15T23:59:59Z", "2026-11-17T00:00:00Z", "16/11/2026", 1794787200000, null, undefined])
            assert.equal(documentMatches({ when }, dates), false, String(when))
    })

    test("an array: every bound on the same element (not one after the start and another before the end)", () => {
        const dates = [{ key: "data.record.created_at", bounds: DAY_16 }]
        const apart = { data: [{ record: { created_at: "2026-11-10T00:00:00Z" } }, { record: { created_at: "2026-11-20T00:00:00Z" } }] }
        const inside = { data: [{ record: { created_at: "2026-11-10T00:00:00Z" } }, { record: { created_at: "2026-11-16T09:00:00Z" } }] }
        assert.equal(documentMatches(apart, dates), false)
        assert.equal(documentMatches(inside, dates), true)
    })

    test("one bound only", () => {
        assert.equal(documentMatches({ when: "2030-01-01" }, [{ key: "when", bounds: { $gte: "2026-11-16" } }]), true)
        assert.equal(documentMatches({ when: "2026-11-15" }, [{ key: "when", bounds: { $gte: "2026-11-16" } }]), false)
        assert.equal(documentMatches({ when: "2026-11-16T00:00:00Z" }, [{ key: "when", bounds: { $lt: "2026-11-16T00:00:00Z" } }]), false)
    })

    test("JSON / CSV file types: in the rows too", () => {
        const dates = [{ key: "day", bounds: DAY_16 }]
        assert.equal(documentMatches({ json: [{ day: "2026-11-16T05:00:00Z" }] }, dates, "json+object"), true)
        assert.equal(documentMatches({ day: "2026-11-16T05:00:00Z" }, dates, "json+object"), true)
        assert.equal(documentMatches({ day: "2026-11-16T05:00:00Z" }, dates, "json"), false)
        assert.equal(documentMatches({ csv: [{ day: "2026-11-16" }] }, dates, "csv"), true)
    })

    test("an unwrapped result: checked when the field is inside its array, otherwise left to the document", () => {
        const dates = [{ key: "data.record.created_at", bounds: DAY_16 }]
        assert.equal(rowMatches({ record: { created_at: "2026-11-16T09:00:00Z" }, unWrappedFrom: "data" }, dates), true)
        assert.equal(rowMatches({ record: { created_at: "2026-11-14T09:00:00Z" }, unWrappedFrom: "data" }, dates), false)
        assert.equal(rowMatches({ created_at: "2026-11-14T09:00:00Z", unWrappedFrom: "data.record" }, dates), false)
        assert.equal(rowMatches({ k: 1, unWrappedFrom: "other" }, dates), true)
        assert.equal(rowMatches({ value: "2026-11-16", unWrappedFrom: "days" }, [{ key: "days", bounds: DAY_16 }]), true)
        assert.equal(rowMatches({ value: "2026-11-18", unWrappedFrom: "days" }, [{ key: "days", bounds: DAY_16 }]), false)
        assert.equal(rowMatches({ day: "2026-11-01", unWrappedFrom: "json" }, [{ key: "day", bounds: DAY_16 }], "json+object"), false)
    })
})
