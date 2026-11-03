// Advanced search with ?unwrap=true: every array of a result document becomes one result per element, so that the
// rows of a JSON / CSV file, the features of a GeoJSON or the items of an API record can be read one by one.
//   - an element that is an object: its fields; any other element (a value, an array): { value: <element> }
//   - unWrappedFrom: the name of the array it comes from
//   - "../<field>": every other field of the document (not the unwrapped arrays), so each result keeps its context
//   - _collection (added by the Query-Engine) stays as it is
// Only the arrays at the top level of the document (the arrays inside an element stay as they are). An empty array
// is not unwrapped (it stays a field of the document). onlyArrayOfObjects (?unwrapOnlyArrayOfObjects=true): only
// the arrays whose elements are all objects; the others (e.g. dimensions: ["Lovech", "2020"]) stay fields too.
// A document without arrays to unwrap is returned as it is.
// E.g. { a: [{ k: 1 }], b: [{ k2: 2 }], c: 3 } -> [{ k: 1, unWrappedFrom: "a", "../c": 3 }, { k2: 2, unWrappedFrom: "b", "../c": 3 }]
//
// path (?unwrapPath=data or data.items): another level than the top of the document, as a dotted path of fields.
//   - an object (data): its arrays are unwrapped; an array (data.items): only that one
//   - the context is relative, like a path: the other fields of the object holding the arrays as "../<field>", those
//     of the object above it as "../../<field>" (without the field leading down), and so on up to the document
//   - unWrappedFrom: the whole path of the array (data.items)
//   - a document without that object / array (or with nothing to unwrap there): as it is
// E.g. path "data": { data: { a: [{ k: 1 }], j: 1 }, x: 2 } -> [{ k: 1, unWrappedFrom: "data.a", "../j": 1, "../../x": 2 }]

const KEPT = ["_collection"]

// A plain object (not an ObjectId, a Date...)
function isPlainObject(value) {
    if (value === null || typeof value != "object" || Array.isArray(value))
        return false
    const proto = Object.getPrototypeOf(value)
    return proto === Object.prototype || proto === null
}

function unwrappable(value, onlyArrayOfObjects) {
    return Array.isArray(value) && value.length > 0 && (!onlyArrayOfObjects || value.every(isPlainObject))
}

// The segments of a path ("data.items" -> ["data", "items"]); none: the top of the document. Empty segments: an error.
function pathSegments(path) {
    if (path === undefined || path === null || path === "")
        return []
    if (typeof path != "string" || path.split(".").some(segment => !segment))
        throw Object.assign(new Error("unwrapPath must be a dotted path of fields, e.g. data or data.items"), { status: 400 })
    return path.split(".")
}

function unwrapDocument(doc, { onlyArrayOfObjects = false, path } = {}) {
    if (!isPlainObject(doc))
        return [doc]
    const segments = pathSegments(path)
    // the objects from the document down to the level: levels[i] holds segments[i]
    const levels = [doc]
    for (let i = 0; i < segments.length - 1; i++) {
        const next = levels[i][segments[i]]
        if (!isPlainObject(next))
            return [doc]
        levels.push(next)
    }
    let holder, arrays
    const last = segments[segments.length - 1]
    if (last === undefined) { // the top of the document
        holder = doc
        arrays = Object.keys(doc).filter(key => !KEPT.includes(key) && unwrappable(doc[key], onlyArrayOfObjects))
    }
    else {
        const target = levels[levels.length - 1][last]
        if (Array.isArray(target)) { // an array: only that one, held by the object above it
            holder = levels[levels.length - 1]
            arrays = unwrappable(target, onlyArrayOfObjects) ? [last] : []
        }
        else if (isPlainObject(target)) { // an object: its arrays
            holder = target
            levels.push(target)
            arrays = Object.keys(target).filter(key => unwrappable(target[key], onlyArrayOfObjects))
        }
        else
            return [doc]
    }
    if (!arrays.length)
        return [doc]
    // the path of the holder: the segments down to it
    const holderPath = segments.slice(0, levels.length - 1)
    const context = {}
    for (const [key, value] of Object.entries(holder))
        if (!arrays.includes(key) && !(holder === doc && KEPT.includes(key)))
            context["../" + key] = value
    // the objects above the holder: one more "../" per level, without the field leading down
    for (let depth = levels.length - 2; depth >= 0; depth--)
        for (const [key, value] of Object.entries(levels[depth]))
            if (key !== holderPath[depth] && !(depth == 0 && KEPT.includes(key)))
                context["../".repeat(levels.length - depth) + key] = value
    const kept = {}
    for (const key of KEPT)
        if (key in doc)
            kept[key] = doc[key]
    const results = []
    for (const field of arrays)
        for (const element of holder[field])
            results.push({ ...(isPlainObject(element) ? element : { value: element }), unWrappedFrom: [...holderPath, field].join("."), ...context, ...kept })
    return results
}

const unwrapDocuments = (docs, options) => docs.flatMap(doc => unwrapDocument(doc, options))

// The limits apply to the unwrapped results (rows), not to the documents: one document can give many rows.
//
// A page ({ limit, skip, skipRows }) of the rows of each collection, from the answer of service.mongoQuery for the
// documents (asked with the same limit: a document gives at least one row). skip: the documents to skip (a number or
// { <collection>: n }, as for the documents); skipRows: { <collection>: n } the rows of the first of those documents
// already given by the previous page (a document cut between two pages). Answer as the documents page, with
// results: the rows, next: { <collection>: documents to skip } and nextRows: { <collection>: rows to skip in the
// first of them } (only the collections with more; send them back as skip / skipRows).
function unwrapPage(answer, page, searched, options) {
    const skipOf = c => typeof page.skip === "object" && page.skip !== null ? (page.skip[c] || 0) : (page.skip || 0)
    const results = []
    const next = {}
    const nextRows = {}
    for (const c of searched) {
        const docs = answer.results.filter(doc => doc._collection === c)
        const rows = []
        let stop
        for (let i = 0; i < docs.length && stop === undefined; i++) {
            const unwrapped = unwrapDocument(docs[i], options)
            const start = i == 0 ? Math.min(page.skipRows?.[c] || 0, unwrapped.length) : 0
            const taken = unwrapped.slice(start, start + page.limit - rows.length)
            rows.push(...taken)
            if (start + taken.length < unwrapped.length)
                stop = { docs: skipOf(c) + i, rows: start + taken.length } // the rest of this document: next page
            else if (rows.length >= page.limit && (i + 1 < docs.length || answer.next?.[c] !== undefined))
                stop = { docs: skipOf(c) + i + 1, rows: 0 }
        }
        if (stop === undefined && answer.next?.[c] !== undefined)
            stop = { docs: answer.next[c], rows: 0 }
        if (stop !== undefined) {
            next[c] = stop.docs
            if (stop.rows)
                nextRows[c] = stop.rows
        }
        results.push(...rows)
    }
    return { ...answer, results, hasMore: Object.keys(next).length > 0, next, nextRows }
}

// Without a page: at most `max` rows per collection (warnings: RESULTS_TRUNCATED, as for the documents)
function unwrapCapped(docs, max, searched, options, warnings = []) {
    const results = []
    for (const c of searched) {
        const rows = unwrapDocuments(docs.filter(doc => doc._collection === c), options)
        if (rows.length > max) {
            if (!warnings.some(w => w.code == "RESULTS_TRUNCATED" && w.source == c)) // the documents were cut already
                warnings.push({ kind: "runtime", code: "RESULTS_TRUNCATED", source: c, message: `Only the first ${max} unwrapped results of ${c} are returned: ask for pages (page: { limit, skip })` })
            rows.length = max
        }
        results.push(...rows)
    }
    return results
}

// An Advanced search answer: the list, or a page ({ results, hasMore, ... }: the page is still counted in documents)
function unwrapAnswer(answer, options) {
    if (Array.isArray(answer))
        return unwrapDocuments(answer, options)
    if (answer && Array.isArray(answer.results))
        return { ...answer, results: unwrapDocuments(answer.results, options) }
    return answer
}

module.exports = { unwrapDocument, unwrapDocuments, unwrapAnswer, unwrapPage, unwrapCapped, isPlainObject, pathSegments }
