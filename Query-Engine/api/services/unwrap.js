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

function unwrapDocument(doc, { onlyArrayOfObjects = false } = {}) {
    if (!isPlainObject(doc))
        return [doc]
    const arrays = Object.keys(doc).filter(key => !KEPT.includes(key) && unwrappable(doc[key], onlyArrayOfObjects))
    if (!arrays.length)
        return [doc]
    const context = {}
    const kept = {}
    for (const [key, value] of Object.entries(doc))
        if (KEPT.includes(key))
            kept[key] = value
        else if (!arrays.includes(key))
            context["../" + key] = value
    const results = []
    for (const field of arrays)
        for (const element of doc[field])
            results.push({ ...(isPlainObject(element) ? element : { value: element }), unWrappedFrom: field, ...context, ...kept })
    return results
}

const unwrapDocuments = (docs, options) => docs.flatMap(doc => unwrapDocument(doc, options))

// An Advanced search answer: the list, or a page ({ results, hasMore, ... }: the page is still counted in documents)
function unwrapAnswer(answer, options) {
    if (Array.isArray(answer))
        return unwrapDocuments(answer, options)
    if (answer && Array.isArray(answer.results))
        return { ...answer, results: unwrapDocuments(answer.results, options) }
    return answer
}

module.exports = { unwrapDocument, unwrapDocuments, unwrapAnswer, isPlainObject }
