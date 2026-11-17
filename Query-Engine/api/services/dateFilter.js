// Date filters of the Advanced search: a field whose value is { $gte, $lte } (or $gt / $lt) with dates as bounds, as
// the frontend sends its "Date" filters (ISO 8601 strings). The key is the path of the field from the top of the
// document, as for the other fields (e.g. data.record.created_at: created_at of the record of any element of data).
//   - a bound may be missing or null: only the other one applies; none left: the filter is dropped (no condition)
//   - the stored value is read as a date: a BSON Date, or a string beginning with YYYY-MM-DD in any ISO 8601 form
//     (with or without time, milliseconds, Z or an offset); other values never match
//   - every bound holds for the same value: on an array (data.record.created_at), one element must be in the range,
//     not one element after the start and another before the end
//   - with unwrap (services/unwrap.js), a field inside an unwrapped array: only the elements in the range are returned
// MongoDB compares strings as strings: it is asked for a range one day wider, as text (or exact, as BSON Date), and the
// documents (then the unwrapped elements) found are checked here, as dates. JSON / CSV file types: the field may be in
// the rows (json.<key>, csv.<key>), as the other fields. GeoJSON: not handled here (the fields are looked for in the
// properties of the features, see service.geojsonFilter): the bounds are sent as they are, the null ones dropped.

const LOWER = ["$gte", "$gt"]
const UPPER = ["$lte", "$lt"]
const DATE_TEXT = /^\d{4}-\d{2}-\d{2}/
const DAY = 24 * 3600 * 1000
const ymd = time => new Date(time).toISOString().split("T")[0]

const isPlainObject = value => value !== null && typeof value == "object" && !Array.isArray(value) && !(value instanceof Date)

// The bounds of a date filter, the empty ones left out ({} when none is left); undefined: not a date filter
function dateBounds(value) {
    if (!isPlainObject(value))
        return undefined
    const ops = Object.keys(value)
    if (!ops.length || ops.some(op => !LOWER.includes(op) && !UPPER.includes(op)))
        return undefined
    const bounds = {}
    for (const op of ops) {
        const bound = value[op]
        if (bound === null || bound === undefined || bound === "")
            continue
        if (typeof bound != "string" || !DATE_TEXT.test(bound) || isNaN(Date.parse(bound)))
            return undefined
        bounds[op] = bound
    }
    return bounds
}

// A stored value as a time (ms), NaN when it is not a date
function timeOf(value) {
    if (value instanceof Date)
        return value.getTime()
    if (typeof value == "string" && DATE_TEXT.test(value))
        return Date.parse(value)
    return NaN
}

function inRange(value, bounds) {
    const time = timeOf(value)
    if (isNaN(time))
        return false
    for (const [op, bound] of Object.entries(bounds)) {
        const limit = Date.parse(bound)
        if ((op == "$gte" && !(time >= limit)) || (op == "$gt" && !(time > limit)) ||
            (op == "$lte" && !(time <= limit)) || (op == "$lt" && !(time < limit)))
            return false
    }
    return true
}

// The values at a dotted path, through arrays as MongoDB does (a numeric segment is also an index)
function valuesAt(value, segments) {
    if (Array.isArray(value)) {
        const values = value.flatMap(element => valuesAt(element, segments))
        if (segments.length && /^\d+$/.test(segments[0]))
            values.push(...valuesAt(value[Number(segments[0])], segments.slice(1)))
        return values
    }
    if (!segments.length)
        return [value]
    if (!isPlainObject(value))
        return []
    return valuesAt(value[segments[0]], segments.slice(1))
}

// Where a field is looked for, by file type (as service.formatFilter does)
function pathsOf(key, format) {
    switch (format) {
        case "json": return ["json." + key]
        case "json+object": return [key, "json." + key]
        case "csv": return ["csv." + key]
        default: return [key]
    }
}

// The fields of an Advanced search split: the date filters out of them ({ key, bounds }), with the conditions for
// MongoDB in their place ($and, one day wider). format: the file type of the search (lower case).
function splitDateFilters(fields, format) {
    const rest = {}
    const dates = []
    for (const [key, value] of Object.entries(fields)) {
        const bounds = key.startsWith("$") ? undefined : dateBounds(value)
        if (bounds === undefined)
            rest[key] = value
        else if (format == "geojson") {
            if (Object.keys(bounds).length)
                rest[key] = bounds
        }
        else if (Object.keys(bounds).length)
            dates.push({ key, bounds })
    }
    if (dates.length)
        rest.$and = [...(Array.isArray(rest.$and) ? rest.$and : []), ...dates.map(({ key, bounds }) => {
            const asText = {}, asDate = {}
            for (const [op, bound] of Object.entries(bounds)) {
                const time = Date.parse(bound)
                asDate[op] = new Date(time)
                if (LOWER.includes(op))
                    asText.$gte = ymd(time - DAY)
                else
                    asText.$lte = ymd(time + DAY) + "￿" // after any text of that day
            }
            return { $or: [{ [key]: asText }, { [key]: asDate }] }
        })]
    return { fields: rest, dates }
}

// Does a document match the date filters (one value in the range each, at one of the paths of its file type)?
function documentMatches(doc, dates, format) {
    return dates.every(({ key, bounds }) =>
        pathsOf(key, format).some(path => valuesAt(doc, path.split(".")).some(value => inRange(value, bounds))))
}

// An unwrapped result (unWrappedFrom: the path of its array): for every date filter whose field is inside that array,
// the value of the element must be in the range; the other filters were checked on the document
function rowMatches(row, dates, format) {
    const from = row?.unWrappedFrom
    if (typeof from != "string")
        return true
    return dates.every(({ key, bounds }) => {
        const inside = pathsOf(key, format).filter(path => path == from || path.startsWith(from + "."))
        if (!inside.length)
            return true
        return inside.some(path => (path == from ? [row.value] : valuesAt(row, path.slice(from.length + 1).split(".")))
            .some(value => inRange(value, bounds)))
    })
}

module.exports = { splitDateFilters, documentMatches, rowMatches, dateBounds, valuesAt }
