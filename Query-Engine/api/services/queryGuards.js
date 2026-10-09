// Guards of the MongoDB queries built from what the user sends (Advanced search fields, GraphQL `filter`):
//
// - checkFilter: only read operators (ALLOWED_OPERATORS) - no $where, $function, $expr, $accumulator, ... i.e.
//   nothing that runs code on the server or reads other collections; a bounded nesting depth. The queries only read
//   (find / count / aggregate pipelines built by the code): nothing the user sends can update or delete.
// - maxTimeMS: queryOptions.mongoMaxTimeMS, the longest a query may run on MongoDB before it is stopped (default 15
//   minutes: some legitimate queries on tens of millions of datapoints take minutes; 0 = no limit). Against queries
//   that would keep MongoDB busy for good (huge scans, catastrophic regular expressions).

const config = require("../../config")

const ALLOWED_OPERATORS = new Set([
    "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",
    "$exists", "$type", "$regex", "$options", "$not",
    "$and", "$or", "$nor",
    "$elemMatch", "$all", "$size", "$mod"
])
const MAX_DEPTH = 12
const DEFAULT_MAX_TIME_MS = 15 * 60 * 1000

const badRequest = message => Object.assign(new Error(message), { status: 400 })

function checkFilter(value, depth = 0, label = "filter") {
    if (depth > MAX_DEPTH)
        throw badRequest(`${label}: too deeply nested`)
    if (Array.isArray(value))
        return value.forEach(item => checkFilter(item, depth + 1, label))
    if (value === null || typeof value !== "object")
        return
    for (const key of Object.keys(value)) {
        if (key.startsWith("$") && !ALLOWED_OPERATORS.has(key))
            throw badRequest(`${label}: operator ${key} is not allowed (allowed: ${[...ALLOWED_OPERATORS].join(", ")})`)
        if (key.includes("\0"))
            throw badRequest(`${label}: invalid field name`)
        checkFilter(value[key], depth + 1, label)
    }
}

// ms, or undefined (no limit)
function maxTimeMS() {
    const configured = config.queryOptions?.mongoMaxTimeMS
    if (configured === undefined || configured === null || configured === "")
        return DEFAULT_MAX_TIME_MS
    const ms = Number(configured)
    return Number.isFinite(ms) && ms > 0 ? ms : undefined
}

// the options of a find / count / aggregate: { maxTimeMS } when there is a limit
const timeLimit = () => {
    const ms = maxTimeMS()
    return ms ? { maxTimeMS: ms } : {}
}

// HTTP status of a query error: 504 when MongoDB stopped it (maxTimeMS)
function httpStatus(error) {
    if (error?.status)
        return error.status
    return error?.code === 50 || error?.codeName === "MaxTimeMSExpired" ? 504 : 500
}

module.exports = { ALLOWED_OPERATORS, MAX_DEPTH, checkFilter, maxTimeMS, timeLimit, httpStatus }
