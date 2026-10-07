// Arguments of the GraphQL `sources` / `sourcesCount` queries -> MongoDB query on the `sources` collection.
//
// - filter: a MongoDB filter, as a JSON string (a GraphQL block string """...""" needs no escaping) or as a
//   JSON object passed through variables. Only read operators are accepted (see ALLOWED_OPERATORS): no $where,
//   $function, $expr, ... i.e. nothing that runs code or reads other collections.
// - name: case-insensitive "contains" on `name`; source: exact match on `source` (the API / Orion origin url).
// - limit / skip: limit defaults to queryOptions.graphQLDefaultLimit (100) and is capped at
//   queryOptions.graphQLMaxLimit (1000).
// - visibility: the same rule as visibility.objectFilter, translated into the query (so limit/skip and the
//   count apply to what the user may see); the resolvers still check every document with objectFilter.

const { UserInputError } = require('apollo-server-express')
const config = require('../../config')

const ALLOWED_OPERATORS = new Set([
    "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",
    "$exists", "$type", "$regex", "$options", "$not",
    "$and", "$or", "$nor",
    "$elemMatch", "$all", "$size", "$mod"
])
const MAX_DEPTH = 12
const DEFAULT_LIMIT = 100
const MAX_LIMIT = 1000
const NOTHING = { _id: { $in: [] } }

const escapeRegex = text => String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

function checkFilter(value, depth = 0) {
    if (depth > MAX_DEPTH)
        throw new UserInputError("filter: too deeply nested")
    if (Array.isArray(value))
        return value.forEach(item => checkFilter(item, depth + 1))
    if (value === null || typeof value !== "object")
        return
    for (const key of Object.keys(value)) {
        if (key.startsWith("$") && !ALLOWED_OPERATORS.has(key))
            throw new UserInputError(`filter: operator ${key} is not allowed (allowed: ${[...ALLOWED_OPERATORS].join(", ")})`)
        if (key.includes("\0"))
            throw new UserInputError("filter: invalid field name")
        checkFilter(value[key], depth + 1)
    }
}

function parseFilter(filter) {
    if (filter === undefined || filter === null || filter === "")
        return {}
    let parsed = filter
    if (typeof filter === "string")
        try {
            parsed = JSON.parse(filter)
        }
        catch (error) {
            throw new UserInputError("filter: invalid JSON (" + error.message + ")")
        }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        throw new UserInputError("filter: must be a JSON object, e.g. \"\"\"{\"record.bucketName\": \"public-data\"}\"\"\"")
    checkFilter(parsed)
    return parsed
}

function limits({ limit, skip } = {}) {
    const options = config.queryOptions || {}
    const max = Number(options.graphQLMaxLimit) > 0 ? Number(options.graphQLMaxLimit) : MAX_LIMIT
    const byDefault = Number(options.graphQLDefaultLimit) > 0 ? Number(options.graphQLDefaultLimit) : DEFAULT_LIMIT
    if (limit !== undefined && limit !== null && (!Number.isInteger(limit) || limit < 1))
        throw new UserInputError("limit must be a positive integer")
    if (skip !== undefined && skip !== null && (!Number.isInteger(skip) || skip < 0))
        throw new UserInputError("skip must be a non-negative integer")
    return { limit: Math.min(limit || Math.min(byDefault, max), max), skip: skip || 0 }
}

// visibility.objectFilter as a MongoDB query (req.body.prefix / bucketName are set by the auth middleware)
function visibilityQuery(req) {
    if (config.authConfig?.disableAuth)
        return {}
    const visibility = req?.headers?.visibility
    const prefix = req?.body?.prefix
    const bucket = req?.body?.bucketName
    const inBucket = name => ({ $or: [{ "record.s3.bucket.name": name }, { "record.bucketName": name }] })
    if (visibility == "private")
        return prefix ? { $or: [{ name: { $regex: escapeRegex(prefix) } }, { "record.name": { $regex: escapeRegex(prefix) } }] } : NOTHING
    if (visibility == "shared")
        return bucket ? { $and: [inBucket(bucket), { name: { $regex: escapeRegex(bucket.toUpperCase() + " SHARED Data/") } }] } : NOTHING
    if (visibility == "public")
        return {
            $or: [
                inBucket("public-data"),
                // API / Orion records: string source and no MinIO record, or a PostgreSQL-style record.from
                { source: { $type: "string" }, "record.bucketName": { $in: [null, ""] }, "record.s3": { $in: [null, ""] } },
                { "record.from": { $nin: [null, ""] } }
            ]
        }
    return NOTHING
}

function sourcesQuery(args = {}, req) {
    const parts = [parseFilter(args.filter), visibilityQuery(req)]
    if (args.name !== undefined && args.name !== null)
        parts.push({ name: { $regex: escapeRegex(args.name), $options: "i" } })
    if (args.source !== undefined && args.source !== null)
        parts.push({ source: args.source })
    const nonEmpty = parts.filter(part => Object.keys(part).length)
    return nonEmpty.length == 0 ? {} : nonEmpty.length == 1 ? nonEmpty[0] : { $and: nonEmpty }
}

module.exports = { ALLOWED_OPERATORS, parseFilter, limits, visibilityQuery, sourcesQuery, escapeRegex }
