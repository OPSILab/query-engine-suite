// Arguments of the GraphQL `sources` / `sourcesCount` queries -> MongoDB query on the `sources` collection.
//
// - filter: a MongoDB filter, as a JSON string (a GraphQL block string """...""" needs no escaping) or as a
//   JSON object passed through variables. Only read operators are accepted (see ALLOWED_OPERATORS): no $where,
//   $function, $expr, ... i.e. nothing that runs code or reads other collections.
// - name: case-insensitive "contains" on `name`; source: exact match on the origin url (api: `source`,
//   orion: `fromUrl`; minio: `source`, a field of the file if any).
// - collections: the collections searched (collections.js); default queryOptions.defaultCollections (api and minio,
//   the old `sources` collection - the Orion datapoints are read with `datapoints`, or with collections: ["orion"]).
//   limit / skip apply to each collection.
// - limit / skip: limit defaults to queryOptions.graphQLDefaultLimit (100 if not set) and is capped at
//   queryOptions.graphQLMaxLimit (1000 if not set).
// - visibility: the same rule as visibility.objectFilter, translated into the query (so limit/skip and the
//   count apply to what the user may see); the resolvers still check every document with objectFilter.

const { UserInputError } = require('apollo-server-express')
const config = require('../../config')
const { collectionVisibilityFilter } = require('../services/visibility')
const { DEFAULT_COLLECTIONS, defaultCollections, collectionSettings, storedCollections, parseCollections } = require('../services/collections')

const ALLOWED_OPERATORS = new Set([
    "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",
    "$exists", "$type", "$regex", "$options", "$not",
    "$and", "$or", "$nor",
    "$elemMatch", "$all", "$size", "$mod"
])
const MAX_DEPTH = 12
const DEFAULT_LIMIT = 100
const MAX_LIMIT = 1000

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

// The visibility of a collection as a MongoDB query (req.body.prefix / bucketName are set by the auth middleware)
function visibilityQuery(req, connector = "minio") {
    return collectionVisibilityFilter(connector, req?.body?.prefix, req?.body?.bucketName, req?.headers?.visibility)
}

// The collections of a sources / sourcesCount query (those not stored in MongoDB are left out)
function sourcesCollections(args = {}) {
    let list
    try {
        list = parseCollections(args.collections) || defaultCollections()
    }
    catch (error) {
        throw new UserInputError(error.message)
    }
    return list.filter(c => storedCollections().includes(c))
}

function sourcesQuery(args = {}, req, connector = "minio") {
    const parts = [parseFilter(args.filter), visibilityQuery(req, connector)]
    if (args.name !== undefined && args.name !== null)
        parts.push({ name: { $regex: escapeRegex(args.name), $options: "i" } })
    if (args.source !== undefined && args.source !== null)
        parts.push({ [collectionSettings(connector).originField]: args.source })
    const nonEmpty = parts.filter(part => Object.keys(part).length)
    return nonEmpty.length == 0 ? {} : nonEmpty.length == 1 ? nonEmpty[0] : { $and: nonEmpty }
}

module.exports = { ALLOWED_OPERATORS, DEFAULT_COLLECTIONS, parseFilter, limits, visibilityQuery, sourcesCollections, sourcesQuery, escapeRegex }
