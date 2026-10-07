// Which query results a user may see. Shared by the REST queries (service.js) and GraphQL (resolvers.js).
//
// - authConfig.disableAuth: there's no user to scope, everything is visible (same as the keys/values/entries
//   suggestions);
// - otherwise only what belongs to the selected visibility:
//   private -> the user's own MinIO files (object path under their prefix, "<email>/<input folder>");
//   shared  -> the "<BUCKET> SHARED Data/" folder of the user's bucket (pilot);
//   public  -> the public-data bucket, plus API / Orion records, which are public data.
const config = require('../../config')

function bucketIs(record, bucket) {
    return (record?.s3?.bucket?.name == bucket || record?.bucketName == bucket)
}

// API / Orion records (Source-Connector sourceRecords.replaceRecords / apiConnector): `source` is the origin url
// and there's no MinIO record; their PostgreSQL rows (`sources` table) carry record.from instead.
function isApiRecord(obj) {
    return (typeof obj?.source === "string" && !obj?.record?.bucketName && !obj?.record?.s3) || !!obj?.record?.from
}

function objectFilter(obj, prefix, bucket, visibility) {
    if (config.authConfig?.disableAuth)
        return true
    if (visibility == "private" && prefix && (obj?.record?.name?.includes(prefix) || obj?.name?.includes(prefix)))
        return true
    if (visibility == "shared" && bucketIs(obj?.record, bucket) && obj?.name?.includes(bucket?.toUpperCase() + " SHARED Data/"))
        return true
    if (visibility == "public" && (bucketIs(obj?.record, "public-data") || isApiRecord(obj)))
        return true
    return false
}

const escapeRegex = text => String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
const NOTHING = { _id: { $in: [] } }

// objectFilter as a MongoDB query (so that limit / skip apply to what the user may see): the queries add it to
// their filter and still check every document with objectFilter.
function visibilityMongoFilter(prefix, bucket, visibility) {
    if (config.authConfig?.disableAuth)
        return {}
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

// Per collection (api/services/collections.js): API and Orion records are public data (visible with the public
// visibility, or with disableAuth); MinIO files follow the rules above.
function collectionVisibilityFilter(connector, prefix, bucket, visibility) {
    if (connector == "minio")
        return visibilityMongoFilter(prefix, bucket, visibility)
    return config.authConfig?.disableAuth || visibility == "public" ? {} : NOTHING
}

function visibleIn(connector, obj, prefix, bucket, visibility) {
    if (connector == "minio")
        return objectFilter(obj, prefix, bucket, visibility)
    return !!(config.authConfig?.disableAuth || visibility == "public")
}

module.exports = { bucketIs, isApiRecord, objectFilter, visibilityMongoFilter, collectionVisibilityFilter, visibleIn }
