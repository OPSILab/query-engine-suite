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

module.exports = { bucketIs, isApiRecord, objectFilter }
