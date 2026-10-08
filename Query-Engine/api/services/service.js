const logger = require('percocologger')
const log = logger.info
const Value = require('../models/Value')
const Key = require('../models/Key')
const Entries = require('../models/Entries')
const queryCache = require('./queryCache')
const { json2csv } = require('../../utils/common')
const config = require('../../config')
const minioWriter = require("../../inputConnectors/minioConnector")
const axios = require('axios')
const getClient = require('../../inputConnectors/postgresConnector')
const mongoose = require("mongoose")
let client
function setClient() {
    client = getClient()
}

let forbiddenTables = new Set(['users', 'credentials'])

// Visibility filter for keys/values/entries suggestions.
// With disableAuth there is no user to scope: no filter at all (every key, value and entry is visible).
function suggestionsVisibilityFilter(prefix, bucketName, visibility) {
    if (config.authConfig?.disableAuth)
        return {}
    if (visibility == "private")
        return { visibility: prefix.split("/")[0] }
    if (visibility == "shared")
        return { visibility: bucketName.toUpperCase() + " SHARED Data" }
    return { visibility: "public-data" }
}

// ---- Advanced search
const ADVANCED_SEARCH_MAX = () => Number(config.queryOptions?.advancedSearchMaxResults) > 0 ? Number(config.queryOptions.advancedSearchMaxResults) : 1000

// fileName / path / fileType of the MinIO object a document comes from (shown by the frontend)
function withFileInfo(obj) {
    if (typeof obj.name === "string") {
        const parts = obj.name.split("/")
        obj.fileName = parts[parts.length - 2]
        obj.path = obj.name
        obj.fileType = obj.name.split(".").pop()
    }
    return obj
}

// The Advanced search form sends text: a number typed in a field also matches that number, since numbers are
// stored as numbers (JSON files, the datapoints' value, ...) and "1234.5" alone would never match 1234.5.
const NUMBER_TEXT = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/
function textOrNumber(value) {
    return typeof value === "string" && NUMBER_TEXT.test(value.trim()) ? { $in: [value, Number(value)] } : value
}

// Where the fields of an Advanced search are looked for, by file type (see mongoQuery)
function formatFilter(query, format) {
    switch (format) {
        case "json": return { json: { $elemMatch: query } }
        case "json+object": return { $or: [query, { json: { $elemMatch: query } }] }
        case "csv": return { csv: { $elemMatch: query } }
        case "geojson": {
            const properties = {}
            for (const key in query)
                if (key != "coordinates")
                    properties[`properties.${key}`] = query[key]
            if (query.coordinates === undefined)
                return { features: { $elemMatch: properties } }
            // a coordinate at any depth of Point / LineString / Polygon / MultiPolygon geometries
            let coordinate = { $eq: Number(query.coordinates) }
            const depths = []
            for (let depth = 1; depth <= 4; depth++) {
                coordinate = { $elemMatch: coordinate }
                depths.push({ features: { $elemMatch: { ...properties, "geometry.coordinates": coordinate } } })
            }
            return { $or: depths }
        }
        default: return query
    }
}

// ---- keys / values / entries suggestions
// page = { limit, skip }: one page, sorted, { items, hasMore } (read limit + 1 to know if there is more).
// No page (older clients): at most SUGGESTIONS_MAX + 1 documents are read - never the whole collection - and above
// SUGGESTIONS_MAX keys / values answer the "too many" message, entries the first SUGGESTIONS_MAX.
// SUGGESTIONS_MAX: queryOptions.suggestionsMaxResults (default 500), also the highest page size.
const SUGGESTIONS_MAX = () => Number.isInteger(Number(config.queryOptions?.suggestionsMaxResults)) && Number(config.queryOptions.suggestionsMaxResults) > 0 ? Number(config.queryOptions.suggestionsMaxResults) : 500
const TOO_MANY_SUGGESTIONS = ["Too many suggestions. Type some characters in order to reduce them"]
const escapeRegex = text => String(text ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
const startsWith = text => ({ $regex: "^" + escapeRegex(text), $options: "i" })
const equalsIgnoringCase = text => ({ $regex: "^" + escapeRegex(text) + "$", $options: "i" })

async function suggestions(Model, filter, projection, sort, page, tooMany) {
    if (page) {
        const rows = await Model.find(filter, projection).sort(sort).skip(page.skip).limit(page.limit + 1).lean()
        return { items: rows.slice(0, page.limit), hasMore: rows.length > page.limit }
    }
    const max = SUGGESTIONS_MAX()
    const rows = await Model.find(filter, projection).limit(max + 1).lean()
    if (rows.length > max)
        return tooMany ? TOO_MANY_SUGGESTIONS : rows.slice(0, max)
    return rows
}

// Everything with disableAuth, otherwise only what the user may see for the selected visibility (see visibility.js).
const { objectFilter, collectionVisibilityFilter, visibleIn } = require('./visibility')
const { CONNECTORS, defaultCollections, storedCollections, collectionModel } = require('./collections')

// keys / values / entries of some collections only (the Source-Connector writes the connectors in `connectors`).
// No collections: queryOptions.defaultCollections (default api + minio: no datapoints). Documents written before the
// connectors existed have no `connectors`: they come from the API records and MinIO files (datapoints were not
// indexed then), so they count as api / minio until the rebuild.
function connectorsFilter(collections = defaultCollections()) {
    const conditions = [{ connectors: { $in: collections } }]
    if (collections.includes("api") || collections.includes("minio"))
        conditions.push({ connectors: { $exists: false } })
    return { $or: conditions }
}
// keys / values / entries of one Advanced search file type only: the formats of their refs (written by the
// Source-Connector, see its entriesStore.entriesFormat) that the file type searches (see formatFilter).
// Documents written before the formats existed have no `formats`: suggested for every file type until the rebuild.
const SUGGESTION_FORMATS = {
    json: ["object", "json"], // top-level fields or rows of JSON arrays ("json+object")
    csv: ["csv"],
    geojson: ["geojson"],
    object: ["object"]        // no file type: top-level fields only
}
function formatsFilter(format) {
    if (format === undefined)
        return {}
    return { $or: [{ formats: { $in: SUGGESTION_FORMATS[format] } }, { formats: { $exists: false } }] }
}
// the filters of the suggestions: collections and file type
const suggestionScope = (collections, format) => ({ $and: [connectorsFilter(collections), formatsFilter(format)] })
const simpleSearch = require('./simpleSearch')

async function listCollections() {
    try {
        const collections = await mongoose.connection.db.listCollections().toArray();
        const collectionNames = collections.map(c => c.name);
        return collectionNames;
    } catch (err) {
        logger.error(err);
    }
}

module.exports = {

    // GraphQL datapoints cache: one collection, versioned (services/queryCache.js)
    backupCache: queryCache.backupCache,
    restoreCache: queryCache.restoreCache,
    resetCache: queryCache.resetCache,
    resetBackup: queryCache.resetBackup,
    listCache: queryCache.listCache,

    listCollections,

    async manageCollections(view) {
        let collections = await this.listCollections()
        return view.replace(/\/\/ here[\s\S]*?\/\/ to here/, "const dbCollections =" + JSON.stringify(collections));
    },

    async deleteCollection(collectionName) {
        await mongoose.connection.dropCollection(collectionName);
    },

    SUGGESTIONS_MAX,
    SUGGESTION_FORMATS,

    // search: prefix, case insensitive, taken literally (not a regex)
    // collections: only the suggestions coming from those collections (undefined: all)
    // format: only the suggestions of that Advanced search file type (SUGGESTION_FORMATS; undefined: all)
    async getKeys(prefix, bucketName, visibility, search, page, collections, format) {
        const filter = { key: startsWith(search), ...suggestionsVisibilityFilter(prefix, bucketName, visibility), ...suggestionScope(collections, format) }
        return suggestions(Key, filter, { key: 1, _id: 0 }, { key: 1 }, page, true)
    },

    // Keys whose values are not (all) suggested: the Source-Connector leaves them out of values / entries for some
    // origins (orion.datapointsNotIndexed, e.g. the datapoints' `value`) and lists those origins in valuesNotIndexed.
    async getKeysWithValuesNotIndexed(prefix, bucketName, visibility, collections) {
        const filter = { "valuesNotIndexed.0": { $exists: true }, ...suggestionsVisibilityFilter(prefix, bucketName, visibility), ...connectorsFilter(collections) }
        const rows = await Key.collection.find(filter, { projection: { key: 1, _id: 0 } }).sort({ key: 1 }).limit(SUGGESTIONS_MAX()).toArray()
        return { keys: rows.map(row => row.key) }
    },

    async getValues(prefix, bucketName, visibility, search, page, collections, format) {
        const filter = { value: startsWith(search), ...suggestionsVisibilityFilter(prefix, bucketName, visibility), ...suggestionScope(collections, format) }
        return suggestions(Value, filter, { value: 1, _id: 0 }, { value: 1 }, page, true)
    },

    // MinIO files: record.insertedBy
    async updateOwner(bearer, email) {
        const Source = collectionModel("minio")
        let sources = await Source.find({ name: { $regex: email, $options: 'i' } });
        let sourcesDetails = (await axios.get(config.minioConfig.ownerInfoEndpoint + "/user/listFiles?email=" + email,
            {
                headers: {
                    Authorization: bearer
                }
            })).data
        for (let source of sources) {
            let owner
            let ownerEmail = sourcesDetails.find(obj => obj.objectPath == source.record.name)?.insertedBy
            if (ownerEmail) {
                source.record.insertedBy = ownerEmail
                await Source.updateOne({ _id: source._id }, { $set: { "record.insertedBy": ownerEmail } })
            }
            else
                try {
                    owner = (await axios.get(config.minioConfig.ownerInfoEndpoint + "/createdBy?filePath=" + source.record.name + "&etag=" + source.record.etag,
                        {
                            headers: {
                                Authorization: bearer
                            }
                        })).data
                    source.record.insertedBy = owner
                    await source.save()
                }
                catch (error) {
                    logger.error(error.toString())
                }
        }
        process.queryEngine.updatedOwners[email] = true
    },

    // exactKey / exactValue: that field must be the whole text (still case insensitive), not just start with it
    async getEntries(prefix, bucketName, visibility, searchKey, searchValue, page, { exactKey = false, exactValue = false, collections, format } = {}) {
        const filter = {
            key: exactKey ? equalsIgnoringCase(searchKey) : startsWith(searchKey),
            value: exactValue ? equalsIgnoringCase(searchValue) : startsWith(searchValue),
            ...suggestionsVisibilityFilter(prefix, bucketName, visibility),
            ...suggestionScope(collections, format)
        }
        return suggestions(Entries, filter, { key: 1, value: 1, _id: 0 }, { key: 1, value: 1 }, page, false)
    },


    async minioListObjects(bucketName) {
        return await minioWriter.listObjects(bucketName)
    },

    ADVANCED_SEARCH_MAX,

    /**
     * Advanced search on the collections of the connectors (collections.js): `collections` (default: api and
     * minio, see below; those not stored in MongoDB are skipped). Where the fields are looked for depends on the format:
     * "object" / none: top-level fields; "json": the rows of JSON arrays; "json+object": either (the "JSON" file
     * type); "csv": CSV rows; "geojson": feature properties (and coordinates) - in every collection.
     * Only what the user may see (visibility in the query, then checked on each document), sorted by _id in each
     * collection. Without `collections` (older clients): queryOptions.defaultCollections, the results as they were (no
     * `_collection`); with it, every result has `_collection` (its collection id).
     * page = { limit, skip }: up to `limit` results per collection; skip is a number (every collection) or
     * { <collection>: n }. Answer { results, hasMore, limit, skip, next }: next = { <collection>: skip } of the
     * collections with more results (send it as page.skip, with those collections, for the next page).
     * Without page: the list, at most ADVANCED_SEARCH_MAX documents per collection (a RESULTS_TRUNCATED warning,
     * with the collection as source, for each one that had more).
     */
    async mongoQuery(query, prefix, bucket, visibility, page, warnings = [], collections) {
        const format = query.format?.toLowerCase()
        const fields = { ...query }
        delete fields.format
        for (const key in fields)
            if (!(format == "geojson" && key == "coordinates")) // compared as a number already
                fields[key] = textOrNumber(fields[key])
        const searched = (collections || defaultCollections()).filter(c => storedCollections().includes(c))
        const max = ADVANCED_SEARCH_MAX()
        const limit = page ? page.limit : max
        const skipOf = c => !page ? 0 : typeof page.skip === "object" && page.skip !== null ? (page.skip[c] || 0) : (page.skip || 0)
        const perCollection = await Promise.all(searched.map(async c => {
            const filter = { $and: [formatFilter(fields, format), collectionVisibilityFilter(c, prefix, bucket, visibility)] }
            const rows = await collectionModel(c).find(filter).sort({ _id: 1 }).skip(skipOf(c)).limit(limit + 1).lean()
            const results = rows.slice(0, limit)
                .filter(obj => visibleIn(c, obj, prefix, bucket, visibility))
                .map(obj => c == "minio" ? withFileInfo(obj) : obj)
                .map(obj => collections ? { ...obj, _collection: c } : obj)
            return { c, results, more: rows.length > limit }
        }))
        const results = perCollection.flatMap(r => r.results)
        if (page) {
            const next = Object.fromEntries(perCollection.filter(r => r.more).map(r => [r.c, skipOf(r.c) + limit]))
            const skip = Object.fromEntries(searched.map(c => [c, skipOf(c)]))
            return { results, hasMore: Object.keys(next).length > 0, limit, skip, next }
        }
        for (const r of perCollection.filter(r => r.more))
            warnings.push({ kind: "runtime", code: "RESULTS_TRUNCATED", source: r.c, message: `Only the first ${max} results of ${r.c} are returned: ask for pages (page: { limit, skip })` })
        return results
    },

    // Simple search on the original sources: the MinIO files and - live, for public data - the APIs and Orion
    // (simpleSearch.js). `warnings` collects what was not searched or is incomplete (config and runtime).
    simpleSearchLimits() {
        return simpleSearch.limits()
    },

    // collections: minio (the files, read from MinIO), api / orion (searched live); default: all of them
    async rawQuery(query, prefix, bucket, visibility, warnings = [], collections = CONNECTORS) {
        logger.info("Raw query")
        let objects = []
        if (visibility == "public")
            bucket = "public-data"
        const options = simpleSearch.options()
        const selected = new Set(collections)
        if (options.minio !== false && selected.has("minio"))
            for (let obj of await minioWriter.listObjects(bucket)) {
                try {
                    if (obj.size && obj.isLatest) {
                        let objectGot = await minioWriter.getObject(bucket, obj.name, obj.name?.split(".").pop())
                        objects.push({ raw: objectGot, record: { ...obj, bucketName: bucket }, name: obj.name })
                    }
                }
                catch (error) {
                    logger.error(error)
                }
            }
        // API / Orion records are public data: searched for the public visibility (or everything, with disableAuth)
        if ((visibility == "public" || config.authConfig?.disableAuth) && (selected.has("api") || selected.has("orion"))) {
            warnings.push(...simpleSearch.limits().filter(w => w.collection != "minio" && selected.has(w.collection)))
            objects.push(...await simpleSearch.searchLive(query.value, warnings, { api: selected.has("api"), orion: selected.has("orion") }))
        }
        if (options.minio === false && selected.has("minio"))
            warnings.unshift(...simpleSearch.limits().filter(w => w.collection == "minio"))
        // no value ("Find all"): no text filter at all, only the visibility
        const value = query.value
        const contains = value
            ? obj => (typeof obj.raw == "string" ? obj.raw : JSON.stringify(obj.raw) ?? "").includes(value)
            : () => true
        return objects.filter(obj => objectFilter(obj, prefix, bucket, visibility) && contains(obj))

    },

    async querySQL(response, query, prefix, bucket, visibility) {
        /*if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(bucket))
            throw new Error('Invalid table name');
        else if (bucket == "users")
            bucket = "users_table"
        else if (bucket == "credentials")
            bucket = "credentials_table"
        else if (bucket == "default")
            bucket = "default_table"
        else if (bucket == "status")
            bucket = "status_table"
        else if (bucket == "sources")
            bucket = "sources_table"*/
        while (process.postgreInit == "busy")
            await new Promise(resolve => setTimeout(resolve, 1000));
        if (!client)
            setClient()
        /*client.query(
            `SELECT 1
             FROM information_schema.tables
             WHERE table_schema = 'public'
               AND table_name = $1`,
            [bucket], async (err, res) => {
                if (err) {
                    logger.error("ERROR");
                    logger.error(err);
                    return response.status(500).json(err.toString())
                }
                else if (res.rows.length === 0) {
                    logger.error("ERROR");
                    logger.error("Table does not exist");
                    return response.status(500).json("Table does not exist")
                }
                else
                    */
                    client.query(query, (err, res) => {
                        if (err) {
                            logger.error("ERROR");
                            logger.error(err);
                            response.status(500).json(err.toString())
                            logger.info("Query sql finished with errors")
                            return;
                        }
                        else {
                            response.send(res.rows.filter(obj => objectFilter(obj, prefix, bucket, visibility)).map(obj => obj.element && obj.name.split(".").pop() == "csv" ? { ...obj, element: json2csv(obj.element) } : obj))
                            logger.info(res.rows);
                            logger.info("Query sql finished")
                        }
                    });
            /*}
        );*/
    }
}