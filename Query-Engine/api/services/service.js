const logger = require('percocologger')
const log = logger.info
const Source = require('../models/Source')
const Value = require('../models/Value')
const Key = require('../models/Key')
const Entries = require('../models/Entries')
const QueriesMap = require('../models/QueriesMap')
const QueriesMapBackup = require('../models/QueriesMapBackup')
const QueryCache = require("../models/QueryCache")
const QueryCacheBackup = require("../models/QueryCacheBackup")
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

// Everything with disableAuth, otherwise only what the user may see for the selected visibility (see visibility.js).
const { objectFilter } = require('./visibility')
const simpleSearch = require('./simpleSearch')

/*async function resetCache(queriesMapfilter, cacheFilter) {
    const { collections, queriesMap } = await filterCollections(queriesMapfilter, cacheFilter, QueriesMap)
    for (let coll of collections)
        await mongoose.connection.dropCollection(coll);
    return "done"
}*/

async function listCollections() {
    try {
        const collections = await mongoose.connection.db.listCollections().toArray();
        const collectionNames = collections.map(c => c.name);
        return collectionNames;
    } catch (err) {
        logger.error(err);
    }
}

/*async function filterCollections(queriesMapfilter, cacheFilter, Collection, BackupCollection) {
    let ids
    if (cacheFilter)
        queriesMapfilter = (queriesMapfilter || []).concat(cacheFilter)
    let collections = await listCollections()
    collections = collections.filter(coll => coll.toLowerCase().startsWith("cached"))
    let queriesMap = await Collection.find()
    if (queriesMapfilter) {
        queriesMap = queriesMap.filter(qm => queriesMapfilter.every(v => qm.query.toLowerCase().includes(v.toLowerCase())))
        ids = queriesMap.map(doc => doc._id);
        collections = collections.filter(coll =>
            ids.some(id => coll.includes(id))
        )
    }
    else
        ids = queriesMap.map(doc => doc._id)
    if (collections.includes("datapoints") || collections.includes("datapoint") || collections.includes("dimensions") || collections.includes("dimension"))
        throw new Error("I was going to delete wrong collections!")
    if (BackupCollection)
        await BackupCollection.insertMany(queriesMap);
    await Collection.deleteMany({
        _id: { $in: ids }
    })
    return { collections, queriesMap }
}*/

module.exports = {

    backupCache: async (queriesMapfilter, cacheFilter) => {

        let backupTimestamp = Date.now()

        let ids
        if (cacheFilter)
            queriesMapfilter = (queriesMapfilter || []).concat(cacheFilter)
        let collections = await listCollections()
        collections = collections.filter(coll => coll.toLowerCase().startsWith("cached"))
        let queriesMap = await QueriesMap.find()
        if (queriesMapfilter) {
            queriesMap = queriesMap.filter(qm => queriesMapfilter.every(v => qm.query.toLowerCase().includes(v.toLowerCase())))
            ids = queriesMap.map(doc => doc._id);
            collections = collections.filter(coll =>
                ids.some(id => coll.includes(id))
            )
        }
        else
            ids = queriesMap.map(doc => doc._id)
        if (collections.includes("datapoints") || collections.includes("datapoint") || collections.includes("dimensions") || collections.includes("dimension"))
            throw new Error("Bad filter")
        await QueriesMapBackup.insertMany(queriesMap);
        //const { collections, queriesMap } = await filterCollections(queriesMapfilter, cacheFilter, QueriesMap, QueriesMapBackup)
        for (let coll of collections) {
            let backupColl = coll + "_" + backupTimestamp + "_backup"
            await mongoose.connection.db.collection(backupColl).insertMany(
                await mongoose.connection.db.collection(coll).find({}).toArray()
            )
            //await mongoose.connection.db.renameCollection(coll, backupColl)
        }
        return "done"
    },

    restoreCache: async (queriesMapfilter, cacheFilter, timestamp) => {
        if (!timestamp)
            return "Missing timestamp"
        let ids, queries
        if (cacheFilter)
            queriesMapfilter = (queriesMapfilter || []).concat(cacheFilter)
        let collections = await listCollections()
        collections = collections.filter(coll => coll.toLowerCase().endsWith("_backup") && coll.toLowerCase().startsWith("cached") && coll.toLowerCase().split("_")[coll.toLowerCase().split("_").length - 2] == timestamp)
        let queriesMap = (await QueriesMapBackup.find().lean()).filter(qm => collections.some(coll => coll.includes(qm._id)))// && (!queriesMapfilter || queriesMapfilter.every(v => qm.query.toLowerCase().includes(v.toLowerCase()))))
        if (queriesMapfilter) {
            queriesMap = queriesMap.filter(qm => queriesMapfilter.every(v => qm.query.toLowerCase().includes(v.toLowerCase())))
            ids = queriesMap.map(doc => doc._id);
            collections = collections.filter(coll =>
                ids.some(id => coll.includes(id))
            )
            queries = queriesMap.map(doc => doc.query)
        }
        else {
            ids = queriesMap.map(doc => doc._id)
            queries = queriesMap.map(doc => doc.query)
        }
        if (collections.includes("datapoints") || collections.includes("datapoint") || collections.includes("dimensions") || collections.includes("dimension"))
            throw new Error("Bad filter")
        await QueriesMap.deleteMany({ query: { $in: queries } });
        await QueriesMap.insertMany(queriesMap);
        /*await QueriesMapBackup.deleteMany({
            _id: { $in: ids }
        })*/
        //const { collections, queriesMap } = await filterCollections(queriesMapfilter, cacheFilter, QueriesMapBackup, QueriesMap)
        for (let coll of collections) {
            let restoredColl = coll.replace("_backup", "")
            restoredColl = restoredColl.substring(0, restoredColl.lastIndexOf("_"))
            /*let splittedRestoredColl = restoredColl.split("_") 
            splittedRestoredColl.pop()
            if (splittedRestoredColl.length > 1)
                restoredColl = splittedRestoredColl.join("_")
            else
                restoredColl = splittedRestoredColl[0]*/
            let deletingColl = (await listCollections()).find(c => c.toLowerCase().split(":").shift() == restoredColl.toLowerCase().split(":").shift() && c != coll)
            if (deletingColl)
                await mongoose.connection.dropCollection(deletingColl);
            if (await mongoose.connection.db.collection(restoredColl).countDocuments() > 0)
                await mongoose.connection.db.collection(restoredColl).drop()
            await mongoose.connection.db.collection(restoredColl).insertMany(
                await mongoose.connection.db.collection(coll).find({}).toArray()
            )
        }
        return "done"
    },

    listCollections,

    async manageCollections(view) {
        let collections = await this.listCollections()
        return view.replace(/\/\/ here[\s\S]*?\/\/ to here/, "const dbCollections =" + JSON.stringify(collections));
    },

    async resetCache(queriesMapfilter, cacheFilter) {
        //return await resetCache(queriesMapfilter, cacheFilter)
        let ids
        if (cacheFilter)
            queriesMapfilter = (queriesMapfilter || []).concat(cacheFilter)
        let collections = await listCollections()
        collections = collections.filter(coll => coll.toLowerCase().startsWith("cached") && !coll.toLowerCase().endsWith("_backup"))
        let queriesMap = await QueriesMap.find()
        if (queriesMapfilter) {
            queriesMap = queriesMap.filter(qm => queriesMapfilter.every(v => qm.query.toLowerCase().includes(v.toLowerCase())))
            ids = queriesMap.map(doc => doc._id);
            collections = collections.filter(coll =>
                ids.some(id => coll.includes(id))
            )
        }
        else
            ids = queriesMap.map(doc => doc._id)
        if (collections.includes("datapoints") || collections.includes("datapoint") || collections.includes("dimensions") || collections.includes("dimension"))
            throw new Error("I was going to delete wrong collections!")
        await QueriesMap.deleteMany({
            _id: { $in: ids }
        })
        for (let coll of collections)
            await mongoose.connection.dropCollection(coll);
        return "done"
    },

    async resetBackup(queriesMapfilter, cacheFilter) {
        //return await resetCache(queriesMapfilter, cacheFilter)
        let ids
        if (cacheFilter)
            queriesMapfilter = (queriesMapfilter || []).concat(cacheFilter)
        let collections = await listCollections()
        collections = collections.filter(coll => coll.toLowerCase().startsWith("cached") && coll.toLowerCase().endsWith("_backup"))
        let queriesMap = await QueriesMapBackup.find()
        if (queriesMapfilter) {
            queriesMap = queriesMap.filter(qm => queriesMapfilter.every(v => qm.query.toLowerCase().includes(v.toLowerCase())))
            ids = queriesMap.map(doc => doc._id);
            collections = collections.filter(coll =>
                ids.some(id => coll.includes(id))
            )
        }
        else
            ids = queriesMap.map(doc => doc._id)
        if (collections.includes("datapoints") || collections.includes("datapoint") || collections.includes("dimensions") || collections.includes("dimension"))
            throw new Error("I was going to delete wrong collections!")
        await QueriesMapBackup.deleteMany({
            _id: { $in: ids }
        })
        for (let coll of collections)
            await mongoose.connection.dropCollection(coll);
        return "done"
    },

    async deleteCollection(collectionName) {
        await mongoose.connection.dropCollection(collectionName);
    },

    async getKeys(prefix, bucketName, visibility, search) {
        console.debug({ visibility, prefix })
        const visibilityFilter = suggestionsVisibilityFilter(prefix, bucketName, visibility)
        console.debug(visibilityFilter)
        let keys = await Key.find({
            key: { $regex: "^" + search, $options: "i" },
            ...visibilityFilter
        }, { "key": 1, "_id": 0 })
        if (keys.length > 500)
            return ["Too many suggestions. Type some characters in order to reduce them"]
        return keys
    },

    async getValues(prefix, bucketName, visibility, search) {
        const visibilityFilter = suggestionsVisibilityFilter(prefix, bucketName, visibility)
        console.debug(visibilityFilter)
        let values = await Value.find({
            value: { $regex: "^" + search, $options: "i" },
            ...visibilityFilter
        }, { "value": 1, "_id": 0 })
        if (values.length > 500)
            return ["Too many suggestions. Type some characters in order to reduce them"]
        return values
    },

    async updateOwner(bearer, email) {
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

    async getEntries(prefix, bucketName, visibility, searchKey, searchValue) {
        const visibilityFilter = suggestionsVisibilityFilter(prefix, bucketName, visibility)
        console.debug(visibilityFilter)
        let entries = await Entries.find({
            "key": { $regex: "^" + searchKey, $options: "i" },
            "value": { $regex: "^" + searchValue, $options: "i" },
            ...visibilityFilter
        }, { "key": 1, "value": 1, "_id": 0 })

        return entries
    },


    async exampleQueryCSV(query) {

        return await Source.find({
            "csv": {
                $elemMatch: query
            }
        })
    },

    async minioListObjects(bucketName) {
        return await minioWriter.listObjects(bucketName)
    },

    async exampleQueryJson(query) {
        logger.debug("example query json: query ", query)

        return await Source.find({
            "json": {
                $elemMatch: query
            }
        })
    },

    async exampleQueryGeoJson(query) {

        logger.debug("example query geojson: query ", query)

        let found = []
        let propertiesQuery = {}
        //TODO now there is a preset deep level search, but this level should be parametrized

        for (let key in query)
            if (key != "coordinates")
                propertiesQuery[`properties.${key}`] = query[key]

        if (query.coordinates)
            found.push(
                ...(await Source.find({
                    "features": {
                        $elemMatch: {
                            ...propertiesQuery,
                            "geometry.coordinates": {
                                $elemMatch: {
                                    $elemMatch: {
                                        $elemMatch: {
                                            $elemMatch: {
                                                $eq: Number(query.coordinates)
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                })),
                ...(await Source.find({
                    "features": {
                        $elemMatch: {
                            ...propertiesQuery,
                            "geometry.coordinates": {
                                $elemMatch: {
                                    $elemMatch: {
                                        $elemMatch: {
                                            $elemMatch: {
                                                $eq: Number(query.coordinates)
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                })),
                ...(await Source.find({
                    "features": {
                        $elemMatch: {
                            ...propertiesQuery,
                            "geometry.coordinates": {
                                $elemMatch: {
                                    $elemMatch: {
                                        $elemMatch: {
                                            $eq: Number(query.coordinates)
                                        }
                                    }
                                }
                            }
                        }
                    }
                })),
                ...(await Source.find({
                    "features": {
                        $elemMatch: {
                            ...propertiesQuery,
                            "geometry.coordinates": {
                                $elemMatch: {
                                    $elemMatch: {
                                        $eq: Number(query.coordinates)
                                    }
                                }
                            }
                        }
                    }
                })),
                ...(await Source.find({
                    "features": {
                        $elemMatch: {
                            ...propertiesQuery,
                            "geometry.coordinates": {
                                $elemMatch: {

                                    $eq: Number(query.coordinates)
                                }
                            }
                        }
                    }
                }))
            )

        else
            found = await Source.find({
                "features": {
                    $elemMatch: {
                        ...propertiesQuery
                    }
                }
            })

        return found
    },

    async simpleQuery(query) {
        let result = await Source.find(query)
        for (let obj of result) {
            obj.fileName = obj.name?.split("/")[obj.name.split("/").length - 2]
            obj.path = obj.name
            obj.fileType = obj.name?.split(".")[obj.name.split(".").length - 1]
        }
        logger.info(result)
        return result
    },

    async mongoQuery(query, prefix, bucket, visibility) {
        logger.debug("format ", query.format)
        let format = query.format?.toLowerCase()
        if (format)
            delete query["format"]
        logger.debug("format ", format)
        switch (format) {
            case "geojson": return (await this.exampleQueryGeoJson(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "csv": return (await this.exampleQueryCSV(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "json": return (await this.exampleQueryJson(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            case "object": return (await this.simpleQuery(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
            default: return (await this.simpleQuery(query)).filter(obj => objectFilter(obj, prefix, bucket, visibility))
        }
    },

    // Simple search on the original sources: the MinIO files and - live, for public data - the APIs and Orion
    // (simpleSearch.js). `warnings` collects what was not searched or is incomplete (config and runtime).
    simpleSearchLimits() {
        return simpleSearch.limits()
    },

    async rawQuery(query, prefix, bucket, visibility, warnings = []) {
        logger.info("Raw query")
        let objects = []
        if (visibility == "public")
            bucket = "public-data"
        const options = simpleSearch.options()
        if (options.minio !== false)
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
        if (visibility == "public" || config.authConfig?.disableAuth) {
            warnings.push(...simpleSearch.limits().filter(w => w.code != "MINIO_DISABLED"))
            objects.push(...await simpleSearch.searchLive(query.value, warnings))
        }
        if (options.minio === false)
            warnings.unshift(...simpleSearch.limits().filter(w => w.code == "MINIO_DISABLED"))
        return objects.filter(obj => typeof obj.raw == "string" ? objectFilter(obj, prefix, bucket, visibility) && (!query.value || obj.raw.includes(query.value)) : objectFilter(obj, prefix, bucket, visibility) && (!query.value || JSON.stringify(obj.raw).includes(query.value)))

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