const service = require("../services/service.js")
const logger = require('percocologger')
const config = require('../../config')
const fs = require("fs")
const simpleSearch = require("../services/simpleSearch")
const { CONNECTORS, collectionSettings, parseCollections } = require("../services/collections")
const manageCollectionsView = fs.readFileSync("api/view/manage-collections.html", "utf-8")

// ?limit=&skip= of the keys / values / entries suggestions: a page ({ items, hasMore }), else undefined (legacy list)
function suggestionsPage(query) {
    if (query.limit === undefined)
        return undefined
    const limit = Number(query.limit)
    const skip = query.skip === undefined ? 0 : Number(query.skip)
    if (!Number.isInteger(limit) || limit < 1 || limit > service.SUGGESTIONS_MAX || !Number.isInteger(skip) || skip < 0)
        throw Object.assign(new Error(`limit must be 1-${service.SUGGESTIONS_MAX}, skip >= 0`), { status: 400 })
    return { limit, skip }
}
const isTrue = value => value === "true" || value === true

// Advanced search page: body.page = { limit, skip } - in the body, the query string carries the searched fields.
// skip: a number (every collection) or { <collection>: n } (the `next` of the previous page).
// Without it: the first queryOptions.advancedSearchMaxResults results of each collection (see service.mongoQuery).
function advancedSearchPage(body) {
    if (body?.page === undefined || body.page === null)
        return undefined
    const limit = Number(body.page.limit)
    const error = () => new Error(`page: limit must be 1-${service.ADVANCED_SEARCH_MAX()}, skip >= 0 (a number or { ${CONNECTORS.join(", ")} })`)
    if (!Number.isInteger(limit) || limit < 1 || limit > service.ADVANCED_SEARCH_MAX())
        throw error()
    const validSkip = n => Number.isInteger(n) && n >= 0
    const raw = body.page.skip
    if (raw !== null && typeof raw === "object") {
        const skip = {}
        for (const [c, n] of Object.entries(raw)) {
            if (!CONNECTORS.includes(c) || !validSkip(Number(n)))
                throw error()
            skip[c] = Number(n)
        }
        return { limit, skip }
    }
    const skip = raw === undefined ? 0 : Number(raw)
    if (!validSkip(skip))
        throw error()
    return { limit, skip }
}

const queryMongo = async (req, res) => {
    logger.info(req.body, req.query)
    if (req.headers.israwquery) {
        const warnings = []
        let collections
        try {
            collections = parseCollections(req.query.collections)
        }
        catch (error) {
            return res.status(400).send(error.message)
        }
        const results = await service.rawQuery(req.query, req.body.prefix, req.body.bucketName, req.headers.visibility, warnings, collections)
        // what was not searched / is incomplete: [{ kind, code, source?, message }], URI-encoded JSON
        if (warnings.length)
            res.set("X-Query-Warnings", encodeURIComponent(JSON.stringify(warnings)))
        res.send(results)
        return logger.info("Raw query finished")
    }
    // Advanced search
    const warnings = []
    let page, collections
    try {
        page = advancedSearchPage(req.body)
        collections = parseCollections(req.body.collections) // in the body too: the query string has the fields
    }
    catch (error) {
        return res.status(400).send(error.message)
    }
    // "JSON" file type: the fields at the top level or in the rows of JSON arrays (one query: it used to be two
    // queries whose results were concatenated, so a document matching both came twice)
    const query = req.query.format == "JSON"
        ? { ...JSON.parse(JSON.stringify(req.body.mongoQuery || req.query)), format: "json+object" }
        : { ...req.body.mongoQuery, ...req.query }
    const result = await service.mongoQuery(query, req.body.prefix, req.body.bucketName, req.headers.visibility, page, warnings, collections)
    if (warnings.length)
        res.set("X-Query-Warnings", encodeURIComponent(JSON.stringify(warnings)))
    res.send(result)
    logger.info("Query mongo finished")
}

const querySQL = async (req, res) => {
    logger.info("Query sql")
    if (!req.body.query)
        return await res.status(400).send("Missing query")
    logger.info("Query : ", req.body.query)
    await service.querySQL(res, req.body.query, req.body.prefix, req.body.bucketName, req.headers.visibility)
}

module.exports = {

    queryMongo,

    // What the configuration leaves out of the simple search (shown by the frontend as a warning)
    // The collections the frontend can offer: { collections: [{ id, advancedSearch, simpleSearch }] }
    // advancedSearch: stored in MongoDB (collections.<id>.toMongo); simpleSearch: searched by the Simple search
    // (simpleSearchOptions). The labels shown to the user are in the frontend config.
    getCollections: async (req, res) => {
        const live = simpleSearch.options()
        res.send({
            collections: CONNECTORS.map(id => ({
                id,
                advancedSearch: collectionSettings(id).toMongo,
                simpleSearch: id == "orion" ? live.orion === true : live[id] !== false
            }))
        })
    },

    simpleSearchLimits: async (req, res) => {
        res.send({ warnings: service.simpleSearchLimits() })
    },

    querySQL,

    resetCache: async (req, res) => {
        res.send(await service.resetCache(req.query.queriesMapFilter, req.query.cacheFilter))
    },

    backupCache: async (req, res) => {
        res.send(await service.backupCache(req.query.queriesMapFilter, req.query.cacheFilter))
    },

    restoreCache: async (req, res) => {
        res.send(await service.restoreCache(req.query.queriesMapFilter, req.query.cacheFilter, req.query.timestamp))
    },

    resetBackup: async (req, res) => {
        res.send(await service.resetBackup(req.query.queriesMapFilter, req.query.cacheFilter))
    },

    assets: async (req, res) => {
        try {
            res.send(fs.readFileSync("examples/Eurostat/" + req.params.name, "utf-8"))
        }
        catch (error) {
            res.status(500).send(error || error.message)
        }
    },

    query: async (req, res) => {
        logger.info("Query: \n", req.query, "\n", "Body : \n", req.body)
        if (req.body.mongoQuery)
            return await queryMongo(req, res)
        await querySQL(req, res)
    },

    getValues: async (req, res) => {
        logger.info("values")
        try {
            res.send(await service.getValues(req.body.prefix, req.body.bucketName, req.headers.visibility, req.query.value, suggestionsPage(req.query), parseCollections(req.query.collections)))
        }
        catch (error) {
            logger.error(error)
            res.status(error.status || 500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    getEntries: async (req, res) => {
        logger.info("entries")
        let email = req.body.prefix.split("/")[0]
        if (config.updateOwner == "later" && !process.queryEngine.updatedOwners[email] && config.minioConfig.ownerInfoEndpoint) {
            try {
                await service.updateOwner(req.headers.authorization, email)
            }
            catch (error) {
                logger.error(error)
            }
        }
        try {
            res.send(await service.getEntries(req.body.prefix, req.body.bucketName, req.headers.visibility, req.query.key, req.query.value,
                suggestionsPage(req.query), { exactKey: isTrue(req.query.exactKey), exactValue: isTrue(req.query.exactValue), collections: parseCollections(req.query.collections) }))
        }
        catch (error) {
            logger.error(error)
            res.status(error.status || 500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    // { keys: [...] }: keys whose values are not suggested (the frontend warns the user)
    getKeysWithValuesNotIndexed: async (req, res) => {
        try {
            res.send(await service.getKeysWithValuesNotIndexed(req.body.prefix, req.body.bucketName, req.headers.visibility, parseCollections(req.query.collections)))
        }
        catch (error) {
            logger.error(error)
            res.status(error.status || 500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    getKeys: async (req, res) => {
        logger.info("keys")
        try {
            res.send(await service.getKeys(req.body.prefix, req.body.bucketName, req.headers.visibility, req.query.key, suggestionsPage(req.query), parseCollections(req.query.collections)))
        }
        catch (error) {
            logger.error(error)
            res.status(error.status || 500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    minioListObjects: async (req, res) => {
        try {
            res.send(await service.minioListObjects(req.params.bucketName || req.query.bucketName))
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    manageCollections: async (req, res) => {
        res.send(await service.manageCollections(manageCollectionsView));
    },
    deleteCollection: async (req, res) => {
        const { collectionName } = req.body;

        const protectedNames = ['system.indexes', 'users', 'roles', 'datapoints', 'dimensions', 'entities', 'entries', 'keys', 'sources', 'values', 'minio', ...CONNECTORS.map(c => collectionSettings(c).mongo)]
        if (!collectionName || protectedNames.includes(collectionName)) {
            return res.status(400).json({ message: 'Collection non valida' });
        }

        try {
            await service.deleteCollection(collectionName)
            return res.json({ message: `Collection '${collectionName}' cancellata con successo.` });
        } catch (err) {
            if (err.codeName === 'NamespaceNotFound') {
                return res.json({ message: `Collection '${collectionName}' non esiste.` });
            }
            logger.error(err);
            return res.status(500).json({ message: 'Errore durante la cancellazione' });
        }
    }
}