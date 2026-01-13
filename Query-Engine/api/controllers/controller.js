const service = require("../services/service.js")
const logger = require('percocologger')
const config = require('../../config')

const queryMongo = async (req, res) => {
    logger.info(req.body, req.query)
    if (req.headers.israwquery)
        return await res.send(await service.rawQuery(req.query, req.body.prefix, req.body.bucketName, req.headers.visibility)) && logger.info("Raw query finished")
    logger.info("Query mongo")
    logger.debug("format ", req.query.format)
    if (req.query.format == "JSON") {
        let objectQuerySet = JSON.parse(JSON.stringify(req.body.mongoQuery || req.query))
        objectQuerySet.format = "Object"
        let JSONQuerySet = JSON.parse(JSON.stringify(req.body.mongoQuery || req.query))
        JSONQuerySet.format = "JSON"
        let objectQuery = await service.mongoQuery(objectQuerySet, req.body.prefix, req.body.bucketName, req.headers.visibility)
        if (objectQuery && !Array.isArray(objectQuery))
            objectQuery = [objectQuery]
        let JSONQuery = await service.mongoQuery(JSONQuerySet, req.body.prefix, req.body.bucketName, req.headers.visibility)
        if (JSONQuery && !Array.isArray(JSONQuery))
            JSONQuery = [JSONQuery]
        if (JSONQuery && objectQuery)
            res.send(JSONQuery.concat(objectQuery))
        else
            res.send(JSONQuery || objectQuery)
    }
    else
        res.send(await service.mongoQuery({ ...req.body.mongoQuery, ...req.query }, req.body.prefix, req.body.bucketName, req.headers.visibility))
    logger.info("Query mongo finished")

}

const querySQL = async (req, res) => {
    logger.info("Query sql")
    if (!req.body.query)
        return await res.status(400).send("Missing query")
    logger.info("Query : ", req.body.query)
    service.querySQL(res, req.body.query, req.body.prefix, req.body.bucketName, req.headers.visibility)
}

module.exports = {

    queryMongo,

    querySQL,

    query: async (req, res) => {
        logger.info("Query: \n", req.query, "\n", "Body : \n", req.body)
        if (req.body.mongoQuery)
            return await queryMongo(req, res)
        querySQL(req, res)
    },

    getValues: async (req, res) => {
        logger.info("values")
        try {
            res.send(await service.getValues(req.body.prefix, req.body.bucketName, req.headers.visibility, req.query.value))
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    getEntries: async (req, res) => {
        logger.info("entries")
        let email = req.body.prefix.split("/")[0]
        if (config.updateOwner == "later" && !process.queryEngine.updatedOwners[email]) {
            service.updateOwner(req.headers.authorization, email)
        }
        try {
            res.send(await service.getEntries(req.body.prefix, req.body.bucketName, req.headers.visibility, req.query.key, req.query.value))
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    getKeys: async (req, res) => {
        logger.info("keys")
        try {
            res.send(await service.getKeys(req.body.prefix, req.body.bucketName, req.headers.visibility, req.query.key))
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
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
    }
}