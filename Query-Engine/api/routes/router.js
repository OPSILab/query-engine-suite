const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
const { auth } = require("../middlewares/auth.js")
const { bodyCheck } = require('../../utils/common.js')
const mongoose = require('mongoose');

router.post(encodeURI("/query"), auth, bodyCheck, controller.query)//, controller.queryMongo)
router.get(encodeURI("/query"), auth, controller.queryMongo)
router.get(encodeURI("/keys"), auth, controller.getKeys)
router.get(encodeURI("/values"), auth, controller.getValues)
router.get(encodeURI("/entries"), auth, controller.getEntries)
router.get(encodeURI("/minio/listObjects"), auth, controller.minioListObjects)
router.post(encodeURI("/minio/resetCache"), controller.resetCache)
//router.get('/manage-collections', controller.manageCollections);
//router.delete('/delete-collection', controller.deleteCollection);

module.exports = router
