const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
const { auth, adminOnly } = require("../middlewares/auth.js")
const { bodyCheck } = require('../../utils/common.js')
const mongoose = require('mongoose');

router.post(encodeURI("/query"), auth, bodyCheck, controller.query)//, controller.queryMongo)
router.get(encodeURI("/query"), auth, controller.queryMongo)
router.get(encodeURI("/query/simple/limits"), auth, controller.simpleSearchLimits)
router.get(encodeURI("/collections"), auth, controller.getCollections)
router.get(encodeURI("/keys"), auth, controller.getKeys)
router.get(encodeURI("/keys/notIndexed"), auth, controller.getKeysWithValuesNotIndexed)
router.get(encodeURI("/values"), auth, controller.getValues)
router.get(encodeURI("/entries"), auth, controller.getEntries)
router.get(encodeURI("/minio/listObjects"), auth, controller.minioListObjects)
// cache management: authenticated, and with one of authConfig.adminRoles if configured
router.post(encodeURI("/minio/resetCache"), auth, adminOnly, controller.resetCache)
router.get(encodeURI("/assets/:name"), controller.assets)
router.get(encodeURI("/backupCache"), auth, adminOnly, controller.backupCache)
router.get(encodeURI("/restoreCache"), auth, adminOnly, controller.restoreCache)
router.post(encodeURI("/resetBackup"), auth, adminOnly, controller.resetBackup)
//router.get('/manage-collections', controller.manageCollections);
//router.delete('/delete-collection', controller.deleteCollection);

module.exports = router
