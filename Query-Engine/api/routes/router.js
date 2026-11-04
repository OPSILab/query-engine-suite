const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
const { auth, adminOnly, adminView, userOnly, managementSecret } = require("../middlewares/auth.js")
const statusLog = require("../services/statusLog.js")
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
// cache management: the reset with one of authConfig.adminRoles if configured; list / backup / choosing the active
// version any logged user; deleting backups also the management password (managementSecret)
router.post(encodeURI("/minio/resetCache"), auth, adminOnly, controller.resetCache)
router.get(encodeURI("/assets/:name"), controller.assets)
router.get(encodeURI("/backupCache"), auth, userOnly, controller.backupCache)
router.get(encodeURI("/restoreCache"), auth, userOnly, controller.restoreCache)
router.post(encodeURI("/resetBackup"), auth, userOnly, managementSecret, controller.resetBackup)
router.get(encodeURI("/listCache"), auth, userOnly, controller.listCache)
// failed HTTP requests of the Source-Connector: readable by everyone, header values only for admins (adminView)
router.get(encodeURI("/statuslog"), adminView, statusLog.endpoint)
// the urls whose failures the list leaves out unless filtered by url: read by everyone, changed by admins
router.get(encodeURI("/statuslog/ignored"), adminView, statusLog.ignoredEndpoint)
router.post(encodeURI("/statuslog/ignored"), auth, adminOnly, statusLog.ignoreEndpoint)
router.delete(encodeURI("/statuslog/ignored"), auth, adminOnly, statusLog.restoreEndpoint)
//router.get('/manage-collections', controller.manageCollections);
//router.delete('/delete-collection', controller.deleteCollection);

module.exports = router
