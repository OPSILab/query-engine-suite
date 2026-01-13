const Minio = require('minio')
const common = require('../utils/common.js')
const { sleep } = common
const config = require('../config.js')
const { minioConfig, delays } = config
const minioClient = new Minio.Client(minioConfig)
const logger = require('percocologger')
const log = logger.info
process.queryEngine = { updatedOwners: {} }

async function listObjects(bucketName) {

  let resultMessage
  let errorMessage

  let data = []
  let stream = minioClient.listObjects(bucketName, '', true, { IncludeVersion: true })
  stream.on('data', function (obj) {
    data.push(obj)
  })
  stream.on('end', function (obj) {
    if (!obj)
      log("ListObjects ended returning an empty object")
    else
      log("Found object ")
    if (data[0])
      resultMessage = data
    else if (!resultMessage)
      resultMessage = []
  })
  stream.on('error', function (err) {
    log(err)
    errorMessage = err
  })

  let logCounterFlag
  while (!errorMessage && !resultMessage) {
    await sleep(delays)
    if (!logCounterFlag) {
      logCounterFlag = true
      sleep(delays + 2000).then(resolve => {
        if (!errorMessage && !resultMessage)
          log("waiting for list")
        logCounterFlag = false
      })
    }
  }
  if (errorMessage)
    throw errorMessage
  if (resultMessage)
    return resultMessage
}

async function getObject(bucketName, objectName, format) {

  logger.trace("Now getting object " + objectName + " in bucket " + bucketName)

  let resultMessage
  let errorMessage

  minioClient.getObject(bucketName, objectName, function (err, dataStream) {
    if (err) {
      errorMessage = err
      log(err)
      return err
    }

    let objectData = '';
    dataStream.on('data', function (chunk) {
      objectData += chunk;
    });

    dataStream.on('end', function () {
      try {
        resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData) : objectData

      }
      catch (error) {
        try {
          if (config.parseCompatibilityMode === 1)
            resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData.substring(1)) : objectData
          else
            resultMessage = (format == 'json' && typeof objectData == "string") ? JSON.parse(objectData.substring(objectData.indexOf("{"))) : objectData
        }
        catch (error) {
          resultMessage = format == 'json' ? [{ data: objectData }] : objectData
        }
      }
      if (!resultMessage)
        resultMessage = "Empty file"
    });

    dataStream.on('error', function (err) {
      log('Error reading object:')
      errorMessage = err
      log(err)
    });

  });

  let logCounterFlag
  while (!errorMessage && !resultMessage) {
    await sleep(delays)
    if (!logCounterFlag) {
      logCounterFlag = true
      sleep(delays + 2000).then(resolve => {
        if (!errorMessage && !resultMessage)
          log("waiting for object " + objectName + " in bucket " + bucketName)
        logCounterFlag = false
      })
    }
  }
  if (errorMessage)
    throw errorMessage
  if (resultMessage)
    return resultMessage
}

module.exports = {

  listObjects,
  getObject
}