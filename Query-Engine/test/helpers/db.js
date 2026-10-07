// Real MongoDB 4.4 for the tests that query the `sources` collection (graphql.test.js).
//
// Start it with `npm run test:db` (docker-compose.test.yml, also used by the GitHub workflow) and stop it with
// `npm run test:db:down`. Another server: MONGO_TEST_URL.
// Every test file gets its own database (qes_qe_test_<file>), dropped at start and at the end.

const path = require("path")

const MONGO_URL = process.env.MONGO_TEST_URL || "mongodb://127.0.0.1:27019"

let mongoose

async function setup(testFile) {
    const dbName = "qes_qe_test_" + path.basename(testFile).replace(/\.test\.js$/, "").replace(/\W/g, "_").toLowerCase()
    mongoose = require("mongoose")
    try {
        await mongoose.connect(MONGO_URL, { dbName, serverSelectionTimeoutMS: 5000 })
    }
    catch (error) {
        throw new Error(
            `MongoDB for the tests is not reachable at ${MONGO_URL.replace(/\/\/[^@/]*@/, "//***@")} (${error.message}).\n` +
            `Start it with "npm run test:db" (Docker), or set MONGO_TEST_URL.`
        )
    }
    await mongoose.connection.dropDatabase()
}

async function teardown() {
    try {
        await mongoose?.connection?.dropDatabase()
    }
    catch { }
    await mongoose?.disconnect().catch(() => { })
}

module.exports = { setup, teardown, MONGO_URL }
