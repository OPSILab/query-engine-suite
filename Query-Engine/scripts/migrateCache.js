// One-off import of the old datapoints cache (one collection per query, plus the _backup collections) into the
// versioned cache in one collection (see utils/cacheMigration.js, api/services/queryCache.js).
//
//   node scripts/migrateCache.js --dry-run     what would be imported (old collections, datapoints, orphans)
//   node scripts/migrateCache.js               imports; the old collections and rows are left as they are
//   node scripts/migrateCache.js --drop-old    imports, then drops the imported old collections and their rows
//
// It can run with the Query-Engine up. Re-runnable: what is already imported is skipped.

process.percocologger = require("../percocologger.config")
const common = require("../utils/common")
const config = common.checkConfig(require("../config"), require("../config.template"))
const mongoose = require("mongoose")
const logger = require("percocologger")
const { migrateCache } = require("../utils/cacheMigration")

async function main() {
    const args = process.argv.slice(2)
    const unknown = args.filter(a => !["--dry-run", "--drop-old"].includes(a))
    if (unknown.length)
        throw new Error("Unknown arguments: " + unknown.join(" ") + " (--dry-run | --drop-old)")
    await mongoose.connect(config.mongo)
    try {
        const stats = await migrateCache({ dryRun: args.includes("--dry-run"), dropOld: args.includes("--drop-old") })
        console.log(JSON.stringify(stats, null, 2))
    }
    finally {
        await mongoose.disconnect()
    }
}

main()
    .then(() => process.exit(0))
    .catch(error => {
        logger.error(error)
        console.error(error.message || error)
        process.exit(1)
    })
