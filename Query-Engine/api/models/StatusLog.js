const mongoose = require("mongoose");

// Failed HTTP requests of the Source-Connector (written by its utils/requestLog.js), read by GET /api/statuslog.
// Collection "statuslog" (named here to make it explicit).
const statusLog = new mongoose.Schema({}, { strict: false, versionKey: false });

module.exports = mongoose.model("statuslog", statusLog, "statuslog");
