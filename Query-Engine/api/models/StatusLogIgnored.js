const mongoose = require("mongoose");

// The urls whose requests GET /api/statuslog leaves out unless a url filter is given (services/statusLog.js):
// { url, createdAt, by } an exact url, { url, match: "prefix", createdAt, by } the urls that begin with it.
// Collection "statuslogignored".
const statusLogIgnored = new mongoose.Schema({}, { strict: false, versionKey: false });

module.exports = mongoose.model("statuslogignored", statusLogIgnored, "statuslogignored");
