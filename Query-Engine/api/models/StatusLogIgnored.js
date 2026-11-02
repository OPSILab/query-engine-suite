const mongoose = require("mongoose");

// The urls whose failed requests GET /api/statuslog leaves out unless a url filter is given (services/statusLog.js):
// { url, createdAt, by }. Collection "statuslogignored".
const statusLogIgnored = new mongoose.Schema({}, { strict: false, versionKey: false });

module.exports = mongoose.model("statuslogignored", statusLogIgnored, "statuslogignored");
