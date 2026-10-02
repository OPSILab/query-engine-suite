const mongoose = require("mongoose");

const queriesMap = new mongoose.Schema({}, { strict: false, versionKey: false });   

module.exports = mongoose.model("queriesmap", queriesMap);