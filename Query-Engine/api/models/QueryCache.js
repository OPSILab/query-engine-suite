const mongoose = require("mongoose");

const cache = new mongoose.Schema({}, { strict: false, versionKey: false });   

module.exports = (query) => mongoose.model(query, cache);