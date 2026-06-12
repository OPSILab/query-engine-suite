const mongoose = require('mongoose');

const datapointSchema = new mongoose.Schema({
  source: String,
  survey: String,
  surveyName: String,
  region: String,
  fromUrl: String,
  timestamp: String,
  
  value: Number
}, {strict: false});

module.exports = mongoose.model('datapointsmock', datapointSchema);