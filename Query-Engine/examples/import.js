
const mongoose = require("mongoose");
const fs = require("fs");
const common = require("../utils/common")
const config = common.checkConfig(require('../config'), require('../config.template'))


const MONGO_URI = config.mongo;
const COLLECTION_NAME = "Datapoint";
const FILE_PATH = "./examples/mock_eurostat_dataset.json";


async function connectDB() {
  try {
    await mongoose.connect(MONGO_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });
    console.log("✅ Connesso a MongoDB");
  } catch (err) {
    console.error("❌ Errore di connessione:", err);
    process.exit(1);
  }
}

// SCHEMA MONGOOSE (flessibile)
const dataSchema = new mongoose.Schema({}, { strict: false });
const EurostatModel = mongoose.model(COLLECTION_NAME, dataSchema);


async function importData() {
  try {
    const rawData = fs.readFileSync(FILE_PATH, "utf-8");
    const jsonData = JSON.parse(rawData);

    console.log(`📊 Trovati ${jsonData.length} record nel file.`);

    await EurostatModel.deleteMany({});
    console.log("🧹 Collezione svuotata.");

    await EurostatModel.insertMany(jsonData);
    console.log("✅ Dati importati con successo!");

    mongoose.connection.close();
  } catch (err) {
    console.error("❌ Errore durante l'importazione:", err);
    mongoose.connection.close();
  }
}

(async () => {
  await connectDB();
  await importData();
})();
