const Source = require("../models/Source");
const Datapoint = require("../models/Datapoint");
const Dimensions = require("../models/Dimensions");
const QueryCache = require("../models/QueryCache")
const QueriesMap = require("../models/QueriesMap")
const util = require("util");
const { translateDataPointsBatch } = require("../services/translationService");
const logger = require("percocologger")

const resolvers = {
  Query: {
    sources: async () => {
      return await Source.find();
    },
    source: async (parent, { id }) => {
      return await Source.findById(id);
    },

    datapoints0: async (_parent, args, { db }) => {

      let queryIn = { query: JSON.stringify({ _parent, args, db }) }
      let queried = await QueriesMap.find(queryIn)
      if (Array.isArray(queried) && queried[0] || queried?.query) {
        const CachedQuery = QueryCache(args.survey + " : " + (Array.isArray(queried) ? queried[0]._id : queried._id))
        const cacheFound = await CachedQuery.find().lean()
        return cacheFound
      }
      // Estrai tutti gli argomenti di "controllo" che hanno una logica speciale.
      const {
        sortBy = [],
        sortOrder = "ASC",
        dimensions = [],
        exclude = [],
        filterBy,
        filter = [],
        limit,
        lang,
        ...otherFilters
      } = args;

      const query = { ...otherFilters };

      if (query.survey) {
        query.survey = query.survey.toUpperCase().replace(/\./g, "");
      }

      const fieldsToNormalize = ["source", "surveyName", "region"];
      fieldsToNormalize.forEach((field) => {
        if (query[field] && typeof query[field] === "string") {
          query[field] = query[field].toUpperCase();
        }
      });

      let dimensionKeysCache = null;

      const getDimensionKeys = async () => {
        if (dimensionKeysCache) return dimensionKeysCache;

        logger.info("First find")
        const sampleDatapoints = await Dimensions.find({ survey: query.survey })
          .select("dimensions")
          .lean()
          .exec();
        logger.info("Found")

        dimensionKeysCache = [
          ...new Set(
            sampleDatapoints.flatMap((doc) => {
              // Supporta sia array che oggetto singolo
              if (Array.isArray(doc.dimensions)) {
                return doc.dimensions.flatMap((d) => Object.keys(d));
              } else if (doc.dimensions && typeof doc.dimensions === "object") {
                return Object.keys(doc.dimensions);
              }
              return [];
            })
          ),
        ];
        return dimensionKeysCache;
      };

      // Costruzione query MongoDB
      const andClauses = [];

      const dimensionKeys = await getDimensionKeys();

      // Filtro inclusione dimensioni
      if (dimensions.length > 0 && dimensionKeys.length > 0) {
        dimensions.forEach((value) => {
          // Costruisci condizioni per entrambi i formati
          const arrayCondition = {
            dimensions: {
              $elemMatch: {
                $or: dimensionKeys.map((k) => ({ [k]: value })),
              },
            },
          };

          const objectCondition = {
            $or: dimensionKeys.map((k) => ({ [`dimensions.${k}`]: value })),
          };

          andClauses.push({
            $or: [arrayCondition, objectCondition],
          });
        });
      }

      // Filtro per dimensione specifica (filterBy index)
      if (typeof filterBy === "number" && filter.length > 0) {
        logger.info("Find one")
        const sampleDoc = await Datapoint.findOne({ survey: query.survey })
          .select("dimensions")
          .lean()
          .exec();
        logger.info("Found one")

        let dimensionKey = null;

        // Gestisci sia array che oggetto
        if (Array.isArray(sampleDoc?.dimensions)) {
          const dimensionObj = sampleDoc.dimensions[filterBy];
          if (dimensionObj) {
            dimensionKey = Object.keys(dimensionObj)[0];
          }
        } else if (
          sampleDoc?.dimensions &&
          typeof sampleDoc.dimensions === "object"
        ) {
          // Per oggetto singolo, usa filterBy come indice delle chiavi
          const keys = Object.keys(sampleDoc.dimensions);
          dimensionKey = keys[filterBy];
        }

        if (dimensionKey) {
          const filterValues = filter.map((v) => {
            const num = Number(v);
            return isNaN(num) ? v : num;
          });

          // Supporta entrambi i formati
          andClauses.push({
            $or: [
              {
                dimensions: {
                  $elemMatch: {
                    [dimensionKey]: { $in: filterValues },
                  },
                },
              },
              {
                [`dimensions.${dimensionKey}`]: { $in: filterValues },
              },
            ],
          });
        }
      }

      if (andClauses.length > 0) query.$and = andClauses;

      // Pipeline di aggregazione
      const pipeline = [
        { $match: query },

        {
          $lookup: {
            from: "sources",
            localField: "source",
            foreignField: "id",
            as: "sourceData",
          },
        },
        {
          $unwind: {
            path: "$sourceData",
            preserveNullAndEmptyArrays: true,
          },
        },
      ];

      // --- LOGICA EXCLUDE ---
      if (exclude.length > 0) {
        pipeline.push(
          {
            $addFields: {
              _tempValuesToCheck: {
                $map: {
                  // mergeObjects unifica sia se 'dimensions' è un array di oggetti, sia se è un oggetto singolo
                  input: { $objectToArray: { $mergeObjects: "$dimensions" } },
                  as: "dim",
                  in: "$$dim.v",
                },
              },
            },
          },
          {
            $match: {
              _tempValuesToCheck: {
                $nin: exclude, // $nin esclude il documento se UNO QUALSIASI dei valori combacia
              },
            },
          },
          {
            $unset: "_tempValuesToCheck",
          }
        );
      }

      // Ordinamento
      if (sortBy.length > 0) {
        const sortByArray = Array.isArray(sortBy) ? sortBy : [sortBy];
        const sortOrderArray = Array.isArray(sortOrder)
          ? sortOrder
          : [sortOrder];

        const addFieldsStage = {};
        const sortStage = {};

        sortByArray.forEach((field, i) => {
          const order = sortOrderArray[i]?.toUpperCase() === "DESC" ? -1 : 1;
          console.log(sortOrderArray[i]);

          if (dimensionKeys.includes(field)) {
            // Gestisci ordinamento per entrambi i formati
            addFieldsStage[`sort_${field}`] = {
              $cond: {
                if: { $isArray: "$dimensions" },
                then: {
                  $first: {
                    $map: {
                      input: {
                        $filter: {
                          input: "$dimensions",
                          as: "dim",
                          cond: {
                            $gt: [{ $type: `$$dim.${field}` }, "missing"],
                          },
                        },
                      },
                      as: "dim",
                      in: `$$dim.${field}`,
                    },
                  },
                },
                else: `$dimensions.${field}`,
              },
            };
            sortStage[`sort_${field}`] = order;
          } else {
            // Campo diretto (value, timestamp, ecc.)
            sortStage[field] = order;
          }
        });

        if (Object.keys(addFieldsStage).length > 0) {
          pipeline.push({ $addFields: addFieldsStage });
        }
        pipeline.push({ $sort: sortStage });
      }

      if (limit && Number.isInteger(limit) && limit > 0) {
        pipeline.push({ $limit: limit });
      }

      // Proiezione finale con dati arricchiti
      pipeline.push({
        $project: {
          _id: 1,
          source: 1,
          survey: 1,
          surveyName: 1,
          surveyData: 1,
          region: 1,
          dimensions: {
            $cond: {
              if: { $isArray: "$dimensions" },
              then: {
                $map: {
                  input: { $objectToArray: { $mergeObjects: "$dimensions" } },
                  as: "dim",
                  in: "$$dim.v",
                },
              },
              else: {
                $map: {
                  input: { $objectToArray: "$dimensions" },
                  as: "dim",
                  in: "$$dim.v",
                },
              },
            },
          },
          aggregationPeriod: 1,
          value: 1,
          timestamp: 1,
          smartKeys: 1,
          references: 1,
          fromUrl: 1,
          meta: 1,
          updateFrequency: 1,
        },
      });

      console.log("MongoDB Query Dinamica:", JSON.stringify(pipeline, null, 2));

      console.log("Esecuzione query...");
      const datapoints = await Datapoint.aggregate(pipeline, { allowDiskUse: true }).exec();
      console.log(`Trovati datapoints.`);

      let queryMap = (await QueriesMap.insertMany([queryIn]))[0]._id.toString()
      const CachedQuery = QueryCache(args.survey + " : " + queryMap)
      if (lang && lang !== "en") {
        let translatedDatapoints = await translateDataPointsBatch(datapoints, lang);
        await CachedQuery.insertMany(translatedDatapoints)
        return translatedDatapoints
      }

      logger.info(datapoints.length)
      await CachedQuery.insertMany(datapoints)
      logger.info("inserted")
      return datapoints;
    },
    datapoints: async (_parent, { survey, dimensions, exclude, filterBy, filter, sortBy, sortOrder = ['ASC'], limit }, { db }) => {
      let queryIn = { query: JSON.stringify({ _parent, args: { survey, dimensions, exclude, filterBy, filter, sortBy, sortOrder, limit }, db }) }
      let queried = await QueriesMap.find(queryIn)
      if (Array.isArray(queried) && queried[0] || queried?.query) {
        const CachedQuery = QueryCache(survey + " : " + (Array.isArray(queried) ? queried[0]._id : queried._id))
        const cacheFound = await CachedQuery.find().lean()
        return cacheFound
      }
      try {
        const pipeline = [];

        // --- Match survey case-insensitive ---
        if (survey) {
          pipeline.push({
            $match: {
              survey: { $regex: `^${survey}$`, $options: 'i' }
            }
          });
        }

        // --- Filtri su dimensions ---
        const exprAnd = [];

        if (dimensions && dimensions.length > 0) {
          dimensions.forEach(val => {
            exprAnd.push({
              $in: [
                val,
                { $map: { input: { $objectToArray: "$dimensions" }, as: "d", in: "$$d.v" } }
              ]
            });
          });
        }

        if (exclude && exclude.length > 0) {
          exclude.forEach(val => {
            exprAnd.push({
              $not: {
                $in: [
                  val,
                  { $map: { input: { $objectToArray: "$dimensions" }, as: "d", in: "$$d.v" } }
                ]
              }
            });
          });
        }

        if (filterBy !== undefined && filter && Array.isArray(filter) && filter.length > 0) {
          // filterBy come indice della chiave nell'oggetto dimensions
          exprAnd.push({
            $in: [
              { $arrayElemAt: [{ $map: { input: { $objectToArray: "$dimensions" }, as: "d", in: "$$d.v" } }, filterBy] },
              filter
            ]
          });
        }

        if (exprAnd.length > 0) {
          pipeline.push({ $match: { $expr: { $and: exprAnd } } });
        }

        // --- Ordinamento ---
        if (sortBy && sortBy.length > 0) {
          const sortStage = {};
          sortBy.forEach((field, idx) => {
            const order = (sortOrder[idx] || 'ASC').toUpperCase() === 'DESC' ? -1 : 1;
            if (field === 'year') {
              // aggiungi campo numerico per year
              pipeline.push({
                $addFields: { yearNumeric: { $toInt: "$dimensions.year" } }
              });
              sortStage['yearNumeric'] = order;
            } else {
              sortStage[`dimensions.${field}`] = order;
            }
          });
          pipeline.push({ $sort: sortStage });
        }

        // --- Limite ---
        if (limit) pipeline.push({ $limit: limit });

        // --- Aggregazione finale ---
        const datapoints = await Datapoint.aggregate(pipeline);

        // --- Trasforma dimensions in array di valori e timestamp ISO ---
        let queryMap = (await QueriesMap.insertMany([queryIn]))[0]._id.toString()
        const CachedQuery = QueryCache(survey + " : " + queryMap)
        logger.info(datapoints.length)
        let savingDP = datapoints.map(dp => ({
          ...dp,
          dimensions: Object.values(dp.dimensions || []),
          timestamp: dp.timestamp ? new Date(dp.timestamp).toISOString() : dp.timestamp,
        }))
        await CachedQuery.insertMany(savingDP)
        return savingDP
      } catch (err) {
        console.error("Error querying datapoints2:", err);
        throw err;
      }
    }
  },

  Mutation: {
    createSource: async (parent, { json, record, name }) => {
      const newSource = new Source({ json, record, name });
      return await newSource.save();
    },

    updateSource: async (parent, { id, json, record, name }) => {
      return await Source.findByIdAndUpdate(
        id,
        { json, record, name },
        { new: true }
      );
    },

    deleteSource: async (parent, { id }) => {
      try {
        await Source.findByIdAndDelete(id);
        return true;
      } catch (err) {
        return false;
      }
    },
  },
};

module.exports = resolvers;
