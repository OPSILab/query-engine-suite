const Source = require("../models/Source");
const Datapoint = require("../models/Datapoint");
const Dimensions = require("../models/Dimensions");
const QueryCache = require("../models/QueryCache")
const QueriesMap = require("../models/QueriesMap")
const util = require("util");
const { translateDataPointsBatch } = require("../services/translationService");
const logger = require("percocologger")

function buildCachePrefix(args) {
  const { source, survey, dimensions, region, sortBy, sortOrder, limit, exclude, filterBy, filter, lang } = args
  return ("Cached" + (source || survey || dimensions.toString() || region || sortBy || sortOrder || limit || exclude || filterBy || filter || lang) + " : ")
}

const resolvers = {
  Query: {
    sources: async () => {
      return await Source.find();
    },
    source: async (parent, { id }) => {
      return await Source.findById(id);
    },

    datapoints: async (_, args, { db }) => {
      if (args.survey)
        args.survey = args.survey.toUpperCase()
      const { source, survey, dimensions, region, sortBy, sortOrder = 'ASC', limit, exclude, filterBy, filter, lang } = args
      let queryIn = { query: JSON.stringify({ _, args, db }) }
      logger.info(queryIn)
      let queried = await QueriesMap.find(queryIn)
      if (Array.isArray(queried) && queried[0] || queried?.query) {
        const CachedQuery = QueryCache(buildCachePrefix(args) + (Array.isArray(queried) ? queried[0]._id : queried._id))
        const cacheFound = await CachedQuery.find().lean()
        return cacheFound
      }
      try {
        const matchStage = {};

        if (source) {
          matchStage.source = source;
        }

        if (survey) {
          matchStage.survey = survey;
        }

        if (region) {
          matchStage.region = region;
        }

        if (dimensions && exclude) {
          const overlap = dimensions.filter(dim => exclude.includes(dim));
          if (overlap.length > 0) {
            throw new Error(`Invalid query: dimensions and exclude arrays have overlapping values: [${overlap.join(', ')}]`);
          }
        }

        if (dimensions && dimensions.length > 0 && exclude && exclude.length > 0) {
          matchStage.dimensions = {
            $all: dimensions,
            $nin: exclude
          };
        } else if (dimensions && dimensions.length > 0) {
          matchStage.dimensions = {
            $all: dimensions
          };
        } else if (exclude && exclude.length > 0) {
          matchStage.dimensions = {
            $nin: exclude
          };
        }

        const pipeline = [{ $match: matchStage }];

        if (typeof filterBy === 'number' && filter && Array.isArray(filter) && filter.length > 0) {
          pipeline.push({
            $match: {
              $expr: {
                $in: [
                  { $arrayElemAt: ["$dimensions", filterBy] },
                  filter
                ]
              }
            }
          });
        }

        // Ensure sortBy and sortOrder are arrays
        const sortFields = Array.isArray(sortBy) ? sortBy : (sortBy ? [sortBy] : []);
        const sortOrders = Array.isArray(sortOrder) ? sortOrder : [sortOrder];

        if (sortFields.length > 0) {
          const sortStage = {};
          let addFieldsStage = null;

          sortFields.forEach((field, index) => {
            const order = (sortOrders[index] || 'ASC').toUpperCase() === 'DESC' ? -1 : 1;
            if (field === 'year') {
              if (!addFieldsStage) {
                addFieldsStage = {
                  $addFields: {
                    yearNumeric: { $toInt: { $arrayElemAt: ["$dimensions", -1] } }
                  }
                };
                pipeline.push(addFieldsStage);
              }
              sortStage['yearNumeric'] = order;
            } else {
              sortStage[field] = order;
            }
          });

          pipeline.push({ $sort: sortStage });
        }

        if (limit) {
          pipeline.push({ $limit: limit });
        }

        const datapoints = await Datapoint.aggregate(pipeline);

        // Convert timestamp to datetime format
        let savingDP = datapoints.map(datapoint => {
          if (datapoint.timestamp) {
            datapoint.timestamp = new Date(datapoint.timestamp).toISOString();
          }
          return datapoint;
        });
        if (lang && lang !== "en")
          savingDP = await translateDataPointsBatch(savingDP, lang);
        let queryMap = (await QueriesMap.insertMany([queryIn]))[0]._id.toString()
        let collName = buildCachePrefix(args) + queryMap
        const CachedQuery = QueryCache(collName)
        await CachedQuery.insertMany(savingDP)
        await QueriesMap.findByIdAndUpdate(queryMap, { coll: collName })
        return savingDP
      } catch (error) {
        console.error(error);
        throw new Error('Error fetching datapoints');
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
