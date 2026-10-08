// One collection per connector (services/collections.js). Datapoints are in the Orion collection ("datapoints"),
// with the Orion records that are not datapoints: DATAPOINT_FILTER keeps the datapoints only.
const { collectionModel, storedCollections } = require("../services/collections")
const DATAPOINT_FILTER = { survey: { $exists: true }, dimensions: { $exists: true } }
const Dimensions = require("../models/Dimensions");
const { readCache, writeCache } = require("../services/queryCache")
const util = require("util");
const { translateDataPointsBatch } = require("../services/translationService");
const logger = require("percocologger")
const { visibleIn } = require("../services/visibility")
const { sourcesQuery, sourcesCollections, limits } = require("./sourcesQuery")

// Same scoping as the REST queries: everything with disableAuth, otherwise only the documents the user may see
// for the visibility header (prefix and bucket are set on req.body by the auth middleware, see index.js).
function visibleTo(req, doc, connector) {
  return visibleIn(connector, plain(doc), req?.body?.prefix, req?.body?.bucketName, req?.headers?.visibility)
}

// The collection a document was read from, for Source.collection
const collectionOf = new WeakMap()
function tagged(doc, connector) {
  if (doc && typeof doc === "object")
    collectionOf.set(doc, connector)
  return doc
}

// Plain-object view of a Source document, computed once per document and
// per request (a WeakMap entry dies with the document). Source documents are
// schemaless (strict: false), so fields are read from this rather than from
// the Mongoose document's own properties.
const plainCache = new WeakMap()
function plain(doc) {
  if (!doc || typeof doc !== "object") return doc
  if (typeof doc.toObject !== "function") return doc
  if (!plainCache.has(doc)) plainCache.set(doc, doc.toObject())
  return plainCache.get(doc)
}

// Typed String fields on a schemaless document: return the value only when
// it is actually a scalar. A document may well carry e.g. `source` as a
// nested object (MinIO documents spread the uploaded JSON's own top-level
// keys); serializing that as String would fail and add an error for that
// document - better null here, with the real value still reachable via `doc`.
function scalarOrNull(value) {
  if (value === null || value === undefined || typeof value === "object") return null
  return String(value)
}

const resolvers = {
  Query: {
    // filter / name / source / limit / skip: see sourcesQuery.js. The visibility is part of the MongoDB query
    // (limit and skip apply to what the user may see) and every document is checked again with objectFilter.
    // collections: see sourcesQuery.js (default api + minio); limit / skip apply to each collection
    sources: async (parent, args, { req }) => {
      const { limit, skip } = limits(args)
      const lists = await Promise.all(sourcesCollections(args).map(async c =>
        (await collectionModel(c).find(sourcesQuery(args, req, c)).sort({ _id: 1 }).skip(skip).limit(limit))
          .filter(doc => visibleTo(req, doc, c)).map(doc => tagged(doc, c))))
      return lists.flat();
    },
    sourcesCount: async (parent, args, { req }) => {
      const counts = await Promise.all(sourcesCollections(args).map(c => collectionModel(c).countDocuments(sourcesQuery(args, req, c))))
      return counts.reduce((a, b) => a + b, 0);
    },
    // One document per survey in `dimensions` (written by the Source-Connector with the datapoints): cheap,
    // unlike a distinct on the datapoints collection.
    surveys: async (parent, args) => {
      const { limit } = limits(args)
      return (await Dimensions.find({ survey: { $type: "string" } }, { survey: 1, _id: 0 }).sort({ survey: 1 }).limit(limit).lean()).map(d => d.survey);
    },
    // the document with this id, in whichever collection
    source: async (parent, { id }, { req }) => {
      for (const c of storedCollections()) {
        let doc
        try {
          doc = await collectionModel(c).findById(id);
        }
        catch {
          return null // not a valid id
        }
        if (doc)
          return visibleTo(req, doc, c) ? tagged(doc, c) : null;
      }
      return null;
    },

    datapoints: async (_, args, { db }) => {
      if (args.survey)
        args.survey = args.survey.toUpperCase()
      const { source, survey, dimensions, region, sortBy, sortOrder = 'ASC', limit, exclude, filterBy, filter, lang } = args
      // cache key: the stringified query, as before (services/queryCache.js: one collection, versioned)
      const query = JSON.stringify({ _, args, db })
      logger.info({ query })
      const cacheFound = await readCache(query)
      if (cacheFound) {
        logger.info("Cache found")
        return cacheFound
      }
      try {
        const matchStage = { ...DATAPOINT_FILTER };

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

        logger.info("Pipeline built")
        logger.info(util.inspect(pipeline, { depth: null }))
        const datapoints = await collectionModel("orion").aggregate(pipeline);
        logger.info("Datapoints fetched: ", datapoints.length)

        // Convert timestamp to datetime format
        let savingDP = datapoints.map(datapoint => {
          if (datapoint.timestamp) {
            datapoint.timestamp = new Date(datapoint.timestamp).toISOString();
          }
          return datapoint;
        });
        if (lang && lang !== "en")
          savingDP = await translateDataPointsBatch(savingDP, lang);
        // a cache that cannot be written does not fail the query
        try {
          await writeCache(query, savingDP, { survey, source, lang })
        } catch (error) {
          logger.error("Datapoints cache not written", error)
        }
        return savingDP
      } catch (error) {
        console.error(error);
        throw new Error('Error fetching datapoints');
      }
    }
  },

  Source: {
    collection: (s) => collectionOf.get(s) ?? null,
    name: (s) => scalarOrNull(plain(s)?.name),
    source: (s) => scalarOrNull(plain(s)?.source),
    sourceId: (s) => scalarOrNull(plain(s)?.sourceId),
    doc: (s, { fields }) => {
      const d = plain(s)
      if (!d || !Array.isArray(fields) || fields.length === 0) return d
      return Object.fromEntries(fields.filter(f => Object.prototype.hasOwnProperty.call(d, f)).map(f => [f, d[f]]))
    },
  },
};

module.exports = resolvers;
