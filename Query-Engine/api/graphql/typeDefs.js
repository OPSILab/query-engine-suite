const { gql } = require('apollo-server-express')

const typeDefs = gql`
  

  # Source documents are schemaless (see api/models/Source.js, strict: false):
  # nothing written by the Source-Connector is guaranteed to have any given
  # field, so no field of Source (or of the Data it may contain) is non-null.
  # A non-null field that turns out null in one document makes GraphQL null
  # out that whole list element and add an error for it.
  type Data {
    datapoints: [DataPoint]
  }

  type Source {
    id: ID
    name: String
    # Set by the Source-Connector's apiConnector (the polled API's URL and the
    # item's original id); absent on documents coming from MinIO.
    source: String
    sourceId: String
    data: Data
    # The whole document as stored, whatever its structure - the way to reach
    # fields this schema doesn't (and, the collection being schemaless, can't)
    # declare, without touching the schema or restarting when new kinds of
    # documents get inserted. \`fields\` keeps only the given top-level keys;
    # omit it to get everything (careful: documents from MinIO carry the whole
    # uploaded file, e.g. under \`json\` or \`csv\`).
    doc(fields: [String]): JSON
  }

  scalar JSON

  type DataPoint {
    _id: String
    source: String
    survey: String
    surveyName: String
    surveyData: String
    region: String
    dimensions: [String]
    aggregationPeriod: String
    value: JSON
    exclude: [String]
    timestamp: String
    smartKeys: [String]
    references: [String]
    fromUrl: String
    meta: Meta
    updateFrequency: String
  }

  type Meta {
    quality: String
  }

  type Query {
    sources: [Source]
    source(id: ID!): Source
    datapoints(
      survey: String
      source: String
      region: String
      geo: String
      sex: String
      age: String
      year: String
      unit: String
      frequency: String
      dimensions: [String]
      exclude: [String]
      filterBy: Int
      filter: [String]
      sortBy: [String]
      sortOrder: [String]
      timestamp: String
      value: Float
      limit: Int
      lang: String
    ): [DataPoint!]!
  }

  type Dimension {
    geo: String
    sex: String
    unit: String
    age: String
    year: String
    frequency: String
  }

  type Mutation {
    createSource(name: String!): Source #, record: RecordInput): Source
    updateSource(id: ID!, name: String): Source
    deleteSource(id: ID!): Boolean
  }
`

module.exports = typeDefs
