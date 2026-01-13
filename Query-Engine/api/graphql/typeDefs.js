const { gql } = require('apollo-server-express')

const typeDefs = gql`
  

  type Data {
    datapoints: [DataPoint!]!
  }

  type Source {
    id: ID!
    name: String!
    data: Data
  }

#  type DataPoint {
#    id: ID!
#    region: String!
#    source: String!
#    timestamp: String!
#    survey: String!
#    dimensions: [String!]!
#    value: Float!
#  }

#  type DataPointV2 {
#    source: String
#    survey: String
#    surveyName: String
#    region: String
#    fromUrl: String
#    timestamp: String
#    dimensions: [String]
#    value: Float
#  }

  type DataPoint {
    _id: String
    source: String
    survey: String
    surveyName: String
    surveyData: String
    region: String
    dimensions: [String]
    aggregationPeriod: String
    value: Float
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

#    datapoints(
#      survey: String!
#      sortBy: [String!]
#      sortOrder: String!
#      dimensions: [String!]!
#      limit: Int
#    ): [DataPoint!]!

#    datapointsV2(
#      source: String
#      survey: String
#      dimensions: [String!]
#      region: String
#      sortBy: [String!]
#      sortOrder: [String!]
#      limit: Int
#      exclude: [String!]
#      filterBy: Int
#      filter: [String!]
#    ): [DataPointV2]

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
