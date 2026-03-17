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
