module.exports = {
  minioConfig: {
    endPoint: 'play.min.io',
    port: 9000,
    useSSL: true,
    accessKey: 'Q3AM3UQ867SPQQA43P2F',
    secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG',
    location: "us-east-1",
    defaultFileInput: "../../input/inputFile.json",
    defaultOutputFolderName: "private generic data",
    defaultInputFolderName: "data model mapper",
    defaultBucket: "default",
    subscribe: {
      all: true,
      buckets: []
    },
    ownerInfoEndpoint: "https://platform.beopendep.it/api/owner"
  },
  postgreConfig: {
    user: '',
    host: 'localhost',
    database: '',
    password: '',
    port: 5432
  },
  postgreReaderConfig: {
    user: '',
    host: 'localhost',
    database: '',
    password: '',
    port: 5432, // Porta di default per PostgreSQL
  },
  mapEndpoint: "http://localhost:5500/api/map/transform",
  parseEndpoint: "http://localhost:5500/api/parse",
  getMapEndpoint: "http://localhost:5500/api/map",
  sessionEndpoint: "http://localhost:5500/api/output?",
  mapID: "",
  orion: {
    protocol: "http",
    subscribe: true,
    deleteAllDuplicateSubscriptions: true,
    attrWithUrl: "datasetUrl",
    orionBaseUrl: "http://localhost:1026",
    hostname: "localhost",
    port: 1026,
    ngsiBrokerUrl: "https://dx-lab.it/",
    notificationUrl: "http://localhost:3000/api/orion/subscribe",
    fiwareService: "",
    fiwareServicePath: "",
    checkSubscriptionInterval: 0,
    recreateSubscriptionAtInterval: 0,
    useNgsiBroker: false
  },
  logLevel: "info",
  syncInterval: 86400000,
  doNotSyncAtStart: false,
  upsertRecords: true,
  delays: 1,
  queryAllowedExtensions: ["csv", "json", "geojson"],
  parseCompatibilityMode: 0,
  port: 3000,
  updateOwner: "later",
  writeLogsOnFile: true,
  mongo: "mongodb://localhost:22000/Minio-Mongo", // mongo url
  authConfig: {
    idmHost: "https://platform.beopendep.it/auth",
    clientId: "",
    username: "",
    password: "",
    userInfoEndpoint: "https://platform.beopendep.it/api/user",
    disableAuth: false,
    authProfile: "oidc",
    authRealm: "",
    introspect: false,
    publicKey: "",
    // Cache reset / backup / restore endpoints: roles required besides a valid token (Keycloak realm roles or roles of
    // clientId). [] = any authenticated user. Not checked with disableAuth.
    adminRoles: [],
    // With disableAuth the cache endpoints are closed, unless this token is set and sent in the X-Admin-Token header
    adminToken: "", // don't push it
    secret: "" // don't push it
  },
  sourceConnectors: {
    minioConnector: true,
    orionConnector: true,
    apiConnector: true
  },
  queryOptions: {
    simpleSearch: true,
    advancedSearch: true,
    SQLQuery: true,
    graphQLQuery: true,
    graphQLDefaultLimit: 100, // GraphQL sources: documents returned when the query has no limit
    graphQLMaxLimit: 1000,    // GraphQL sources: highest accepted limit
    advancedSearchMaxResults: 1000, // Advanced search: highest page size, and results without page
    suggestionsMaxResults: 500,     // keys / values / entries suggestions: highest page size, and results without page
    // Collections searched by the requests without `collections` (clients older than the collections): Advanced
    // search, keys / values / entries suggestions, GraphQL sources. Default: what the old `sources` collection held,
    // no datapoints. Ids: api, orion, minio. The Simple search does not read these collections.
    defaultCollections: ["api", "minio"],
    // Advanced search, suggestions and GraphQL: MongoDB stops a query after this time (ms) and the request gets 504.
    // Generous: some legitimate queries are slow. 0: no limit.
    mongoMaxTimeMS: 900000
  },
  // Cache of the GraphQL datapoints queries: one collection (the datapoints of every version of every query) and the
  // queriesmap collection (one row per version). keepVersions: versions kept per query, the active one included;
  // the versions saved by backupCache are kept until resetBackup.
  cache: {
    collection: "querycache",
    keepVersions: 3,
    // highest number of cached datapoints (all versions of all queries); beyond it the results are not cached. 0: no limit
    maxDatapoints: 5000000
  },
  // The MongoDB collections of the Source-Connector, one per connector: same values as in its config (only mongo /
  // toMongo / toPostgres are used here). The frontend lets the user choose which ones to search, and warns that the
  // SQL queries don't find the ones with toPostgres false (GET /api/collections).
  collections: {
    api: { mongo: "sources", toMongo: true, postgres: "sources", toPostgres: true },
    orion: { mongo: "datapoints", toMongo: true, postgres: "datapoints", toPostgres: false },
    minio: { mongo: "minio", toMongo: true, toPostgres: true }
  },
  // Simple search: besides the MinIO files, the APIs of apiConnectorConfig.apiUrls (same format as in the
  // Source-Connector config; an API with simpleSearch: false is skipped) and Orion are searched live.
  // What is left out is reported to the frontend, which shows it as a warning.
  simpleSearchOptions: {
    minio: true,
    api: true,
    orion: false,          // every Orion entity points to a whole dataset, downloaded (and mapped, with a mapID) at every search
    maxPages: 20,          // pages read from a paginated API
    maxOrionEntities: 20,  // Orion entities downloaded per search
    maxResults: 1000,      // results of the API / Orion sources
    timeout: 30000         // ms, per request
  },
  apiConnectorConfig: {
    upsertRecords: false,
    pollInterval: 1000 * 60 * 60 * 24/*,
    apiUrls: [
      {
        name: "Example API",
        url: "https://example.com/api/data",
        headers: {
          "Authorization": {
            type: "bearerToken",
            authProfile: "basic",
            credentials: {
              username: "username",
              password: "password"
            },
            authUrl: {
              value: "https://example.com/api/token",
              requestType: "POST"
            }
          },
          "Authorization": {
            type: "bearerToken",
            authProfile: "OAuth 2.0 Client Credentials Grant",
            credentials: {
              client_id: "username",
              client_secret: "password"
            },
            authUrl: {
              value: "https://example.com/api/token",
              requestType: "POST"
            }
          }
        }
      },            
      {
                name: "Orion pagination test",
                pagination: {
                    offsetParam: "offset",
                    limitParam: "limit",
                    limit: 2,
                    offset: 0,
                    condition: (response) => (response.data.length > 0)
                },
                url: "http://localhost:1026/ngsi-ld/v1/entities?type=DistributionDCAT-AP",
      },
      {
                name: "Get batch example",
                batch: {
                    from: "http://url-to-get-batch.com/api/batch",
                    param: "id"
                },
                url: "https://url-to-use-batch/{batch}"
            }
    ]*/
  }
}