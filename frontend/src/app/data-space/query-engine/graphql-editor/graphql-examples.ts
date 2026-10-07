/**
 * Example queries of the GraphQL editor, built from the backend's own data: a document the user can see
 * (for the filter / name examples), an API record (for the source example) and the surveys that have
 * datapoints. The hardcoded examples below are only the fallback for what can't be read (backend unreachable,
 * an older Query-Engine without `surveys`, ...).
 */
export interface GqlExample {
  label: string;
  query: string;
}

// Valid whatever the data: no fallback needed.
const GENERIC: GqlExample[] = [
  // sources: filter (MongoDB filter as JSON, a """block string""" needs no escaping), name, source,
  // collections (api / orion / minio, default api + minio), limit (default 100, per collection), skip; doc returns
  // the stored document - see the Query-Engine's typeDefs.js.
  { label: 'Available sources', query: `query {
  sourcesCount
  sources(limit: 20) {
    id
    collection
    name
    source
  }
}` },
  { label: 'API data only (Sources)', query: `query {
  sourcesCount(collections: ["api"])
  sources(collections: ["api"], limit: 20) {
    name
    source
    doc
  }
}` },
  { label: 'Sources with their data', query: `query {
  sources(limit: 5) {
    name
    doc
  }
}` },
  // "Find all": no filter, every field. Without limit the backend applies queryOptions.graphQLDefaultLimit (100),
  // at most graphQLMaxLimit (1000) per query: sourcesCount says how many there are, skip reads the next pages.
  { label: 'All sources, all fields (find all)', query: `query {
  sourcesCount
  sources {
    id
    name
    source
    doc
  }
}` },
];

const FALLBACK_SOURCES: GqlExample[] = [
  { label: 'Public files, filtered', query: `query {
  sources(
    filter: """{"record.bucketName": "public-data"}"""
    limit: 10
  ) {
    name
    doc(fields: ["json", "csv"])
  }
}` },
  // doc without fields: the whole stored document, whatever its fields (sources have no fixed schema)
  { label: 'Public files, all fields', query: `query {
  sources(
    filter: """{"record.bucketName": "public-data"}"""
    limit: 5
  ) {
    name
    doc
  }
}` },
  { label: 'API records by name', query: `query {
  sources(name: "rome", limit: 10) {
    name
    source
    doc
  }
}` },
];

// Taken from Query-Engine/examples/GraphQL (ExampleQuery1, 2 and 5). Every datapoints example passes
// `survey`: see the note in the backend's buildCachePrefix() for why a datapoints query with neither
// `source` nor `survey` (nor `dimensions`) currently fails there.
const FALLBACK_DATAPOINTS: GqlExample[] = [
  { label: 'Online purchase — Severozapaden', query: `query {
  datapoints(
    survey: "ISOC_R_BLT12_I"
    sortBy: ["year"]
    sortOrder: "desc"
    dimensions: [
      "Severozapaden"
      "Last online purchase: in the last 3 months"
      "Percentage of individuals"
    ]
    limit: 1
  ) {
    region
    source
    timestamp
    survey
    dimensions
    value
  }
}` },
  { label: 'PIL per inhabitant — Lovech', query: `query {
  datapoints(
    survey: "nama_10r_3gdp"
    sortBy: "year"
    sortOrder: "asc"
    dimensions: ["Lovech", "Euro per inhabitant"]
  ) {
    region
    survey
    dimensions
    value
  }
}` },
  { label: 'Population change — Trento', query: `query {
  datapoints(
    survey: "demo_r_gind3"
    sortBy: "year"
    sortOrder: "asc"
    dimensions: ["Trento", "Total population change"]
  ) {
    region
    survey
    dimensions
    value
  }
}` },
];

/** Shown before the data has been read, and when none of it can be. */
export const FALLBACK_GQL_EXAMPLES: GqlExample[] = [...GENERIC, ...FALLBACK_SOURCES, ...FALLBACK_DATAPOINTS];

/** A document the user can see, and an API record (polled by the Source-Connector: http(s) source, no MinIO record). */
export const SOURCES_DISCOVERY_QUERY = `query DataSpaceExampleSources {
  sample: sources(limit: 1) { name source doc }
  api: sources(filter: """{"source": {"$regex": "^https?://"}, "record": {"$exists": false}}""", limit: 1) { name source }
}`;

export const SURVEYS_DISCOVERY_QUERY = `query DataSpaceExampleSurveys { surveys(limit: 2) }`;

export interface GqlExampleData {
  /** null: couldn't be read -> fallback examples. */
  sources: { sample?: any; api?: any } | null;
  /** null: couldn't be read -> fallback examples; []: no datapoints -> no datapoints example. */
  surveys: string[] | null;
}

// GraphQL string literal (same escapes as JSON for what can appear here)
const gqlString = (text: string) => JSON.stringify(text);
const short = (value: unknown) => {
  const text = typeof value === 'string' ? value : JSON.stringify(value);
  return text.length > 30 ? text.slice(0, 29) + '…' : text;
};

// Fields that say nothing about the data (added by the connectors, or whole files)
const TECHNICAL = new Set(['_id', 'id', 'name', 'source', 'sourceId', 'source_original', 'sourceId_original', 'record', 'raw', 'json', 'csv', 'datePolled']);

/** A plain scalar field of `obj` usable in a filter: [path, value]. */
function scalarField(obj: any, prefix = ''): [string, string | number | boolean] | null {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return null;
  for (const [key, value] of Object.entries(obj)) {
    if ((!prefix && TECHNICAL.has(key)) || !/^[A-Za-z_][\w-]*$/.test(key)) continue;
    if (typeof value === 'number' || typeof value === 'boolean') return [prefix + key, value];
    if (typeof value === 'string' && value && value.length <= 60 && !/["\\\r\n]/.test(value)) return [prefix + key, value];
  }
  return null;
}

/** The filter on a real field: with doc(fields) on that field only, and with the whole documents (doc). */
function filterExamples(doc: any): GqlExample[] {
  // a top-level field (API / Orion records, JSON files stored as they are), else a field of the first row of a
  // JSON array / CSV file (MinIO files are stored as { json: [...] } / { csv: [...] })
  const found = scalarField(doc)
    ?? (Array.isArray(doc?.json) ? scalarField(doc.json[0], 'json.') : null)
    ?? (Array.isArray(doc?.csv) ? scalarField(doc.csv[0], 'csv.') : null);
  if (!found) return [];
  const [path, value] = found;
  const filter = `"""${JSON.stringify({ [path]: value })}"""`;
  return [
    {
      label: `Filter: ${path} = ${short(value)}`,
      query: `query {
  sources(
    filter: ${filter}
    limit: 10
  ) {
    name
    doc(fields: [${gqlString(path.split('.')[0])}])
  }
}`,
    },
    {
      // doc without fields: every field of the document, even without knowing them
      label: `Filter: ${path} = ${short(value)}, all fields`,
      query: `query {
  sources(
    filter: ${filter}
    limit: 5
  ) {
    name
    doc
  }
}`,
    },
  ];
}

function nameExample(name: any): GqlExample | null {
  if (typeof name !== 'string') return null;
  // a word of the name (the argument is a case-insensitive "contains")
  const word = name.split(/[^\p{L}\p{N}]+/u).find(w => w.length >= 3);
  if (!word) return null;
  return {
    label: `Name contains "${short(word)}"`,
    query: `query {
  sources(name: ${gqlString(word)}, limit: 10) {
    name
    source
    doc
  }
}`,
  };
}

function apiExample(api: any): GqlExample | null {
  if (typeof api?.source !== 'string') return null;
  return {
    label: `Records of ${short(typeof api.name === 'string' && api.name ? api.name : api.source)}`,
    query: `query {
  sources(source: ${gqlString(api.source)}, limit: 10) {
    name
    doc
  }
}`,
  };
}

function datapointsExample(survey: string): GqlExample {
  return {
    label: `Datapoints — ${short(survey)}`,
    query: `query {
  datapoints(survey: ${gqlString(survey)}, limit: 5) {
    survey
    region
    dimensions
    value
  }
}`,
  };
}

export function buildGqlExamples(data: GqlExampleData): GqlExample[] {
  const examples = [...GENERIC];
  if (data.sources) {
    for (const example of [...filterExamples(data.sources.sample?.doc), nameExample(data.sources.sample?.name), apiExample(data.sources.api)])
      if (example) examples.push(example);
  } else {
    examples.push(...FALLBACK_SOURCES);
  }
  if (data.surveys) {
    examples.push(...data.surveys.filter(s => typeof s === 'string' && s).slice(0, 2).map(datapointsExample));
  } else {
    examples.push(...FALLBACK_DATAPOINTS);
  }
  return examples;
}
