import { describe, expect, it } from 'vitest';
import { FALLBACK_GQL_EXAMPLES, SOURCES_DISCOVERY_QUERY, SURVEYS_DISCOVERY_QUERY, buildGqlExamples } from './graphql-examples';
import { analyze } from './graphql-analyzer';

const labels = (examples: { label: string }[]) => examples.map(e => e.label);
const queryOf = (examples: { label: string; query: string }[], label: string) => examples.find(e => e.label.startsWith(label))!.query;

describe('buildGqlExamples', () => {
  it('nothing could be read: the hardcoded examples', () => {
    expect(buildGqlExamples({ sources: null, surveys: null })).toEqual(FALLBACK_GQL_EXAMPLES);
  });

  it('API record: filter on one of its fields, name and source examples from the real values', () => {
    const examples = buildGqlExamples({
      sources: {
        sample: { name: 'Bike lanes Rome', doc: { _id: 'x', name: 'Bike lanes Rome', source: 'https://api/x', sourceId: 3, city: 'Rome', km: 12 } },
        api: { name: 'Bike lanes Rome', source: 'https://api.example.org/lanes?city=rome' },
      },
      surveys: ['NAMA_10R_3GDP'],
    });
    expect(labels(examples)).toEqual([
      'Available sources', 'Sources with their data',
      'Filter: city = Rome', 'Name contains "Bike"', 'Records of Bike lanes Rome',
      'Datapoints — NAMA_10R_3GDP',
    ]);
    expect(queryOf(examples, 'Filter')).toContain('filter: """{"city":"Rome"}"""');
    expect(queryOf(examples, 'Filter')).toContain('doc(fields: ["city"])');
    expect(queryOf(examples, 'Records of')).toContain('sources(source: "https://api.example.org/lanes?city=rome", limit: 10)');
    expect(queryOf(examples, 'Datapoints')).toContain('datapoints(survey: "NAMA_10R_3GDP", limit: 5)');
  });

  it('MinIO file: filter on a field of its first row (json / csv)', () => {
    const json = buildGqlExamples({ sources: { sample: { name: 'a@b.it/data model mapper/f.json', doc: { name: 'x', json: [{ id: 'skip-not', 'bad key': 1, year: 2021 }] } } }, surveys: [] });
    expect(queryOf(json, 'Filter')).toContain('"""{"json.id":"skip-not"}"""');
    expect(queryOf(json, 'Filter')).toContain('doc(fields: ["json"])');
    const csv = buildGqlExamples({ sources: { sample: { doc: { csv: [{ 'a"b': 'x', comune: 'Trento' }] } } }, surveys: [] });
    expect(queryOf(csv, 'Filter')).toContain('"""{"csv.comune":"Trento"}"""');
  });

  it('values that would break the query or say nothing are skipped', () => {
    const examples = buildGqlExamples({
      sources: { sample: { name: 'ab', doc: { record: { bucketName: 'x' }, quoted: 'say "hi"', long: 'x'.repeat(80), nested: { a: 1 }, ok: true } } },
      surveys: [],
    });
    expect(labels(examples)).toEqual(['Available sources', 'Sources with their data', 'Filter: ok = true']);
  });

  it('no data / no datapoints: only the examples valid on any data', () => {
    expect(labels(buildGqlExamples({ sources: {}, surveys: [] }))).toEqual(['Available sources', 'Sources with their data']);
  });

  it('surveys unreadable but sources readable: hardcoded datapoints examples only', () => {
    const examples = buildGqlExamples({ sources: {}, surveys: null });
    expect(labels(examples).slice(2)).toEqual(labels(FALLBACK_GQL_EXAMPLES).slice(-3));
  });

  it('every example and the discovery queries are well-formed (no mutation, nothing the tokenizer rejects)', () => {
    const all = [
      ...FALLBACK_GQL_EXAMPLES,
      ...buildGqlExamples({ sources: { sample: { name: 'Città di Roma', doc: { comune: 'Città', n: 1 } }, api: { source: 'https://x/y' } }, surveys: ['A', 'B', 'C'] }),
    ].map(e => e.query).concat(SOURCES_DISCOVERY_QUERY, SURVEYS_DISCOVERY_QUERY);
    for (const query of all) {
      const analysis = analyze(query, null);
      expect(analysis.blocked).toBeNull();
      expect(analysis.tokens.filter(t => t.kind === 'other')).toEqual([]);
      expect(query.split('{').length).toBe(query.split('}').length);
    }
  });
});
