import { beforeEach, describe, expect, it, vi } from 'vitest';
import { of, throwError, map } from 'rxjs';
import { HttpHeaders, HttpResponse } from '@angular/common/http';

// Only NbAuthService's getToken() is used (snippet with the real token): no need to load Nebular
vi.mock('@nebular/auth', () => ({ NbAuthService: class { } }));

import { QueryEngineComponent } from './query-engine.component';
import { BeopenAPIService, QueryWarning } from '../../services/be-open.service';

const TOO_MANY = ['Too many suggestions. Type some characters in order to reduce them'];

// the "Search in" choice is remembered in localStorage: every test starts from scratch
beforeEach(() => localStorage.clear());

interface Backend {
  keys?: any;                                   // GET /api/keys
  values?: any;                                 // GET /api/values
  notIndexed?: any;                             // GET /api/keys/notIndexed
  collections?: any;                            // GET /api/collections
  entries?: (key: string) => any;               // GET /api/entries?key=...
  query?: (format: string | null, body: any) => any; // POST /api/query (Advanced search), by format param and body
  fail?: RegExp;                                // urls that answer with an error
  limits?: QueryWarning[];                      // GET /api/query/simple/limits
  warningsHeader?: string;                      // X-Query-Warnings of the query responses
  graphql?: (query: string) => any;             // POST /graphql, by document
}

// Real BeopenAPIService on a fake HttpClient: the component's requests go through the real request builders.
function fakeHttp(backend: Backend) {
  const requests: any[] = [];
  const answer = (url: string, value: () => any) => backend.fail?.test(url) ? throwError(() => new Error('HTTP 500')) : of(value());
  return {
    requests,
    get: (url: string, options?: any) => answer(url, () => {
      requests.push({ method: 'GET', url, options });
      if (url.includes('/api/collections')) return backend.collections ?? [];
      if (url.includes('/api/keys/notIndexed')) return backend.notIndexed ?? { keys: [] };
      if (url.includes('/api/keys')) return backend.keys ?? [];
      if (url.includes('/api/values')) return backend.values ?? [];
      if (url.includes('/api/entries')) return backend.entries?.(options.params.get('key')) ?? [];
      if (url.includes('/api/user')) return { email: 'anna@demetrix.it' };
      if (url.includes('/api/query/simple/limits')) return { warnings: backend.limits ?? [] };
      return [];
    }),
    request: (method: string, url: string, options: any) => {
      requests.push({ method, url, options });
      const reply = url.endsWith('/graphql')
        ? () => { const r = backend.graphql?.(options.body.query); if (r instanceof Error) throw r; return r ?? { data: null }; }
        : () => backend.query?.(options.params.get('format'), options.body) ?? [];
      return answer(url, reply).pipe(map(body =>
        new HttpResponse({ body, headers: new HttpHeaders(backend.warningsHeader ? { 'X-Query-Warnings': backend.warningsHeader } : {}) })));
    },
  };
}

function create({ backend = {} as Backend, settings = {}, token = null as string | null, translation = {} as any } = {}) {
  const allSettings: any = { beopenApiBaseUrl: 'http://qe', ...settings };
  const config = {
    getSettings: (key?: string, defaultValue?: any) => !key ? allSettings : (allSettings[key] ?? defaultValue),
  };
  const http = fakeHttp(backend);
  const api = new BeopenAPIService(http as any, config as any);
  const nbAuth = { getToken: () => of({ getPayload: () => ({ access_token: token }) }) };
  const shared = { userRoles$: of([]) };
  const toast = { show: vi.fn() };
  const comp = new QueryEngineComponent(
    {} as any, api, translation, toast as any, {} as any, shared as any, { BucketObjectsPush: () => { } } as any, nbAuth as any, config as any,
    { nativeElement: document.createElement('div') } as any,
  );
  comp.visibility = 'public';
  return { comp, http, api, toast };
}

describe('startup', () => {
  const suggestionRequests = (requests: any[]) => requests.filter(r => /\/api\/(keys|values|entries)(\?|$)/.test(r.url))
    .map(r => [r.url.replace('http://qe/api/', ''), r.options.params.get('skip'), r.options.params.get('limit')]);

  it('preloads the first page of keys and of values, for the autocompletes', async () => {
    const { comp, http } = create({ backend: { keys: { items: [{ key: 'city' }], hasMore: true }, values: { items: [{ value: 'Rome' }], hasMore: false } } });
    await comp.ngOnInit();
    expect(await comp.preloadedKeys).toEqual({ items: ['city'], hasMore: true });
    expect(await comp.preloadedValues).toEqual({ items: ['Rome'], hasMore: false });
    expect(suggestionRequests(http.requests)).toEqual([['keys', '0', '100'], ['values', '0', '100']]);
  });

  it('the demo uses the preloaded keys', async () => {
    const { comp, http } = create({ backend: { keys: [{ key: 'city' }], entries: key => [{ key, value: 'Rome' }], query: () => [{}] } });
    await comp.ngOnInit();
    await comp.demo();
    expect(comp.lines[0]).toEqual({ key: 'city', value: 'Rome', type: 'String' });
    expect(suggestionRequests(http.requests).filter(([url]) => url == 'keys').length).toBe(1);
  });

  it('a failed preload is not an error (the autocompletes read the page when needed)', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => { });
    const { comp } = create({ backend: { fail: /api\/(keys|values)/ } });
    await comp.ngOnInit();
    await expect(comp.preloadedKeys).rejects.toThrow();
    await new Promise(resolve => setTimeout(resolve)); // no unhandled rejection
  });
});

describe('demo', () => {
  it('fills the form with a real key/value pair and the format that finds it', async () => {
    const { comp } = create({
      backend: {
        keys: [{ key: 'city' }],
        entries: key => [{ key, value: 'Rome' }, { key: key + 'Code', value: 'RM' }],
        query: format => format == 'CSV' ? [{ name: 'f.csv' }] : [],
      },
    });
    await comp.demo();
    expect(comp.lines).toEqual([{ key: 'city', value: 'Rome', type: 'String' }]);
    expect(comp.type).toBe('CSV');
    expect(comp.demoLoading).toBe(false);
  });

  it('prefers data keys over the technical ones added by the Source-Connector', async () => {
    const tried: string[] = [];
    const { comp } = create({
      backend: {
        keys: [{ key: 'source' }, { key: 'sourceId' }, { key: 'color' }],
        entries: key => { tried.push(key); return [{ key, value: key == 'color' ? 'red' : 'x' }]; },
        query: () => [{}],
      },
    });
    await comp.demo();
    expect(tried[0]).toBe('color');
    expect(comp.lines[0]).toEqual({ key: 'color', value: 'red', type: 'String' });
  });

  it('skips keys without values, and long/JSON values when short ones exist', async () => {
    const { comp } = create({
      backend: {
        keys: [{ key: 'empty' }, { key: 'k' }],
        entries: key => key == 'empty' ? [] : [{ key, value: { nested: true } }, { key, value: 'x'.repeat(100) }, { key, value: 'short' }],
        query: () => [{}],
      },
    });
    await comp.demo();
    expect(comp.lines[0].value).toBe('short');
  });

  it('falls back to the hardcoded example when the backend has too many keys or fails', async () => {
    for (const backend of [{ keys: TOO_MANY }, { fail: /api\/keys/ }, { keys: [] }]) {
      const { comp } = create({ backend });
      await comp.demo();
      expect(comp.lines).toEqual([{ key: 'a', value: 'a1', type: 'String' }]);
      expect(comp.type).toBe('JSON');
    }
  });
});

describe('"Generate curl / fetch / axios"', () => {
  it('Advanced search: the request "Apply Query" would send, without auth when it is disabled', () => {
    const { comp } = create();
    comp.lines = [{ key: 'city', value: 'Rome', type: 'String' }];
    comp.type = 'JSON';
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain(`curl -X POST 'http://qe/api/query?format=JSON&city=Rome'`);
    expect(comp.snippetCode).toContain('"city": "Rome"');
    expect(comp.snippetCode).toContain('"limit": 50');   // the first page, like "Apply Query"
    expect(comp.snippetCode).not.toContain('Authorization');
  });

  it('"Find all": no filters', () => {
    const { comp } = create();
    comp.lines = [{ key: 'city', value: 'Rome', type: 'String' }];
    comp.snippetFindAll = true;
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain('"mongoQuery": {}');
  });

  it('auth enabled: <TOKEN> placeholder by default, the real token only on request', () => {
    const { comp } = create({ settings: { enableAuthentication: true }, token: 'eyJ.real.token' });
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain(`Authorization: Bearer <TOKEN>`);
    expect(comp.snippetTokenNote).toMatch(/Replace <TOKEN>/);
    comp.snippetIncludeToken = true;
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain('Authorization: Bearer eyJ.real.token');
    expect(comp.snippetTokenNote).toMatch(/do not share/);
  });

  it('auth enabled but no token stored: placeholder', () => {
    const { comp } = create({ settings: { enableAuthentication: true }, token: null });
    comp.snippetIncludeToken = true;
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain('Bearer <TOKEN>');
  });

  it('other languages and modes', () => {
    const { comp } = create();
    comp.setMode('Simple search');
    comp.value = 'Rome';
    comp.setSnippetLang('fetch');
    expect(comp.snippetCode).toContain('await fetch("http://qe/api/query?value=Rome"');
    expect(comp.snippetFileName).toBe('query-engine-request.mjs');

    comp.setMode('Query GraphQL');
    comp.graphqlText = '{ sources { id } }';
    comp.setSnippetLang('axios');
    expect(comp.snippetCode).toContain('url: "http://qe/graphql"');
    expect(comp.snippetCode).toContain('"query": "{ sources { id } }"');

    comp.setMode('Query SQL');
    comp.sqlQuery = 'SELECT * FROM publicdata';
    comp.setSnippetLang('curl');
    expect(comp.snippetCode).toContain('"query": "SELECT * FROM publicdata"');
    expect(comp.snippetFileName).toBe('query-engine-request.sh');
  });
});

describe('Simple search warnings', () => {
  const limits: QueryWarning[] = [
    { kind: 'config', code: 'MINIO_DISABLED', message: 'MinIO files are not searched' },
    { kind: 'config', code: 'ORION_DISABLED', message: 'Orion sources are not searched' },
    { kind: 'config', code: 'API_EXCLUDED', source: 'Huge', message: 'API "Huge" is not searched' },
  ];
  const flush = () => new Promise(resolve => setTimeout(resolve));

  it('loads the configuration limits at startup', async () => {
    const { comp } = create({ backend: { limits } });
    await comp.ngOnInit();
    await flush();
    expect(comp.simpleSearchLimits).toEqual(limits);
  });

  it('API / Orion limits only matter for public data (or with authentication disabled)', async () => {
    const { comp } = create({ backend: { limits }, settings: { enableAuthentication: true } });
    await comp.ngOnInit();
    await flush();
    comp.visibility = 'private';
    expect(comp.shownSimpleSearchLimits.map(w => w.code)).toEqual(['MINIO_DISABLED']);
    comp.visibility = 'public';
    expect(comp.shownSimpleSearchLimits.length).toBe(3);
    comp.authEnabled = false;
    comp.visibility = 'private';
    expect(comp.shownSimpleSearchLimits.length).toBe(3);
  });

  it('a Query-Engine without the limits endpoint: no limits shown', async () => {
    const { comp } = create({ backend: { fail: /simple\/limits/ } });
    await comp.ngOnInit();
    await flush();
    expect(comp.simpleSearchLimits).toEqual([]);
  });

  it('warningText: the translation with {{source}}, else the backend message', () => {
    const translation = { instant: (key: string, p: any) => key === 'Simple search warning API_EXCLUDED' ? `API ${p.source} esclusa` : key };
    const { comp } = create({ translation });
    expect(comp.warningText(limits[2])).toBe('API Huge esclusa');
    expect(comp.warningText(limits[1])).toBe('Orion sources are not searched');
    expect(create().comp.warningText(limits[2])).toBe('API "Huge" is not searched'); // no translation service
  });

  it('runtime warnings of a search become a toast; configuration ones do not', async () => {
    const warnings: QueryWarning[] = [
      { kind: 'config', code: 'ORION_DISABLED', message: 'Orion sources are not searched' },
      { kind: 'runtime', code: 'API_ERROR', source: 'Weather', message: 'API "Weather" could not be searched (HTTP 500)' },
    ];
    const { comp, toast } = create({ backend: { warningsHeader: encodeURIComponent(JSON.stringify(warnings)), query: () => [{ name: 'x' }] } });
    await comp.ngOnInit();
    comp.setMode('Simple search');
    comp.value = 'Rome';
    comp.minioQuery(false);
    await flush();
    expect(toast.show).toHaveBeenCalledTimes(1);
    const [status, , text] = toast.show.mock.calls[0];
    expect(status).toBe('warning');
    expect(text).toBe('API "Weather" could not be searched (HTTP 500)');
    comp.ngOnDestroy();
  });
});

describe('"Find all" in Simple search', () => {
  it('sends no search text, whatever is typed; "Apply Query" sends it', () => {
    const { comp, http } = create();
    comp.setMode('Simple search');
    comp.value = 'Rome';
    comp.minioQuery(true);
    expect(http.requests[0].options.params.get('value')).toBe('');
    comp.loading = null;
    comp.minioQuery(false);
    expect(http.requests[1].options.params.get('value')).toBe('Rome');
  });

  it('the snippet follows the "Find all" option', () => {
    const { comp } = create();
    comp.setMode('Simple search');
    comp.value = 'Rome';
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain('/api/query?value=Rome');
    comp.snippetFindAll = true;
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain(`/api/query?value='`);
  });
});

describe('GraphQL examples from the real data', () => {
  const backend = (overrides: Partial<Record<'sources' | 'surveys', any>> = {}) => ({
    graphql: (query: string) => query.includes('DataSpaceExampleSources')
      ? overrides.sources ?? { data: { sample: [{ name: 'Bike lanes', doc: { city: 'Rome' } }], api: [{ name: 'Lanes', source: 'https://api/lanes' }] } }
      : overrides.surveys ?? { data: { surveys: ['NAMA_10R_3GDP'] } },
  });

  it('built when the GraphQL mode is opened, with the current visibility', async () => {
    const { comp, http } = create({ backend: backend() });
    expect(comp.graphqlExamples).toBeUndefined();
    comp.setMode('Query GraphQL');
    await new Promise(resolve => setTimeout(resolve));
    expect(comp.graphqlExamples!.map(e => e.label)).toEqual([
      'Available sources', 'API data only (Sources)', 'Sources with their data', 'All sources, all fields (find all)', 'Filter: city = Rome', 'Filter: city = Rome, all fields', 'Name contains "Bike"', 'Records of Lanes', 'Datapoints — NAMA_10R_3GDP',
    ]);
    expect(http.requests.every(r => r.options.headers.get('visibility') === 'public')).toBe(true);
  });

  it('read once per visibility', async () => {
    const { comp, http } = create({ backend: backend() });
    await comp.loadGraphqlExamples();
    await comp.loadGraphqlExamples();
    expect(http.requests.length).toBe(2); // sources + surveys
    comp.visibility = 'private';
    await comp.loadGraphqlExamples();
    expect(http.requests.length).toBe(4);
  });

  it('an older backend without `surveys` (HTTP 400): hardcoded datapoints examples', async () => {
    const { comp } = create({ backend: { ...backend({ surveys: new Error('HTTP 400') }) } });
    await comp.loadGraphqlExamples();
    expect(comp.graphqlExamples!.map(e => e.label)).toContain('PIL per inhabitant — Lovech');
    expect(comp.graphqlExamples!.map(e => e.label)).toContain('Filter: city = Rome');
  });

  it('backend unreachable: all the hardcoded examples', async () => {
    const { comp } = create({ backend: { fail: /graphql/ } });
    await comp.loadGraphqlExamples();
    expect(comp.graphqlExamples).toEqual(comp.fallbackGqlExamples);
  });
});

describe('Advanced search results, one page at a time', () => {
  const flush = () => new Promise(resolve => setTimeout(resolve));
  // n results, served in pages ({ results, hasMore }), or all at once without a page (an older Query-Engine)
  const paged = (n: number, pages = true) => (_format: string | null, body: any) => {
    const all = Array.from({ length: n }, (_, i) => ({ name: 'r' + i, json: [{ i }] }));
    if (!pages || !body?.page) return all;
    const { limit, skip = 0 } = body.page;
    return { results: all.slice(skip, skip + limit), hasMore: n > skip + limit, skip, limit };
  };
  const queries = (requests: any[]) => requests.filter(r => r.method === 'POST' && r.url.endsWith('/api/query')).map(r => r.options.body);

  it('config.json advancedSearchPageSize: the size of every page, "Load more results" included', async () => {
    const { comp, http } = create({ backend: { query: paged(7) }, settings: { advancedSearchPageSize: 5 } });
    comp.lines = [{ key: 'city', value: 'Rome', type: 'String' }];
    comp.minioQuery(false);
    await flush();
    comp.loadMoreResults();
    await flush();
    expect(queries(http.requests).map(q => q.page)).toEqual([{ limit: 5, skip: 0 }, { limit: 5, skip: 5 }]);
    expect(comp.extractedElements.length).toBe(7);
    expect(comp.hasMoreResults).toBe(false);
  });

  it('"Apply Query" asks for the first page; "Load more results" appends the next ones, until there are no more', async () => {
    const { comp, http } = create({ backend: { query: paged(120) } });
    const more: boolean[] = [];
    const loading: boolean[] = [];
    let shown: any[] = [];
    comp.hasMoreResultsChange.subscribe(m => more.push(m));
    comp.loadingChange.subscribe(l => loading.push(l));
    comp.extractedElementsChange.subscribe(e => shown = e);
    comp.lines = [{ key: 'city', value: 'Rome', type: 'String' }];
    comp.minioQuery(false);
    await flush();
    expect(shown.length).toBe(50);
    expect(comp.hasMoreResults).toBe(true);

    comp.lines = [{ key: 'other', value: 'x', type: 'String' }]; // the next pages are of the query on screen
    comp.loadMoreResults();
    await flush();
    comp.loadMoreResults();
    await flush();
    expect(shown.map(e => e.name)).toEqual(Array.from({ length: 120 }, (_, i) => 'r' + i));
    expect(comp.hasMoreResults).toBe(false);
    comp.loadMoreResults(); // nothing more: no request
    expect(queries(http.requests)).toEqual([
      { mongoQuery: { city: 'Rome' }, page: { limit: 50, skip: 0 } },
      { mongoQuery: { city: 'Rome' }, page: { limit: 50, skip: 50 } },
      { mongoQuery: { city: 'Rome' }, page: { limit: 50, skip: 100 } },
    ]);
    expect(more).toEqual([true, false]);
    expect(loading).toEqual([true, false]); // "Load more results" does not fade the results on screen
  });

  it('a new query starts again from the first page', async () => {
    const { comp } = create({ backend: { query: paged(60) } });
    comp.minioQuery(true);
    await flush();
    comp.loadMoreResults();
    await flush();
    expect(comp.extractedElements.length).toBe(60);
    comp.minioQuery(true);
    await flush();
    expect(comp.extractedElements.length).toBe(50);
    expect(comp.hasMoreResults).toBe(true);
  });

  it('a Query-Engine without pages answers everything: no "Load more results"', async () => {
    const { comp } = create({ backend: { query: paged(70, false) } });
    comp.minioQuery(true);
    await flush();
    expect(comp.extractedElements.length).toBe(70);
    expect(comp.hasMoreResults).toBe(false);
  });

  it('Simple search and GraphQL have no pages', async () => {
    const { comp, http } = create({ backend: { query: paged(120), graphql: () => ({ data: { sources: [] } }) } });
    comp.minioQuery(true);
    await flush();
    expect(comp.hasMoreResults).toBe(true);
    comp.setMode('Simple search');
    comp.minioQuery(true);
    await flush();
    expect(http.requests.at(-1).options.body).toBeUndefined();
    expect(comp.hasMoreResults).toBe(false);
    comp.setMode('Advanced search');
    comp.minioQuery(true);
    await flush();
    comp.setMode('Query GraphQL');
    comp.graphqlText = '{ sources { id } }';
    comp.minioQuery(false);
    await flush();
    expect(comp.hasMoreResults).toBe(false);
  });
});

describe('keys whose values are not suggested', () => {
  it('read at startup: the rows with that key warn (exact key only)', async () => {
    const { comp } = create({ backend: { notIndexed: { keys: ['value'] } } });
    await comp.ngOnInit();
    await new Promise(resolve => setTimeout(resolve));
    expect(comp.valuesNotSuggested('value')).toBe(true);
    expect(comp.valuesNotSuggested('Value')).toBe(false);
    expect(comp.valuesNotSuggested('region')).toBe(false);
    expect(comp.valuesNotSuggested(undefined)).toBe(false);
  });

  it('an older Query-Engine without the endpoint: no warnings', async () => {
    const { comp } = create({ backend: { fail: /keys\/notIndexed/ } });
    await comp.ngOnInit();
    await new Promise(resolve => setTimeout(resolve));
    expect(comp.keysWithValuesNotSuggested.size).toBe(0);
  });
});

describe('collections ("Search in")', () => {
  const flush = () => new Promise(resolve => setTimeout(resolve));
  const COLLECTIONS = { collections: [
    { id: 'api', advancedSearch: true, simpleSearch: true },
    { id: 'orion', advancedSearch: true, simpleSearch: false },
    { id: 'minio', advancedSearch: true, simpleSearch: true },
  ] };
  const queries = (requests: any[]) => requests.filter(r => r.method === 'POST' && r.url.endsWith('/api/query')).map(r => r.options.body);

  it('the collections of the Query-Engine with their labels; all selected the first time', async () => {
    const { comp } = create({ backend: { collections: COLLECTIONS }, settings: { collectionLabels: { orion: 'Eurostat' } } });
    await comp.ngOnInit();
    await flush();
    expect(comp.collectionOptions.map(c => [c.id, c.label])).toEqual([['api', 'Sources'], ['orion', 'Eurostat'], ['minio', 'Files']]);
    expect(comp.collectionsFor('Advanced search')).toEqual(['api', 'orion', 'minio']);
    expect(comp.collectionsFor('Simple search')).toEqual(['api', 'minio']); // Orion not in the Simple search
  });

  it('never chosen in this browser: the Query-Engine defaults (queryOptions.defaultCollections), also for the suggestions', async () => {
    const withDefaults = { collections: COLLECTIONS.collections.map(c => ({ ...c, default: c.id !== 'orion' })) };
    const { comp, http } = create({ backend: { collections: withDefaults } });
    await comp.ngOnInit();
    expect(comp.selectedCollections).toEqual(['api', 'minio']);
    expect(comp.collectionsFor('Advanced search')).toEqual(['api', 'minio']);
    const keys = http.requests.filter(r => r.url.endsWith('/api/keys'));
    expect(keys.map(r => r.options.params.get('collections'))).toEqual(['api,minio']); // read once, with the defaults
    expect(localStorage.getItem('qe.selectedCollections')).toBeNull(); // not a choice of the user
  });

  it('a choice saved in this browser wins over the defaults', async () => {
    localStorage.setItem('qe.selectedCollections', JSON.stringify(['orion']));
    const withDefaults = { collections: COLLECTIONS.collections.map(c => ({ ...c, default: c.id !== 'orion' })) };
    const { comp } = create({ backend: { collections: withDefaults } });
    await comp.ngOnInit();
    await flush();
    expect(comp.selectedCollections).toEqual(['orion']);
  });

  it('an older Query-Engine: no choice, nothing sent', async () => {
    const { comp, http } = create({ backend: { query: () => [] } });
    await comp.ngOnInit();
    await flush();
    expect(comp.collectionOptions).toEqual([]);
    expect(comp.collectionsFor()).toBeUndefined();
    comp.minioQuery(true);
    await flush();
    expect(queries(http.requests)[0].collections).toBeUndefined();
  });

  it('the choice: sent with the queries and the suggestions, remembered in the browser', async () => {
    const { comp, http } = create({ backend: { collections: COLLECTIONS, query: () => ({ results: [], hasMore: false, next: {} }) } });
    await comp.ngOnInit();
    await flush();
    comp.toggleCollection('orion');
    comp.toggleCollection('minio');
    expect(JSON.parse(localStorage.getItem('qe.selectedCollections')!)).toEqual(['api']);
    comp.minioQuery(true);
    await flush();
    expect(queries(http.requests).at(-1).collections).toEqual(['api']);
    const keys = http.requests.filter(r => r.url.endsWith('/api/keys')).at(-1);
    expect(keys.options.params.get('collections')).toBe('api'); // re-preloaded with the new choice
    expect(create().comp.selectedCollections).toEqual(['api']); // a new page starts from the remembered choice
  });

  it('nothing selected for the mode: no query, buttons disabled', async () => {
    const { comp, http } = create({ backend: { collections: COLLECTIONS } });
    await comp.ngOnInit();
    await flush();
    for (const id of ['api', 'minio']) comp.toggleCollection(id);
    comp.setMode('Simple search');                 // only Orion selected, not searched by the Simple search
    expect(comp.noCollectionSelected).toBe(true);
    comp.minioQuery(true);
    expect(http.requests.filter(r => r.url.endsWith('/api/query'))).toEqual([]);
    comp.setMode('Advanced search');
    expect(comp.noCollectionSelected).toBe(false);
  });

  it('"Load more results": only the collections with more, from their `next`; each result labelled', async () => {
    const answers = [
      { results: [{ name: 'a1', _collection: 'api' }, { name: 'o1', _collection: 'orion' }], hasMore: true, next: { orion: 50 } },
      { results: [{ name: 'o2', _collection: 'orion' }], hasMore: false, next: {} },
    ];
    const { comp, http } = create({ backend: { collections: COLLECTIONS, query: () => answers.shift() } });
    await comp.ngOnInit();
    await flush();
    comp.minioQuery(true);
    await flush();
    comp.loadMoreResults();
    await flush();
    const [first, second] = queries(http.requests);
    expect(first.page).toEqual({ limit: 50, skip: 0 });
    expect([second.page, second.collections]).toEqual([{ limit: 50, skip: { orion: 50 } }, ['orion']]);
    expect(comp.extractedElements.map(e => [e.name, e.collection])).toEqual([['a1', 'Sources'], ['o1', 'Datapoints'], ['o2', 'Datapoints']]);
    expect(comp.extractedElements[0].element).toEqual({ name: 'a1' }); // _collection is not shown as data
    expect(comp.hasMoreResults).toBe(false);
  });

  it('Simple search limits: only those of the selected collections', async () => {
    const limits: QueryWarning[] = [
      { kind: 'config', code: 'ORION_DISABLED', collection: 'orion', message: 'o' },
      { kind: 'config', code: 'API_EXCLUDED', collection: 'api', source: 'X', message: 'a' },
    ];
    const { comp } = create({ backend: { collections: COLLECTIONS, limits } });
    await comp.ngOnInit();
    await flush();
    expect(comp.shownSimpleSearchLimits.map(w => w.code)).toEqual(['ORION_DISABLED', 'API_EXCLUDED']);
    comp.toggleCollection('orion');
    expect(comp.shownSimpleSearchLimits.map(w => w.code)).toEqual(['API_EXCLUDED']);
  });

  it('the snippet has the collections', async () => {
    const { comp } = create({ backend: { collections: COLLECTIONS } });
    await comp.ngOnInit();
    await flush();
    comp.toggleCollection('orion');
    comp.refreshSnippet();
    expect(comp.snippetCode).toContain('"collections": [');
    expect(comp.snippetCode).toContain('"api"');
    expect(comp.snippetCode).not.toContain('"orion"');
  });
});

describe('no collection selected', () => {
  const flush = () => new Promise(resolve => setTimeout(resolve));
  const COLLECTIONS = { collections: [
    { id: 'api', advancedSearch: true, simpleSearch: true, default: true },
    { id: 'orion', advancedSearch: true, simpleSearch: false, default: false },
    { id: 'minio', advancedSearch: true, simpleSearch: true, default: true },
  ] };
  const suggestionGets = (requests: any[]) => requests.filter(r => /\/api\/(keys|values|entries)(\/notIndexed)?$/.test(r.url));

  it('no suggestions at all, and no request for them', async () => {
    const { comp, http } = create({ backend: { collections: COLLECTIONS, keys: { items: [{ key: 'city' }], hasMore: false } } });
    await comp.ngOnInit();
    await flush();
    comp.toggleCollection('api');          // only Files: the suggestions of Files are read
    await flush();
    const before = suggestionGets(http.requests).length;
    comp.toggleCollection('minio');        // nothing selected
    expect(comp.suggestionCollections).toEqual([]);
    expect(await comp.preloadedKeys).toEqual({ items: [], hasMore: false });
    await flush();
    expect(comp.keysWithValuesNotSuggested.size).toBe(0);
    expect(suggestionGets(http.requests).length).toBe(before);
  });

  it('an older Query-Engine (no collections): a choice saved in this browser does not hide its suggestions', async () => {
    localStorage.setItem('qe.selectedCollections', JSON.stringify([]));
    const { comp, http } = create({ backend: { keys: { items: [{ key: 'city' }], hasMore: false } } });
    await comp.ngOnInit();
    await flush();
    expect(comp.suggestionCollections).toBeUndefined();
    expect(await comp.preloadedKeys).toEqual({ items: ['city'], hasMore: false });
    expect(http.requests.filter(r => r.url.endsWith('/api/keys')).at(-1).options.params.has('collections')).toBe(false);
  });
});
