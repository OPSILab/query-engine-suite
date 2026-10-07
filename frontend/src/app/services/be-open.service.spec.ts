import { describe, expect, it, beforeEach } from 'vitest';
import { firstValueFrom, of, throwError } from 'rxjs';
import { HttpHeaders, HttpParams, HttpResponse } from '@angular/common/http';
import { BeopenAPIService, QueryWarning, definedHeaders, parseQueryWarnings } from './be-open.service';

// Minimal HttpClient: records calls, answers `response` (request(): as an HttpResponse with `responseHeaders`)
function fakeHttp(response: any = [], responseHeaders: Record<string, string> = {}) {
  const calls: { method: string; url: string; options?: any }[] = [];
  return {
    calls,
    request: (method: string, url: string, options: any) => {
      calls.push({ method, url, options });
      return of(new HttpResponse({ body: response, headers: new HttpHeaders(responseHeaders) }));
    },
    get: (url: string) => { calls.push({ method: 'GET', url }); return typeof response === 'function' ? response(url) : of(response); },
  };
}

function fakeConfig(settings: any) {
  return {
    getSettings: (key?: string, defaultValue?: any) => {
      if (!key) return settings;
      if (settings[key] !== undefined) return settings[key];
      if (defaultValue !== undefined) return defaultValue;
      throw new Error('No setting ' + key);
    },
  };
}

function service(settings: any = { beopenApiBaseUrl: 'http://be', queryEngineBaseUrl: 'http://qe' }, response?: any, responseHeaders?: Record<string, string>) {
  const http = fakeHttp(response, responseHeaders);
  return { http, api: new BeopenAPIService(http as any, fakeConfig(settings) as any) };
}

describe('definedHeaders', () => {
  it('drops undefined/null, stringifies the rest', () => {
    expect(definedHeaders({ visibility: undefined, a: null, b: 0, c: 'x' })).toEqual({ b: '0', c: 'x' });
  });
});

describe('endpoints from config', () => {
  it('queryEngineBaseUrl, falling back to beopenApiBaseUrl; graphqlUrl overrides /graphql', () => {
    expect(service().api.graphqlEndpoint).toBe('http://qe/graphql');
    expect(service({ beopenApiBaseUrl: 'http://be' }).api.queryEngineBaseUrl).toBe('http://be');
    expect(service({ beopenApiBaseUrl: 'http://be', graphqlUrl: 'http://gw/gql' }).api.graphqlEndpoint).toBe('http://gw/gql');
  });
});

describe('buildQueryRequest', () => {
  let api: BeopenAPIService;
  beforeEach(() => ({ api } = service()));

  it('Advanced search: POST with mongoQuery in the body and as query params (+ format)', () => {
    expect(api.buildQueryRequest('Advanced search', '', { city: 'Rome', n: 3, empty: null }, '', 'public', 'JSON')).toEqual({
      method: 'POST',
      url: 'http://qe/api/query',
      params: { format: 'JSON', city: 'Rome', n: '3', empty: '' },
      headers: { visibility: 'public' },
      body: { mongoQuery: { city: 'Rome', n: 3, empty: null } },
    });
  });

  it('Advanced search without a format: no format param', () => {
    expect(api.buildQueryRequest('Advanced search', '', {}, '', 'private', undefined)!.params).toEqual({});
  });

  it('Query SQL: POST with the query', () => {
    expect(api.buildQueryRequest('Query SQL', '', {}, 'SELECT 1', 'shared', 'CSV')).toEqual({
      method: 'POST', url: 'http://qe/api/query', params: {}, headers: { visibility: 'shared' }, body: { query: 'SELECT 1' },
    });
  });

  it('Simple search: GET with value and isRawQuery', () => {
    expect(api.buildQueryRequest('Simple search', 'abc', {}, '', 'public', undefined)).toEqual({
      method: 'GET', url: 'http://qe/api/query', params: { value: 'abc' }, headers: { visibility: 'public', isRawQuery: 'yes' },
    });
  });

  it('unknown mode: undefined (and minioQuery sends nothing)', () => {
    const { http, api: a } = service();
    expect(a.buildQueryRequest('Query GraphQL', '', {}, '', 'public', undefined)).toBeUndefined();
    expect(a.minioQuery('Query GraphQL', '', {}, '', 'public', undefined)).toBeUndefined();
    expect(http.calls).toEqual([]);
  });
});

describe('sending', () => {
  it('minioQuery sends exactly the built request; undefined headers are not sent', () => {
    const { http, api } = service();
    api.minioQuery('Advanced search', '', { k: 'v' }, '', undefined as any, 'JSON').subscribe();
    const [call] = http.calls;
    expect(call.method).toBe('POST');
    expect(call.url).toBe('http://qe/api/query');
    expect(call.options.body).toEqual({ mongoQuery: { k: 'v' } });
    expect(call.options.params).toBeInstanceOf(HttpParams);
    expect(call.options.params.toString()).toBe('format=JSON&k=v');
    expect(call.options.headers).toBeInstanceOf(HttpHeaders);
    expect(call.options.headers.has('visibility')).toBe(false);
  });

  it('graphqlQuery posts { query } to the GraphQL endpoint with the visibility header', () => {
    const { http, api } = service();
    expect(api.buildGraphqlRequest('{ sources { id } }', 'private')).toEqual({
      method: 'POST', url: 'http://qe/graphql', params: {}, headers: { visibility: 'private' }, body: { query: '{ sources { id } }' },
    });
    api.graphqlQuery('{ sources { id } }', 'private').subscribe();
    expect(http.calls[0].options.headers.get('visibility')).toBe('private');
    expect(http.calls[0].options.body).toEqual({ query: '{ sources { id } }' });
  });
});

describe('suggestions', () => {
  it('getKeys / getValues / getEntries urls, a leading "[" escaped for the backend regex', async () => {
    const { http, api } = service(undefined, [{ key: 'k' }]);
    expect(await api.getKeys('[a')).toEqual([{ key: 'k' }]);
    await api.getValues();
    await api.getEntries('[k', '[v');
    expect(http.calls.map(c => c.url)).toEqual([
      'http://qe/api/keys?key=\\[a',
      'http://qe/api/values?value=',
      'http://qe/api/entries?key=\\[k&value=\\[v',
    ]);
  });
});

describe('query warnings (Simple search)', () => {
  const warnings: QueryWarning[] = [
    { kind: 'config', code: 'ORION_DISABLED', message: 'Orion sources are not searched' },
    { kind: 'runtime', code: 'API_ERROR', source: 'Città', message: 'API "Città" could not be searched' },
  ];
  const header = encodeURIComponent(JSON.stringify(warnings));

  it('parseQueryWarnings: URI-encoded JSON array; anything else is no warning', () => {
    expect(parseQueryWarnings(header)).toEqual(warnings);
    for (const bad of [null, undefined, '', '%E0%A4%A', 'not json', encodeURIComponent('{"code":"X"}'), encodeURIComponent('[1, {"no":"code"}]')])
      expect(parseQueryWarnings(bad as any)).toEqual([]);
  });

  it('a query returns the body and publishes the X-Query-Warnings of its response', async () => {
    const { api } = service(undefined, [{ name: 'r' }], { 'X-Query-Warnings': header });
    const published: QueryWarning[][] = [];
    api.queryWarnings$.subscribe(w => published.push(w));
    expect(await firstValueFrom(api.minioQuery('Simple search', 'Rome', {}, '', 'public', undefined)!)).toEqual([{ name: 'r' }]);
    expect(published).toEqual([warnings]);
  });

  it('no header: nothing published', async () => {
    const { api } = service(undefined, []);
    const published: any[] = [];
    api.queryWarnings$.subscribe(w => published.push(w));
    await firstValueFrom(api.graphqlQuery('{ sources { id } }', 'public'));
    expect(published).toEqual([]);
  });

  it('getSimpleSearchLimits: the warnings of /api/query/simple/limits, [] when unavailable', async () => {
    const ok = service(undefined, (url: string) => of({ warnings }));
    expect(await ok.api.getSimpleSearchLimits()).toEqual(warnings);
    expect(ok.http.calls[0].url).toBe('http://qe/api/query/simple/limits');
    const missing = service(undefined, () => throwError(() => new Error('404')));
    expect(await missing.api.getSimpleSearchLimits()).toEqual([]);
    const odd = service(undefined, () => of({}));
    expect(await odd.api.getSimpleSearchLimits()).toEqual([]);
  });
});
