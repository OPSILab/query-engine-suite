import { describe, expect, it, beforeEach } from 'vitest';
import { of } from 'rxjs';
import { HttpHeaders, HttpParams } from '@angular/common/http';
import { BeopenAPIService, definedHeaders } from './be-open.service';

// Minimal HttpClient: records calls, answers `response`
function fakeHttp(response: any = []) {
  const calls: { method: string; url: string; options?: any }[] = [];
  return {
    calls,
    request: (method: string, url: string, options: any) => { calls.push({ method, url, options }); return of(response); },
    get: (url: string) => { calls.push({ method: 'GET', url }); return of(response); },
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

function service(settings: any = { beopenApiBaseUrl: 'http://be', queryEngineBaseUrl: 'http://qe' }, response?: any) {
  const http = fakeHttp(response);
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
