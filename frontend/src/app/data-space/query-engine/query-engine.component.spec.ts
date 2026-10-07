import { describe, expect, it, vi } from 'vitest';
import { of, throwError } from 'rxjs';

// Only NbAuthService's getToken() is used (snippet with the real token): no need to load Nebular
vi.mock('@nebular/auth', () => ({ NbAuthService: class { } }));

import { QueryEngineComponent } from './query-engine.component';
import { BeopenAPIService } from '../../services/be-open.service';

const TOO_MANY = ['Too many suggestions. Type some characters in order to reduce them'];

interface Backend {
  keys?: any;                                   // GET /api/keys
  entries?: (key: string) => any;               // GET /api/entries?key=...
  query?: (format: string | null) => any[];     // POST /api/query (Advanced search), by format param
  fail?: RegExp;                                // urls that answer with an error
}

// Real BeopenAPIService on a fake HttpClient: the component's requests go through the real request builders.
function fakeHttp(backend: Backend) {
  const requests: any[] = [];
  const answer = (url: string, value: () => any) => backend.fail?.test(url) ? throwError(() => new Error('HTTP 500')) : of(value());
  return {
    requests,
    get: (url: string) => answer(url, () => {
      if (url.includes('/api/keys')) return backend.keys ?? [];
      if (url.includes('/api/values')) return [];
      if (url.includes('/api/entries')) {
        const key = decodeURIComponent(url.split('key=')[1].split('&')[0]);
        return backend.entries?.(key) ?? [];
      }
      if (url.includes('/api/user')) return { email: 'anna@demetrix.it' };
      return [];
    }),
    request: (method: string, url: string, options: any) => {
      requests.push({ method, url, options });
      return answer(url, () => backend.query?.(options.params.get('format')) ?? []);
    },
  };
}

function create({ backend = {}, settings = {}, token = null as string | null } = {}) {
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
    {} as any, api, {} as any, toast as any, {} as any, shared as any, { BucketObjectsPush: () => { } } as any, nbAuth as any, config as any,
    { nativeElement: document.createElement('div') } as any,
  );
  comp.visibility = 'public';
  return { comp, http };
}

describe('startup', () => {
  it('loads keys/values/entries and flags the autocompletes ready', async () => {
    const { comp } = create({ backend: { keys: [{ key: 'city' }, { key: 'city' }, { key: 'n' }] } });
    await comp.ngOnInit();
    expect(comp.keys).toEqual(['city', 'n']);
    expect(comp.autocompleteDataReady).toBe(true);
  });

  it('keeps the backend "too many" message as it is', async () => {
    const { comp } = create({ backend: { keys: TOO_MANY } });
    await comp.ngOnInit();
    expect(comp.keys).toEqual(TOO_MANY);
  });

  it('ready even when a call fails', async () => {
    const { comp } = create({ backend: { fail: /api\/values/ } });
    await expect(comp.ngOnInit()).rejects.toThrow();
    expect(comp.autocompleteDataReady).toBe(true);
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
