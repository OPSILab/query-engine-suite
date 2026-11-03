import { describe, expect, it } from 'vitest';
import { StatusLogComponent } from './status-log.component';
import { StatusLogPage, StatusLogRecord } from '../services/be-open.service';

const record = (n: number, extra: Partial<StatusLogRecord> = {}): StatusLogRecord =>
  ({ createdAt: `2026-11-0${n}T10:00:00Z`, method: 'GET', url: `https://api.example/${n}`, status: 500, ...extra });

// BeopenAPIService.getStatusLog: records the calls, answers from `pages` (in order) or with `answer(call)`
function fakeApi(answer: (call: any) => Promise<StatusLogPage>) {
  const calls: any[] = [];
  return { calls, api: { getStatusLog: (filters: any, page: any) => { const call = { filters, page }; calls.push(call); return answer(call); } } };
}
const page = (items: StatusLogRecord[], total = items.length, masked = true): Promise<StatusLogPage> => Promise.resolve({ items, total, masked, ignoredExcluded: 0 });
const config = (auth = false) => ({ getSettings: (key: string, def?: any) => key === 'enableAuthentication' ? auth : def });

function create(answer: (call: any) => Promise<StatusLogPage>, auth = false) {
  const { calls, api } = fakeApi(answer);
  const navigated: any[] = [];
  const component = new StatusLogComponent(api as any, config(auth) as any, { navigate: (to: any) => navigated.push(to) } as any);
  return { component, calls, navigated };
}

describe('StatusLogComponent', () => {
  it('first page without filters on init', async () => {
    const { component, calls } = create(() => page([record(2), record(1)], 2));
    component.ngOnInit();
    await Promise.resolve(); await Promise.resolve();
    expect(calls).toEqual([{ filters: {}, page: { limit: StatusLogComponent.PAGE_SIZE, skip: 0 } }]);
    expect(component.items.length).toBe(2);
    expect(component.total).toBe(2);
    expect(component.masked).toBe(true);
    expect(component.hasMore).toBe(false);
    expect(component.loading).toBeNull();
  });

  it('filters: whole local days as ISO dates, the url trimmed, empty ones left out', () => {
    const filters = StatusLogComponent.filtersOf('2026-11-01', '2026-11-02', '  api.example ');
    expect(filters.from).toBe(new Date(2026, 10, 1, 0, 0, 0).toISOString());
    expect(filters.to).toBe(new Date(2026, 10, 2, 23, 59, 59, 999).toISOString());
    expect(filters.url).toBe('api.example');
    expect(StatusLogComponent.filtersOf('', '', '  ')).toEqual({});
    expect(StatusLogComponent.filtersOf('not a date', '', '')).toEqual({});
    expect(StatusLogComponent.filtersOf('', '', '', 'success')).toEqual({ outcome: 'success' });
    expect(StatusLogComponent.filtersOf('', '', '', '')).toEqual({});
  });

  it('outcome pills: applied at once with the rest of the form; reset back to all', async () => {
    const { component, calls } = create(() => page([]));
    expect(component.outcomes.map(o => o.value)).toEqual(['', 'error', 'success']);
    component.url = 'example';
    await component.setOutcome('error');
    expect(calls[0].filters).toEqual({ url: 'example', outcome: 'error' });
    await component.setOutcome('');
    expect(calls[1].filters).toEqual({ url: 'example' });
    component.outcome = 'success';
    await component.reset();
    expect(component.outcome).toBe('');
    expect(calls[2].filters).toEqual({});
  });

  it('search applies the form; load more asks the next page and appends it', async () => {
    const { component, calls } = create(call => page(call.page.skip ? [record(1)] : [record(3), record(2)], 3, false));
    component.url = 'example';
    await component.search();
    expect(calls[0].filters).toEqual({ url: 'example' });
    expect(component.hasMore).toBe(true);
    expect(component.masked).toBe(false);
    await component.loadMore();
    expect(calls[1]).toEqual({ filters: { url: 'example' }, page: { limit: StatusLogComponent.PAGE_SIZE, skip: 2 } });
    expect(component.items.map(i => i.url)).toEqual(['https://api.example/3', 'https://api.example/2', 'https://api.example/1']);
    expect(component.hasMore).toBe(false);
  });

  it('reset empties the form and searches again', async () => {
    const { component, calls } = create(() => page([]));
    Object.assign(component, { from: '2026-11-01', to: '2026-11-02', url: 'x', outcome: 'error' });
    await component.reset();
    expect([component.from, component.to, component.url, component.outcome]).toEqual(['', '', '', '']);
    expect(calls[0].filters).toEqual({});
  });

  it('the answer of a request replaced by a newer one is dropped', async () => {
    let releaseFirst!: () => void;
    const { component } = create(call => call.filters.url === 'old'
      ? new Promise(resolve => releaseFirst = () => resolve({ items: [record(1)], total: 1, masked: true, ignoredExcluded: 0 }))
      : page([record(2)]));
    component.url = 'old';
    const first = component.search();
    component.url = 'new';
    await component.search();
    releaseFirst();
    await first;
    expect(component.items.map(i => i.url)).toEqual(['https://api.example/2']);
  });

  it('an error: error state, not loading', async () => {
    const { component } = create(() => Promise.reject(new Error('down')));
    await component.search();
    expect(component.error).toBe(true);
    expect(component.loading).toBeNull();
  });

  it('opening a request; a new search closes them', async () => {
    const { component } = create(() => page([record(1)]));
    await component.search();
    const item = component.items[0];
    component.toggle(item);
    expect(component.isOpen(item)).toBe(true);
    component.toggle(item);
    expect(component.isOpen(item)).toBe(false);
    component.toggle(item);
    await component.search();
    expect(component.isOpen(item)).toBe(false);
  });

  it('status: the code, or the error code without answer; below 400 success, 4xx client, the rest server', () => {
    const { component } = create(() => page([]));
    expect([component.statusLabel(record(1, { status: 200, outcome: 'success' })), component.statusKind(record(1, { status: 200, outcome: 'success' }))]).toEqual(['200', 'success']);
    expect(component.statusKind(record(1, { status: 304 }))).toBe('success');
    expect(component.statusKind(record(1, { status: 200, outcome: 'error' }))).toBe('server'); // failed after the answer (e.g. a client validateStatus)
    expect([component.statusLabel(record(1, { status: 404 })), component.statusKind(record(1, { status: 404 }))]).toEqual(['404', 'client']);
    expect([component.statusLabel(record(1, { status: 502 })), component.statusKind(record(1, { status: 502 }))]).toEqual(['502', 'server']);
    const refused = record(1, { status: undefined, code: 'ECONNREFUSED', error: 'connect ECONNREFUSED' });
    expect([component.statusLabel(refused), component.statusKind(refused)]).toEqual(['ECONNREFUSED', 'server']);
  });

  it('details: only the parts the request has, JSON indented, cut texts flagged', () => {
    const parts = StatusLogComponent.detailsOf({
      createdAt: '2026-11-01T10:00:00Z', url: 'https://api.example/x', fullUrl: 'https://api.example/x?from=1',
      params: { from: '1' }, headers: { Authorization: '<masked>' }, body: '{"id":7}',
      status: 500, statusText: 'Internal Server Error', data: '{"message":"bo', dataTruncated: true, dataLength: 900,
    });
    expect(parts.map(p => p.label)).toEqual(['URL', 'Query params', 'Headers', 'Body', 'Response', 'Response data']);
    expect(parts[0].text).toBe('https://api.example/x?from=1');
    expect(parts[2].text).toBe('{\n  "Authorization": "<masked>"\n}');
    expect(parts[3].text).toBe('{\n  "id": 7\n}');
    expect(parts[4].text).toBe('500 Internal Server Error');
    expect(parts[5]).toEqual({ label: 'Response data', text: '{"message":"bo', cutAt: 900 }); // cut JSON: as it is
    const refused = StatusLogComponent.detailsOf({ createdAt: '', url: 'u', error: 'connect ECONNREFUSED', code: 'ECONNREFUSED' });
    expect(refused.map(p => p.text)).toEqual(['u', 'ECONNREFUSED: connect ECONNREFUSED']);
    const success = StatusLogComponent.detailsOf({ createdAt: '', url: 'u', status: 200, statusText: 'OK', outcome: 'success', durationMs: 42 });
    expect(success.map(p => [p.label, p.text])).toEqual([['URL', 'u'], ['Response', '200 OK'], ['Duration', '42 ms']]);
  });

  it('statusLogSince of config.json: the valid days only; none: {}', () => {
    expect(StatusLogComponent.sinceOf({ errors: '2026-11-02', successes: '2026-11-03' })).toEqual({ errors: '2026-11-02', successes: '2026-11-03' });
    expect(StatusLogComponent.sinceOf({ errors: '2/11', successes: '2026-11-03' })).toEqual({ successes: '2026-11-03' });
    expect(StatusLogComponent.sinceOf(null)).toEqual({});
    expect(StatusLogComponent.sinceOf('2026-11-02')).toEqual({});
    const { component } = create(() => page([]));
    expect(component.since).toEqual({});
  });

  it('login: back to this page afterwards', () => {
    const { component, navigated } = create(() => page([]), true);
    component.login();
    expect(navigated).toEqual([['/keycloak-auth']]);
    expect(sessionStorage.getItem('qe.returnUrl')).toBe('/status-log');
    expect(component.authEnabled).toBe(true);
  });

  it('ignore: the url saved, the ignored urls updated, the first page again with the same filters', async () => {
    const changes: any[] = [];
    const { component, calls } = create(call => page(call.page.skip ? [] : [record(1)], 1, false));
    Object.assign((component as any).api, {
      getStatusLogIgnored: () => Promise.resolve([]),
      ignoreStatusLogUrl: (url: string) => { changes.push(['ignore', url]); return Promise.resolve([{ url }]); },
      restoreStatusLogUrl: (url: string) => { changes.push(['restore', url]); return Promise.resolve([]); },
    });
    component.url = 'api';
    await component.search();
    expect(component.canManage).toBe(true);
    const stopped: string[] = [];
    await component.ignore(component.items[0], { stopPropagation: () => stopped.push('x') } as any);
    expect(stopped).toEqual(['x']); // the row does not open
    expect(changes).toEqual([['ignore', 'https://api.example/1']]);
    expect(component.ignoredUrls).toEqual([{ url: 'https://api.example/1' }]);
    expect(calls.length).toBe(2);
    expect(calls[1]).toEqual({ filters: { url: 'api' }, page: { limit: StatusLogComponent.PAGE_SIZE, skip: 0 } });
    expect(component.busyUrl).toBeNull();
    await component.restore('https://api.example/1');
    expect(changes[1]).toEqual(['restore', 'https://api.example/1']);
    expect(component.ignoredUrls).toEqual([]);
  });

  it('a change refused (not an administrator): actionError, nothing else changes', async () => {
    const { component } = create(() => page([record(1)], 1, true));
    Object.assign((component as any).api, { ignoreStatusLogUrl: () => Promise.reject(new Error('403')) });
    await component.search();
    expect(component.canManage).toBe(false);
    await component.ignore(component.items[0]);
    expect(component.actionError).toBe(true);
    expect(component.busyUrl).toBeNull();
  });

  it('the ignored urls left out of the answer: from the page', async () => {
    const { component } = create(() => Promise.resolve({ items: [record(1)], total: 1, masked: false, ignoredExcluded: 2 }));
    await component.search();
    expect(component.ignoredExcluded).toBe(2);
  });

  it('urls ignored in the Query-Engine config: recognized, not restored from here', async () => {
    const { component } = create(() => page([]));
    component.ignoredUrls = [{ url: 'https://a.example', fromConfig: true }, { url: 'https://b.example' }];
    expect(component.isFromConfig('https://a.example')).toBe(true);
    expect(component.isFromConfig('https://b.example')).toBe(false);
    expect(component.isFromConfig(undefined)).toBe(false);
    component.ignoredUrls = [{ url: 'https://a.example/', match: 'prefix', fromConfig: true }];
    expect(component.isFromConfig('https://a.example/', 'prefix')).toBe(true);
    expect(component.isFromConfig('https://a.example/')).toBe(false); // the same text as an exact url: not that one
  });

  it('prefixes: at least scheme and host; ignored and restored as prefixes; the form emptied after it', async () => {
    const changes: any[] = [];
    const { component, calls } = create(() => page([record(1)], 1, false));
    Object.assign((component as any).api, {
      ignoreStatusLogUrl: (url: string, match?: string) => { changes.push(['ignore', url, match]); return Promise.resolve([{ url, match: 'prefix' }]); },
      restoreStatusLogUrl: (url: string, match?: string) => { changes.push(['restore', url, match]); return Promise.resolve([]); },
    });
    await component.search();
    for (const bad of ['', 'dx-lab.eng.it', 'https://'])
      await component.ignorePrefix(bad);
    expect(component.prefixInvalid).toBe(true);
    expect(changes).toEqual([]);
    component.prefixInput = ' https://dx-lab.eng.it/ ';
    await component.ignorePrefix(component.prefixInput);
    expect(changes).toEqual([['ignore', 'https://dx-lab.eng.it/', 'prefix']]);
    expect([component.prefixInvalid, component.prefixInput]).toEqual([false, '']);
    expect(calls.length).toBe(2); // the first page again
    await component.restore('https://dx-lab.eng.it/', 'prefix');
    expect(changes[1]).toEqual(['restore', 'https://dx-lab.eng.it/', 'prefix']);
  });

  it('the domain of a request as a prefix', () => {
    expect(StatusLogComponent.domainPrefix('https://dx-lab.eng.it/api/v1/x?y=1')).toBe('https://dx-lab.eng.it/');
    expect(StatusLogComponent.domainPrefix('http://localhost:5500/api/map')).toBe('http://localhost:5500/');
    expect(StatusLogComponent.domainPrefix('/relative')).toBeUndefined();
    expect(StatusLogComponent.domainPrefix(undefined)).toBeUndefined();
  });
});
