import { describe, expect, it } from 'vitest';
import { CacheComponent } from './cache.component';

const row = (query: string, version: number, state: string, extra: any = {}) => ({ _id: query + version, query, version, state, count: 10, ...extra });

function fakeApi(rows: any[]) {
  const calls: any[] = [];
  const api: any = {
    list: async (filter?: string) => { calls.push(['list', filter]); return rows; },
    backup: async (filter?: string) => { calls.push(['backup', filter]); return { timestamp: 1700000000000, queries: 2 }; },
    restoreBackup: async (ts: number) => { calls.push(['restoreBackup', ts]); return { restored: 1, alreadyActive: 1 }; },
    activate: async (id: string) => { calls.push(['activate', id]); return { restored: 1, alreadyActive: 0 }; },
    deleteBackup: async (ts: number, secret: string) => { calls.push(['deleteBackup', ts, secret]); return { released: 2 }; },
    deactivate: async (filter?: string) => { calls.push(['deactivate', filter]); return { reset: 3 }; },
  };
  return { api, calls };
}

describe('CacheComponent', () => {
  const ROWS = [
    row('{ q1 }', 1, 'previous', { backups: [100] }),
    row('{ q1 }', 2, 'active', { survey: 'NAMA', backups: [200] }),
    row('{ q2 }', 1, 'active', { backups: [100, 200] }),
  ];

  it('the versions grouped by query (active first), the backups newest first', () => {
    const queries = CacheComponent.queriesOf(ROWS as any);
    expect(queries.map(q => [q.label, q.active?.version, q.versions.map(v => v.version)])).toEqual([['{ q2 }', 1, [1]], ['NAMA', 2, [2, 1]]]);
    expect(CacheComponent.backupsOf(ROWS as any)).toEqual([
      { timestamp: 200, versions: 2, queries: 2, active: true },
      { timestamp: 100, versions: 2, queries: 2, active: false },
    ]);
    expect(CacheComponent.shortQuery('a'.repeat(100)).length).toBe(90);
  });

  it('actions: then the list again, with the filter applied', async () => {
    const { api, calls } = fakeApi(ROWS);
    const component = new CacheComponent(api);
    component.filter = ' nama ';
    await component.search();
    await component.backup();
    await component.restoreBackup({ timestamp: 100 } as any);
    await component.activate(ROWS[0] as any);
    expect(calls).toEqual([['list', 'nama'], ['backup', 'nama'], ['list', 'nama'], ['restoreBackup', 100], ['list', 'nama'], ['activate', '{ q1 }1'], ['list', 'nama']]);
    expect(component.lastMessage).toEqual({ key: 'Cache version activated', params: { version: 1 } });
  });

  it('deactivating the active cache: after the confirmation, of the filtered queries or all; refused: the message', async () => {
    const { api, calls } = fakeApi(ROWS);
    const component = new CacheComponent(api);
    component.askDeactivate();
    expect(component.deactivating).toBe('');
    component.cancelDeactivate();
    await component.confirmDeactivate(); // cancelled: nothing
    expect(calls).toEqual([]);
    component.askDeactivate();
    await component.confirmDeactivate();
    expect(calls.slice(0, 1)).toEqual([['deactivate', undefined]]);
    expect([component.deactivating, component.lastMessage]).toEqual([null, { key: 'Cache deactivated', params: { reset: 3 } }]);
    component.filter = 'nama';
    await component.search();
    component.askDeactivate();
    component.filter = 'other'; // typed but not applied: the confirmation keeps the applied filter
    await component.confirmDeactivate();
    expect(calls.filter(c => c[0] == 'deactivate')).toEqual([['deactivate', undefined], ['deactivate', 'nama']]);
    api.deactivate = async () => { throw { status: 403, error: 'One of these roles is required: qe-admin' }; };
    component.askDeactivate();
    await component.confirmDeactivate();
    expect(component.actionError).toBe('One of these roles is required: qe-admin');
    expect(component.deactivating).toBe('nama');
  });

  it('authentication disabled in the Query-Engine: asks its admin token, uses it (in memory) until leaving the page', async () => {
    const asked = { status: 403, error: 'X-Admin-Token missing or wrong' };
    const { api, calls } = fakeApi(ROWS);
    api.adminToken = '';
    api.list = async () => { calls.push(['list', api.adminToken]); if (api.adminToken !== 'tok') throw asked; return ROWS; };
    const component = new CacheComponent(api);
    await component.load();
    expect([component.adminTokenNeeded, component.adminTokenWrong, component.error, component.queries]).toEqual([true, false, null, []]);
    component.adminTokenInput = 'bad';
    await component.useAdminToken();
    expect([component.adminTokenNeeded, component.adminTokenWrong, component.adminTokenInput]).toEqual([true, true, '']);
    component.adminTokenInput = ' tok ';
    await component.useAdminToken();
    expect([component.adminTokenNeeded, component.adminTokenWrong, component.queries.length]).toEqual([false, false, 2]);
    expect(calls.map(c => c[1])).toEqual(['', 'bad', 'tok']);
    component.ngOnDestroy();
    expect(api.adminToken).toBe('');
  });

  it('no admin token configured in the Query-Engine: its message as it is, no form', async () => {
    const { api } = fakeApi(ROWS);
    api.list = async () => { throw { status: 403, error: 'Cache management is disabled while authConfig.disableAuth is true: set authConfig.adminToken and send it in the X-Admin-Token header' }; };
    const component = new CacheComponent(api);
    await component.load();
    expect(component.adminTokenNeeded).toBe(false);
    expect(component.error).toMatch(/set authConfig.adminToken/);
  });

  it('deleting a backup: the confirmation, the password (not kept); refused: the message of the Query-Engine', async () => {
    const { api, calls } = fakeApi(ROWS);
    const component = new CacheComponent(api);
    component.askDelete({ timestamp: 100 } as any);
    await component.confirmDelete(); // no password: nothing
    expect(calls).toEqual([]);
    component.secret = 'pass';
    await component.confirmDelete();
    expect(calls[0]).toEqual(['deleteBackup', 100, 'pass']);
    expect([component.secret, component.deleting]).toEqual(['', null]);
    api.deleteBackup = async () => { throw { status: 403, error: 'Wrong or missing management password (X-Management-Secret)' }; };
    component.askDelete({ timestamp: 200 } as any);
    component.secret = 'wrong';
    await component.confirmDelete();
    expect(component.actionError).toMatch(/management password/);
    expect(component.deleting).toBe(200);
    expect(CacheComponent.messageOf({ error: { error: 'boom' } })).toBe('boom');
  });
});
