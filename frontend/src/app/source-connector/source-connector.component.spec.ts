import { describe, expect, it } from 'vitest';
import { SourceConnectorComponent } from './source-connector.component';

function fakeApi(overrides: Record<string, any> = {}) {
  const calls: any[] = [];
  const api: any = {
    baseUrl: 'http://sc',
    getRebuildStatus: async () => ({ running: false }),
    getResetStatus: async () => ({ running: false }),
    getResetLog: async () => ({ items: [], secretConfigured: true }),
    getQueue: async () => ({ queue: 0, processing: null, waiting: [] }),
    incrementalApis: async () => [{ name: 'A', url: 'https://a', records: { mongo: 3 } }, { name: 'B', url: 'https://b', records: { mongo: 1, postgres: 1 } }],
    startRebuild: async (connector?: string) => { calls.push(['rebuild', connector]); return { started: true }; },
    startReset: async (names: string[], secret: string) => { calls.push(['reset', names, secret]); return { started: true, apis: names }; },
    ...overrides,
  };
  return { api, calls };
}

describe('SourceConnectorComponent', () => {
  it('without sourceConnectorBaseUrl: nothing asked', async () => {
    const { api } = fakeApi({ baseUrl: '', getRebuildStatus: async () => { throw new Error('not expected'); } });
    const component = new SourceConnectorComponent(api);
    component.ngOnInit();
    expect(component.configured).toBe(false);
  });

  it('rebuild: of one connector or all; the error text of the Source-Connector shown', async () => {
    const { api, calls } = fakeApi();
    const component = new SourceConnectorComponent(api);
    await component.startRebuild();
    component.connector = 'orion';
    await component.startRebuild();
    expect(calls).toEqual([['rebuild', undefined], ['rebuild', 'orion']]);
    api.startRebuild = async () => { throw { status: 409, error: 'A rebuild is already running' }; };
    await component.startRebuild();
    expect(component.rebuildError).toBe('A rebuild is already running');
  });

  it('reset: only with APIs selected and the password, after the confirmation; the password not kept', async () => {
    const { api, calls } = fakeApi();
    const component = new SourceConnectorComponent(api);
    await component.loadApis();
    expect(component.apis.map(a => a.name)).toEqual(['A', 'B']);
    expect(component.canReset).toBe(false);
    component.toggleApi('B');
    expect(component.canReset).toBe(false); // no password
    component.secret = 'pass';
    expect(component.canReset).toBe(true);
    component.askReset();
    expect(component.confirming).toBe(true);
    expect(component.selectedNames).toBe('B');
    await component.confirmReset();
    expect(calls).toEqual([['reset', ['B'], 'pass']]);
    expect([component.secret, component.selected.size, component.confirming]).toEqual(['', 0, false]);
  });

  it('reset refused (wrong password): the message, the selection kept; disabled without a password configured', async () => {
    const { api } = fakeApi({ startReset: async () => { throw { status: 403, error: 'Wrong or missing management password (X-Management-Secret)' }; } });
    const component = new SourceConnectorComponent(api);
    await component.loadApis();
    component.toggleApi('A');
    component.secret = 'wrong';
    component.askReset();
    await component.confirmReset();
    expect(component.resetError).toMatch(/management password/);
    expect(component.selected.has('A')).toBe(true);
    component.secretConfigured = false;
    expect(component.canReset).toBe(false);
  });

  it('while a rebuild or a reset runs: no other one, the states read again', async () => {
    const { api } = fakeApi({ getResetStatus: async () => ({ running: true, phase: 'polling', apis: ['A'] }) });
    const component = new SourceConnectorComponent(api);
    await component.refresh();
    expect(component.busy).toBe(true);
    component.toggleApi('A');
    component.secret = 'x';
    expect(component.canReset).toBe(false);
    component.ngOnDestroy();
  });

  it('the queue of the Orion notifications: read with the states, again while not empty; datasets on one line', async () => {
    const queue = { queue: 13, processing: { receivedAt: 'a', startedAt: 'b', entities: [{ id: 'x', url: 'https://d/1' }] },
      waiting: Array.from({ length: 12 }, (_, i) => ({ receivedAt: 'r' + i, entities: [{ id: 'urn:' + i }] })) };
    const { api } = fakeApi({ getQueue: async () => queue });
    const component = new SourceConnectorComponent(api);
    await component.refresh();
    expect(component.queue).toEqual(queue);
    expect(component.waitingShown.length).toBe(SourceConnectorComponent.WAITING_SHOWN);
    expect((component as any).timer).not.toBeNull(); // read again in a few seconds
    component.ngOnDestroy();
    expect(SourceConnectorComponent.datasetsOf(queue.processing)).toBe('https://d/1');
    expect(SourceConnectorComponent.datasetsOf({ entities: [{ id: 'a' }, { url: 'u' }], moreEntities: 3 })).toBe('a, u (+3)');
    expect(SourceConnectorComponent.datasetsOf(null)).toBe('-');
    const { api: failing } = fakeApi({ getQueue: async () => { throw { status: 0 }; } });
    const broken = new SourceConnectorComponent(failing);
    await broken.refresh();
    expect(broken.queueError).toBe('Source-Connector not reachable');
  });

  it('counts and messages', () => {
    expect(SourceConnectorComponent.counts({ mongo: 3, postgres: 2 })).toBe('MongoDB 3 · PostgreSQL 2');
    expect(SourceConnectorComponent.counts(undefined)).toBe('-');
    expect(SourceConnectorComponent.messageOf({ status: 0 })).toBe('Source-Connector not reachable');
  });
});
