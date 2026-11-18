import { describe, expect, it } from 'vitest';
import { ApiBackupsComponent } from './api-backups.component';

function fakeApi(overrides: Record<string, any> = {}) {
  const calls: any[] = [];
  const api: any = {
    baseUrl: 'http://sc',
    getBackups: async () => ({
      items: [
        { id: 'bk_2', createdAt: '2026-11-18T10:00:00Z', by: 'bob', kind: 'before-restore', label: 'before restoring bk_1', counts: { records: 4, incrementalstate: 1 } },
        { id: 'bk_1', createdAt: '2026-11-17T10:00:00Z', by: 'anna', kind: 'manual', counts: { records: 3, postgres: 3 }, restoredAt: '2026-11-18T10:00:01Z', restoredBy: 'bob' },
      ],
      status: { running: false }, maxBackups: 10, secretConfigured: true,
    }),
    getBackupLog: async () => [],
    createBackup: async (label: string, secret: string) => { calls.push(['backup', label, secret]); return { started: true }; },
    restoreBackup: async (id: string, secret: string) => { calls.push(['restore', id, secret]); return { started: true, id }; },
    deleteBackup: async (id: string, secret: string) => { calls.push(['delete', id, secret]); return { id }; },
    ...overrides,
  };
  return { api, calls };
}

describe('ApiBackupsComponent', () => {
  it('the list, what each backup holds, the last restored', async () => {
    const { api } = fakeApi();
    const component = new ApiBackupsComponent(api);
    await component.refresh();
    expect(component.backups.map(b => b.id)).toEqual(['bk_2', 'bk_1']);
    expect(component.records(component.backups[1].counts)).toBe('MongoDB 3 · PostgreSQL 3');
    expect(component.lastRestored).toBe('bk_1');
    expect(component.maxBackups).toBe(10);
  });

  it('backup: only with the password; the label trimmed; the password not kept', async () => {
    const { api, calls } = fakeApi();
    const component = new ApiBackupsComponent(api);
    await component.refresh();
    component.label = '  weekly ';
    await component.backUp();
    expect(calls).toEqual([]);
    component.secret = 'pass';
    await component.backUp();
    expect(calls).toEqual([['backup', 'weekly', 'pass']]);
    expect([component.secret, component.label]).toEqual(['', '']);
  });

  it('restore and delete: after the confirmation; refused: the message, the confirmation kept', async () => {
    const { api, calls } = fakeApi();
    const component = new ApiBackupsComponent(api);
    await component.refresh();
    component.secret = 'pass';
    component.ask('restore', 'bk_1');
    expect(component.pending).toEqual({ action: 'restore', id: 'bk_1' });
    expect(calls).toEqual([]);
    await component.confirm();
    expect(calls).toEqual([['restore', 'bk_1', 'pass']]);
    expect(component.pending).toBeNull();
    api.deleteBackup = async () => { throw { status: 403, error: 'Wrong or missing management password (X-Management-Secret)' }; };
    component.secret = 'wrong';
    component.ask('delete', 'bk_2');
    await component.confirm();
    expect(component.actionError).toBe('Wrong or missing management password (X-Management-Secret)');
    expect(component.pending).toEqual({ action: 'delete', id: 'bk_2' });
  });

  it('nothing allowed while an action runs or without the password configured; read again while running', async () => {
    let running = true;
    const { api } = fakeApi({ getBackups: async () => ({ items: [], status: { running, action: 'backup' }, maxBackups: 0, secretConfigured: true }) });
    const component = new ApiBackupsComponent(api);
    await component.refresh();
    component.secret = 'pass';
    expect(component.canAct).toBe(false);
    running = false;
    await component.refresh();
    expect(component.canAct).toBe(true);
    component.secretConfigured = false;
    expect(component.canAct).toBe(false);
    component.ngOnDestroy();
  });
});
