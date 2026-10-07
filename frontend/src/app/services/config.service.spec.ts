import { describe, expect, it } from 'vitest';
import { of, throwError } from 'rxjs';
import { ConfigService } from './config.service';

async function loaded(settings: any) {
  const service = new ConfigService({ get: () => of(settings) } as any);
  await service.init('./assets/config.json');
  return service;
}

describe('ConfigService', () => {
  it('init loads the settings; no key returns them all', async () => {
    const service = await loaded({ a: 1 });
    expect(service.getSettings()).toEqual({ a: 1 });
    expect(service.getSettings([])).toEqual({ a: 1 });
  });

  it('dotted keys and arrays walk nested settings', async () => {
    const service = await loaded({ auth: { realm: 'r' } });
    expect(service.getSettings('auth.realm')).toBe('r');
    expect(service.getSettings(['auth', 'realm'])).toBe('r');
  });

  it('missing key: default value, or an error without one', async () => {
    const service = await loaded({ fontScale: 0 });
    expect(service.getSettings('fontScale', 1)).toBe(0); // falsy but present
    expect(service.getSettings('defaultTheme', 'system')).toBe('system');
    expect(service.getSettings('graphqlUrl', null)).toBe(null);
    expect(() => service.getSettings('missing')).toThrow(/missing/);
  });

  it('init rejects when the file can not be loaded', async () => {
    const service = new ConfigService({ get: () => throwError(() => new Error('404')) } as any);
    await expect(service.init()).rejects.toBe('Endpoint unreachable!');
  });
});
