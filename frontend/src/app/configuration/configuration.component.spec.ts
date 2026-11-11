import { describe, expect, it } from 'vitest';
import { ConfigurationComponent } from './configuration.component';

const DAY = 24 * 3600 * 1000;

describe('ConfigurationComponent', () => {
  it('without sourceConnectorBaseUrl: nothing asked', () => {
    const api: any = { baseUrl: '', getConfiguration: async () => { throw new Error('not expected'); } };
    const component = new ConfigurationComponent(api);
    component.ngOnInit();
    expect(component.configured).toBe(false);
    expect(component.configuration).toBeNull();
  });

  it('the configuration of the Source-Connector; an error shown with its text', async () => {
    const configuration = {
      sources: { minio: false, orion: true, api: true }, pollInterval: DAY,
      apis: [{ name: 'KPIs', url: 'https://kpi', method: 'GET', paginated: false, incremental: true, startDateValue: '2026-10-01', maxWindow: DAY }],
    };
    const api: any = { baseUrl: 'http://sc', getConfiguration: async () => configuration };
    const component = new ConfigurationComponent(api);
    await component.load();
    expect(component.configuration).toEqual(configuration);
    expect(component.incrementalCount).toBe(1);
    api.getConfiguration = async () => { throw { status: 0 }; };
    await component.load();
    expect(component.error).toBe('Source-Connector not reachable');
  });

  it('durations and kinds', () => {
    expect([DAY, 30 * DAY, 6 * 3600 * 1000, 15 * 60 * 1000, 1500, 0, null].map(ms => ConfigurationComponent.duration(ms)))
      .toEqual([
        { key: 'One day', params: { count: 1 } }, { key: 'Days count', params: { count: 30 } }, { key: 'Hours count', params: { count: 6 } },
        { key: 'Minutes count', params: { count: 15 } }, { key: 'Milliseconds count', params: { count: 1500 } }, null, null,
      ]);
    expect(ConfigurationComponent.kind({ method: 'GET', paginated: true, incremental: false })).toBe('Paginated');
    expect(ConfigurationComponent.kind({ method: 'GET', paginated: false, batch: { from: 'x' }, incremental: false })).toBe('Batch');
    expect(ConfigurationComponent.kind({ method: 'GET', paginated: false, incremental: false })).toBe('Single request');
  });
});
