import { afterEach, describe, expect, it } from 'vitest';
import { SiteNoticesComponent } from './site-notices.component';
import { NoticesService } from '../../services/notices.service';

const withGlobal = (global: any) => new NoticesService({ getSettings: (k: string, d: any) => (k === 'notices' ? { global } : d) } as any, { currentLang: 'en' } as any);

describe('SiteNoticesComponent', () => {
  afterEach(() => localStorage.clear());

  it('nothing when there is no global notice (the bar is not shown)', () => {
    expect(new SiteNoticesComponent(withGlobal(undefined)).shown).toEqual([]);
    expect(new SiteNoticesComponent(withGlobal([])).shown).toEqual([]);
  });

  it('closed by the visitor: gone, also on the next visit, until its text changes', () => {
    const component = new SiteNoticesComponent(withGlobal(['Maintenance on Saturday', 'Another']));
    expect(component.shown.map(n => n.text)).toEqual(['Maintenance on Saturday', 'Another']);
    component.close(component.shown[0]);
    expect(component.shown.map(n => n.text)).toEqual(['Another']);
    expect(new SiteNoticesComponent(withGlobal(['Maintenance on Saturday', 'Another'])).shown.map(n => n.text)).toEqual(['Another']);
    expect(new SiteNoticesComponent(withGlobal(['Maintenance on Sunday'])).shown.map(n => n.text)).toEqual(['Maintenance on Sunday']);
  });
});
