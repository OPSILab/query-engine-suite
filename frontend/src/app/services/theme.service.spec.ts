import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Injector, runInInjectionContext } from '@angular/core';
import { ConfigService } from './config.service';
import { THEMES, ThemeService } from './theme.service';

const STORAGE_KEY = 'data-space-theme';

function create(settings: Record<string, any> = {}, { configThrows = false } = {}) {
  const config = {
    getSettings: (key: string, defaultValue?: any) => {
      if (configThrows) throw new Error('config not loaded');
      return settings[key] !== undefined ? settings[key] : defaultValue;
    },
  };
  const injector = Injector.create({ providers: [{ provide: ConfigService, useValue: config }] });
  return runInInjectionContext(injector, () => new ThemeService());
}

function prefersDark(dark: boolean) {
  vi.spyOn(window, 'matchMedia').mockImplementation((query: string) =>
    ({ matches: dark && query.includes('dark'), media: query } as any));
}

const html = () => document.documentElement;

beforeEach(() => {
  localStorage.clear();
  html().removeAttribute('data-ds-theme');
  html().style.removeProperty('--ds-font-scale');
  prefersDark(false);
});
afterEach(() => localStorage.clear());

describe('initial theme', () => {
  it('the saved choice wins over config and OS', () => {
    localStorage.setItem(STORAGE_KEY, 'forest');
    prefersDark(true);
    expect(create({ defaultTheme: 'halo' }).theme).toBe('forest');
  });

  it('an unknown saved value is ignored', () => {
    localStorage.setItem(STORAGE_KEY, 'neon');
    expect(create({ defaultTheme: 'cosmos' }).theme).toBe('cosmos');
  });

  it('defaultTheme from config.json', () => {
    expect(create({ defaultTheme: 'smartera' }).theme).toBe('smartera');
  });

  it('"system" (or nothing, or config not loaded) follows the OS preference', () => {
    prefersDark(true);
    expect(create({ defaultTheme: 'system' }).theme).toBe('dark');
    expect(create({}).theme).toBe('dark');
    expect(create({}, { configThrows: true }).theme).toBe('dark');
    prefersDark(false);
    expect(create({ defaultTheme: 'system' }).theme).toBe('light');
  });

  it('is applied on <html>', () => {
    create({ defaultTheme: 'sunset' });
    expect(html().getAttribute('data-ds-theme')).toBe('sunset');
  });
});

describe('set / toggle', () => {
  it('set applies, persists and emits', () => {
    const service = create();
    const seen: string[] = [];
    service.theme$.subscribe(t => seen.push(t));
    service.set('navy');
    expect(html().getAttribute('data-ds-theme')).toBe('navy');
    expect(localStorage.getItem(STORAGE_KEY)).toBe('navy');
    expect(seen).toEqual(['light', 'navy']);
    expect(service.dark).toBe(true);
    expect(service.current.label).toBe('Theme navy');
  });

  it('toggle goes light <-> dark, from any theme by its darkness', () => {
    const service = create({ defaultTheme: 'halo' }); // light theme
    service.toggle();
    expect(service.theme).toBe('dark');
    service.set('cosmos'); // dark theme
    service.toggle();
    expect(service.theme).toBe('light');
  });

  it('works without localStorage', () => {
    const service = create();
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('denied'); });
    expect(() => service.set('olive')).not.toThrow();
    expect(service.theme).toBe('olive');
  });
});

describe('font scale', () => {
  it('fontScale from config.json -> --ds-font-scale', () => {
    create({ fontScale: 1.15 });
    expect(html().style.getPropertyValue('--ds-font-scale')).toBe('1.15');
  });

  it('missing, invalid, zero or negative -> 1', () => {
    for (const fontScale of [undefined, 'big', 0, -2]) {
      create({ fontScale });
      expect(html().style.getPropertyValue('--ds-font-scale')).toBe('1');
    }
  });
});

describe('THEMES', () => {
  it('unique ids, every theme has a translation label', () => {
    const ids = THEMES.map(t => t.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(THEMES.every(t => t.label.startsWith('Theme '))).toBe(true);
  });
});
