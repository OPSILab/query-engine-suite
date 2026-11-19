import { describe, expect, it } from 'vitest';
import { NoticesService } from './notices.service';

const service = (notices: any, lang = 'it') => new NoticesService(
  { getSettings: (key: string, def: any) => (key === 'notices' ? notices ?? def : def) } as any,
  { currentLang: lang, defaultLang: 'en' } as any,
);
const NOW = new Date('2026-11-20T12:00:00');

describe('NoticesService', () => {
  it('a section of config.json "notices": texts or objects, in order; another section, a missing one: none', () => {
    const s = service({ global: ['Manutenzione sabato', { text: 'Second', level: 'info' }], advancedSearch: ['A'] });
    expect(s.notices('global', NOW).map(n => [n.text, n.level])).toEqual([['Manutenzione sabato', 'warning'], ['Second', 'info']]);
    expect(s.notices('advancedSearch', NOW).map(n => n.text)).toEqual(['A']);
    expect(s.notices('sqlQuery', NOW)).toEqual([]);
    expect(service(undefined).notices('global', NOW)).toEqual([]);
    expect(service({ global: 'not a list' }).notices('global', NOW)).toEqual([]);
  });

  it('the text in the language of the site, else English, else the first one', () => {
    expect(NoticesService.textOf({ it: 'Ciao', en: 'Hi' }, 'it')).toBe('Ciao');
    expect(NoticesService.textOf({ it: 'Ciao', en: 'Hi' }, 'de')).toBe('Hi');
    expect(NoticesService.textOf({ fr: 'Salut' }, 'de')).toBe('Salut');
    expect(service({ global: [{ text: { it: 'Ciao', en: 'Hi' } }] }, 'en').notices('global', NOW)[0].text).toBe('Hi');
  });

  it('from / to: shown in its period only (a date alone: to includes that day); a bad date or no text: left out', () => {
    const at = (from?: string, to?: string) => NoticesService.resolve({ text: 'x', from, to }, 'it', NOW);
    expect(at('2026-11-20', '2026-11-20')).not.toBeNull();
    expect(at('2026-11-21')).toBeNull();
    expect(at(undefined, '2026-11-19')).toBeNull();
    expect(at(undefined, '2026-11-20T11:00:00')).toBeNull();
    expect(at('not a date')).toBeNull();
    expect(NoticesService.resolve({ text: '  ' }, 'it', NOW)).toBeNull();
    expect(NoticesService.resolve(null, 'it', NOW)).toBeNull();
  });

  it('link: http(s) only, its text or the url; the key changes with the text', () => {
    const n = NoticesService.resolve({ text: 'x', link: { url: 'https://status.example.org', text: { it: 'Dettagli' } } }, 'it', NOW)!;
    expect(n.link).toEqual({ url: 'https://status.example.org', text: 'Dettagli' });
    expect(NoticesService.resolve({ text: 'x', link: 'https://a.example' }, 'it', NOW)!.link).toEqual({ url: 'https://a.example', text: 'https://a.example' });
    expect(NoticesService.resolve({ text: 'x', link: 'javascript:alert(1)' }, 'it', NOW)!.link).toBeUndefined();
    expect(NoticesService.resolve('x', 'it', NOW)!.key).toBe(NoticesService.resolve({ text: 'x' }, 'it', NOW)!.key);
    expect(NoticesService.resolve('x', 'it', NOW)!.key).not.toBe(NoticesService.resolve('y', 'it', NOW)!.key);
  });
});
