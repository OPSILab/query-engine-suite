import { Injectable } from '@angular/core';
import { TranslateService } from '@ngx-translate/core';
import { ConfigService } from './config.service';

/** Where a notice of config.json "notices" is shown. */
export type NoticeSection = 'global' | 'simpleSearch' | 'advancedSearch' | 'sqlQuery' | 'graphqlQuery';

/** A notice ready to show: its text (and link) in the language of the site. */
export interface SiteNotice {
  /** Stable for the same level and text: a notice closed by the visitor stays closed until its text changes. */
  key: string;
  level: 'info' | 'warning';
  text: string;
  link?: { url: string; text: string };
}

/** The modes of the query card and their section of "notices". */
export const MODE_NOTICES: Record<string, NoticeSection> = {
  'Simple search': 'simpleSearch',
  'Advanced search': 'advancedSearch',
  'Query SQL': 'sqlQuery',
  'Query GraphQL': 'graphqlQuery',
};

/**
 * The notices of config.json "notices", written by whoever runs the site for its visitors:
 *
 *   "notices": {
 *     "global": [ ... ],            a bar under the header, on every page (only when one is active)
 *     "simpleSearch": [ ... ],      in the query card, in yellow, after the notices the mode already shows
 *     "advancedSearch": [ ... ], "sqlQuery": [ ... ], "graphqlQuery": [ ... ]
 *   }
 *
 * A notice is a text, or { text, level?, from?, to?, link? }: text a string or { "en": "...", "it": "..." } (the
 * language of the site, else English, else the first one); level "info" or "warning" (default; only the global bar
 * shows it); from / to the period it is shown in (ISO dates; a date alone: to includes that whole day); link
 * { url, text? } (text as text, default the url). A notice without text, or with a bad date, is left out.
 */
@Injectable({ providedIn: 'root' })
export class NoticesService {
  constructor(private config: ConfigService, private translate: TranslateService) { }

  /** The notices of a section shown now, in their order. */
  notices(section: NoticeSection, now = new Date()): SiteNotice[] {
    let all: any;
    try {
      all = this.config.getSettings('notices', {});
    } catch {
      all = {};
    }
    const list = all && typeof all === 'object' && Array.isArray(all[section]) ? all[section] : [];
    const lang = this.translate?.currentLang || this.translate?.defaultLang || 'en';
    return list.map((item: any) => NoticesService.resolve(item, lang, now)).filter((n: SiteNotice | null): n is SiteNotice => !!n);
  }

  /** A notice of the configuration in the language given, or null (no text, outside its period, bad date). */
  static resolve(item: any, lang: string, now: Date): SiteNotice | null {
    const notice = typeof item === 'string' ? { text: item } : item;
    if (!notice || typeof notice !== 'object') return null;
    const text = NoticesService.textOf(notice.text, lang);
    if (!text) return null;
    const from = NoticesService.dateOf(notice.from, false), to = NoticesService.dateOf(notice.to, true);
    if (from === undefined || to === undefined) return null;
    if ((from && now < from) || (to && now >= to)) return null;
    const level = notice.level === 'info' ? 'info' : 'warning';
    const url = typeof notice.link === 'string' ? notice.link : notice.link?.url;
    const link = typeof url === 'string' && /^https?:\/\//i.test(url)
      ? { url, text: NoticesService.textOf(typeof notice.link === 'object' ? notice.link.text : undefined, lang) || url }
      : undefined;
    return { key: NoticesService.keyOf(level + '|' + text + '|' + (link?.url ?? '')), level, text, ...(link ? { link } : {}) };
  }

  /** A text, or { lang: text }: the one of the language, else English, else the first one. */
  static textOf(value: any, lang: string): string {
    if (typeof value === 'string') return value.trim();
    if (!value || typeof value !== 'object') return '';
    const pick = value[lang] ?? value['en'] ?? Object.values(value).find(v => typeof v === 'string');
    return typeof pick === 'string' ? pick.trim() : '';
  }

  /** null: no limit; undefined: a bad date. A date alone (YYYY-MM-DD) is local midnight; as an end, the next one. */
  static dateOf(value: any, end: boolean): Date | null | undefined {
    if (value === undefined || value === null || value === '') return null;
    if (typeof value !== 'string') return undefined;
    const dayOnly = /^\d{4}-\d{2}-\d{2}$/.test(value);
    const date = new Date(dayOnly ? value + 'T00:00:00' : value);
    if (isNaN(date.getTime())) return undefined;
    if (dayOnly && end) date.setDate(date.getDate() + 1);
    return date;
  }

  private static keyOf(text: string): string {
    let hash = 5381;
    for (let i = 0; i < text.length; i++) hash = ((hash << 5) + hash + text.charCodeAt(i)) | 0;
    return (hash >>> 0).toString(36);
  }
}
