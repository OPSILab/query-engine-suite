import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

const STORAGE_KEY = 'data-space-theme';

export type ThemeId = 'light' | 'dark' | 'graphite' | 'sky' | 'navy' | 'sage' | 'olive';

export interface ThemeOption {
  id: ThemeId;
  /** Translation key of the name shown in the picker. */
  label: string;
  /** Dark background (light text): drives `dark` and the light/dark toggle. */
  dark: boolean;
}

/**
 * Every theme, in the order the picker lists them. The colours themselves
 * live in styles.scss, one `[data-ds-theme='<id>']` block per entry here.
 */
export const THEMES: ThemeOption[] = [
  { id: 'light', label: 'Theme light', dark: false },
  { id: 'dark', label: 'Theme dark', dark: true },
  { id: 'graphite', label: 'Theme graphite', dark: true },
  { id: 'sky', label: 'Theme sky', dark: false },
  { id: 'navy', label: 'Theme navy', dark: true },
  { id: 'sage', label: 'Theme sage', dark: false },
  { id: 'olive', label: 'Theme olive', dark: true },
];

/**
 * Theme of the redesigned UI. Applies `data-ds-theme` on <html> (consumed by
 * the CSS custom properties in styles.scss) and persists the choice - falls
 * back to the OS light/dark preference the first time. The values stored by
 * the old light/dark-only toggle ('light' / 'dark') are still valid ids.
 */
@Injectable({ providedIn: 'root' })
export class ThemeService {

  private themeSubject = new BehaviorSubject<ThemeId>(this.readInitial());
  theme$ = this.themeSubject.asObservable();

  constructor() {
    this.apply(this.themeSubject.value);
  }

  get theme(): ThemeId {
    return this.themeSubject.value;
  }

  get current(): ThemeOption {
    return THEMES.find(t => t.id === this.themeSubject.value) || THEMES[0];
  }

  get dark(): boolean {
    return this.current.dark;
  }

  /** Light <-> dark, as the original toggle did. */
  toggle(): void {
    this.set(this.dark ? 'light' : 'dark');
  }

  set(id: ThemeId): void {
    this.themeSubject.next(id);
    this.apply(id);
    try {
      localStorage.setItem(STORAGE_KEY, id);
    } catch {
      // localStorage can be unavailable (privacy mode, old browsers, ...) -
      // the theme just won't persist across reloads.
    }
  }

  private readInitial(): ThemeId {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      const known = THEMES.find(t => t.id === stored);
      if (known) return known.id;
    } catch {
      // ignore
    }
    return typeof window !== 'undefined' && !!window.matchMedia
      && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  private apply(id: ThemeId): void {
    if (typeof document === 'undefined') return;
    document.documentElement.setAttribute('data-ds-theme', id);
  }
}
