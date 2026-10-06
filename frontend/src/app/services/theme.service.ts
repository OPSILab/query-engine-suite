import { Injectable, inject } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { ConfigService } from './config.service';

const STORAGE_KEY = 'data-space-theme';

export type ThemeId = 'light' | 'dark' | 'graphite' | 'sky' | 'navy' | 'sage' | 'olive'
  | 'dusk' | 'halo' | 'forest' | 'sunset' | 'cosmos' | 'smartera';

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
  // Gradient themes (background image, see styles.scss)
  { id: 'dusk', label: 'Theme dusk', dark: true },
  { id: 'halo', label: 'Theme halo', dark: false },
  { id: 'forest', label: 'Theme forest', dark: true },
  { id: 'sunset', label: 'Theme sunset', dark: true },
  { id: 'cosmos', label: 'Theme cosmos', dark: true },
  // Project theme (SMART ERA platform colours; also styles the topbar and the query panel)
  { id: 'smartera', label: 'Theme smartera', dark: false },
];

/**
 * Theme of the redesigned UI. Applies `data-ds-theme` on <html> (consumed by
 * the CSS custom properties in styles.scss) and persists the choice. Initial
 * theme: the user's saved choice, else `defaultTheme` from assets/config.json
 * (a theme id, or "system"), else the OS light/dark preference. The values
 * stored by the old light/dark-only toggle ('light' / 'dark') are still valid ids.
 */
@Injectable({ providedIn: 'root' })
export class ThemeService {

  // Declared before themeSubject: readInitial() (its initializer) reads it. Config is loaded by an app
  // initializer, i.e. before AppComponent - the first injector of this service - is created.
  private configs = inject(ConfigService);
  private themeSubject = new BehaviorSubject<ThemeId>(this.readInitial());
  theme$ = this.themeSubject.asObservable();

  constructor() {
    this.apply(this.themeSubject.value);
    this.applyFontScale();
  }

  /** "fontScale" in assets/config.json (e.g. 1.5) -> --ds-font-scale on <html>; absent/invalid = 1. */
  private applyFontScale(): void {
    if (typeof document === 'undefined') return;
    let scale: any;
    try {
      scale = this.configs.getSettings('fontScale', 1);
    } catch {
      scale = 1;
    }
    scale = Number(scale);
    if (!(scale > 0)) scale = 1;
    document.documentElement.style.setProperty('--ds-font-scale', String(scale));
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
    let configured: string | undefined;
    try {
      configured = this.configs.getSettings('defaultTheme', '');
    } catch {
      // config not loaded: fall back to the OS preference
    }
    const configuredTheme = THEMES.find(t => t.id === configured);
    if (configuredTheme) return configuredTheme.id;
    return typeof window !== 'undefined' && !!window.matchMedia
      && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  private apply(id: ThemeId): void {
    if (typeof document === 'undefined') return;
    document.documentElement.setAttribute('data-ds-theme', id);
  }
}
