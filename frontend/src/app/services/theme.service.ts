import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

const STORAGE_KEY = 'data-space-theme';

/**
 * Light/dark toggle for the redesigned UI. Applies `data-ds-theme` on
 * <html> (consumed by the CSS custom properties in styles.scss) and
 * persists the choice - falls back to the OS preference the first time,
 * same as the design mockup.
 */
@Injectable({ providedIn: 'root' })
export class ThemeService {

  private darkSubject = new BehaviorSubject<boolean>(this.readInitial());
  dark$ = this.darkSubject.asObservable();

  constructor() {
    this.apply(this.darkSubject.value);
  }

  get dark(): boolean {
    return this.darkSubject.value;
  }

  toggle(): void {
    this.set(!this.darkSubject.value);
  }

  set(dark: boolean): void {
    this.darkSubject.next(dark);
    this.apply(dark);
    try {
      localStorage.setItem(STORAGE_KEY, dark ? 'dark' : 'light');
    } catch {
      // localStorage can be unavailable (privacy mode, old browsers, ...) -
      // the theme just won't persist across reloads.
    }
  }

  private readInitial(): boolean {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored === 'dark') return true;
      if (stored === 'light') return false;
    } catch {
      // ignore
    }
    return typeof window !== 'undefined' && !!window.matchMedia
      ? window.matchMedia('(prefers-color-scheme: dark)').matches
      : false;
  }

  private apply(dark: boolean): void {
    if (typeof document === 'undefined') return;
    document.documentElement.setAttribute('data-ds-theme', dark ? 'dark' : 'light');
  }
}
