import { Component, HostListener, Optional } from '@angular/core';
import { Observable, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { SessionService } from '../../services/session.service';

const COLLAPSED_KEY = 'ds-sidenav-collapsed';

/**
 * The frame of every page: the bar at the top (app-topbar, with the page's controls: the content marked
 * ngProjectAs="[topbar]") and, below it, the side menu on the left (query engine, status log, configuration; source connector
 * and cache only for logged users) next to the page.
 * On desktop the menu can be reduced to its icons (remembered per browser); on phones it is a drawer opened by the
 * ☰ button of the bar, closed by a choice, a click outside or Esc. Styles: global (styles.scss, .ds-layout).
 */
@Component({
  selector: 'app-layout',
  templateUrl: './layout.component.html',
  standalone: false
})
export class LayoutComponent {
  collapsed = LayoutComponent.readCollapsed();
  drawerOpen = false;

  static readonly ITEMS: { link: string; label: string; icon: string; loggedInOnly?: boolean }[] = [
    {
      link: '/', label: 'queryEngine',
      icon: 'M11 18a7 7 0 1 1 0-14 7 7 0 0 1 0 14zM21 21l-4.3-4.3',
    },
    {
      link: '/status-log', label: 'Status log',
      icon: 'M3 12h4l3-8 4 16 3-8h4',
    },
    {
      link: '/configuration', label: 'Configuration',
      icon: 'M4 6h10M18 6h2M4 12h4M12 12h8M4 18h12M20 18h0M14 4v4M8 10v4M16 16v4',
    },
    {
      link: '/source-connector', label: 'Source connector', loggedInOnly: true,
      icon: 'M12 3v4M12 17v4M5 8h14v8H5zM8 12h.01M12 12h.01',
    },
    {
      link: '/cache', label: 'Cache', loggedInOnly: true,
      icon: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6',
    },
  ];

  /** The entries shown: those for logged users only when someone is logged in. */
  readonly items$: Observable<typeof LayoutComponent.ITEMS>;

  constructor(@Optional() session?: SessionService) {
    const loggedIn$ = session ? session.loggedIn$ : of(false);
    this.items$ = loggedIn$.pipe(map(loggedIn => LayoutComponent.ITEMS.filter(item => !item.loggedInOnly || loggedIn)));
  }
  // the query engine is "/" also with the filters of a link (?key=...&value=...)
  readonly activeOptions = { paths: 'exact', queryParams: 'ignored', matrixParams: 'ignored', fragment: 'ignored' } as const;

  toggleCollapsed(): void {
    this.collapsed = !this.collapsed;
    try {
      if (this.collapsed) localStorage.setItem(COLLAPSED_KEY, '1');
      else localStorage.removeItem(COLLAPSED_KEY);
    } catch {
      // not remembered: fine
    }
  }

  toggleDrawer(): void {
    this.drawerOpen = !this.drawerOpen;
  }

  closeDrawer(): void {
    this.drawerOpen = false;
  }

  @HostListener('document:keydown.escape')
  onEscape(): void {
    this.closeDrawer();
  }

  private static readCollapsed(): boolean {
    try {
      return localStorage.getItem(COLLAPSED_KEY) === '1';
    } catch {
      return false;
    }
  }
}
