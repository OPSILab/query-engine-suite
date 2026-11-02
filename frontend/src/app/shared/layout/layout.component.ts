import { Component, HostListener } from '@angular/core';

const COLLAPSED_KEY = 'ds-sidenav-collapsed';

/**
 * The frame of every page: the bar at the top (app-topbar, with the page's controls: the content marked
 * ngProjectAs="[topbar]") and, below it, the side menu on the left (query engine, status log) next to the page.
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

  readonly items = [
    {
      link: '/', label: 'queryEngine',
      icon: 'M11 18a7 7 0 1 1 0-14 7 7 0 0 1 0 14zM21 21l-4.3-4.3',
    },
    {
      link: '/status-log', label: 'Status log',
      icon: 'M3 12h4l3-8 4 16 3-8h4',
    },
  ];
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
