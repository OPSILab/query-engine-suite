import { afterEach, describe, expect, it } from 'vitest';
import { of } from 'rxjs';
import { LayoutComponent } from './layout.component';

describe('LayoutComponent', () => {
  afterEach(() => localStorage.clear());

  it('the side menu reduced to its icons: remembered per browser', () => {
    const layout = new LayoutComponent();
    expect(layout.collapsed).toBe(false);
    layout.toggleCollapsed();
    expect(layout.collapsed).toBe(true);
    expect(new LayoutComponent().collapsed).toBe(true);
    layout.toggleCollapsed();
    expect(new LayoutComponent().collapsed).toBe(false);
  });

  it('the drawer of the phones: opened by the ☰ button, closed by Esc or a choice', () => {
    const layout = new LayoutComponent();
    layout.toggleDrawer();
    expect(layout.drawerOpen).toBe(true);
    layout.onEscape();
    expect(layout.drawerOpen).toBe(false);
    layout.toggleDrawer();
    layout.closeDrawer();
    expect(layout.drawerOpen).toBe(false);
  });

  it('the query engine (also with link filters) and the status log; source connector and cache only logged in', () => {
    const shown = (loggedIn: boolean) => {
      let links: string[] = [];
      new LayoutComponent({ loggedIn$: of(loggedIn) } as any).items$.subscribe(items => links = items.map(i => i.link));
      return links;
    };
    expect(shown(false)).toEqual(['/', '/status-log']);
    expect(shown(true)).toEqual(['/', '/status-log', '/source-connector', '/cache']);
    expect(new LayoutComponent().activeOptions.queryParams).toBe('ignored');
  });
});
