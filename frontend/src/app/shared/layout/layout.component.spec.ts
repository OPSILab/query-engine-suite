import { afterEach, describe, expect, it } from 'vitest';
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

  it('two entries: the query engine (also with link filters) and the status log', () => {
    const layout = new LayoutComponent();
    expect(layout.items.map(i => i.link)).toEqual(['/', '/status-log']);
    expect(layout.activeOptions.queryParams).toBe('ignored');
  });
});
