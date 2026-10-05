import { Component } from '@angular/core';

const COLLAPSED_KEY = 'data-space-intro-collapsed';

/**
 * The "what is this tool" card at the top of the home page: an accordion
 * whose header (title + one-line tagline) is always visible and whose panel
 * describes the Data Space, the four query modes and the visibility selector.
 * Purely informational - the query engine's own tabs are the one place to
 * switch mode.
 */
@Component({
  selector: 'app-intro',
  templateUrl: './intro.component.html',
  styleUrls: ['./intro.component.scss'],
  standalone: false
})
export class IntroComponent {
  // `mode` is also the translation key of the name, same as the query
  // engine's tabs; `text` is the description's key; `icon` is the `d` of a
  // 24x24 stroke path.
  readonly modes = [
    { mode: 'Simple search', text: 'Intro mode simple', icon: 'M11 18a7 7 0 1 1 0-14 7 7 0 0 1 0 14zM21 21l-4.3-4.3' },
    { mode: 'Advanced search', text: 'Intro mode advanced', icon: 'M4 7h9M17 7h3M4 17h3M11 17h9M15 5v4M9 15v4' },
    { mode: 'Query SQL', text: 'Intro mode sql', icon: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3' },
    { mode: 'Query GraphQL', text: 'Intro mode graphql', icon: 'M12 3l8 4.5v9L12 21l-8-4.5v-9zM12 3v18M4 7.5l16 9M20 7.5l-16 9' },
  ];

  // Open until the user collapses it, then the choice is remembered per
  // browser. localStorage can be unavailable (privacy mode, blocked
  // storage) - then it just starts open.
  open = !IntroComponent.readCollapsed();

  toggle(): void {
    this.open = !this.open;
    try {
      if (this.open) {
        localStorage.removeItem(COLLAPSED_KEY);
      } else {
        localStorage.setItem(COLLAPSED_KEY, '1');
      }
    } catch {
      // not persisted - fine
    }
  }

  private static readCollapsed(): boolean {
    try {
      return localStorage.getItem(COLLAPSED_KEY) === '1';
    } catch {
      return false;
    }
  }
}
