import { Component, ViewChild } from '@angular/core';
import { QueryEngineComponent } from '../data-space/query-engine/query-engine.component';
import { ThemeService } from '../services/theme.service';

const INTRO_HIDDEN_KEY = 'data-space-intro-hidden';

// This used to be AppComponent's own content, before AppComponent became a
// bare <router-outlet> to make room for the /keycloak-auth routes.
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
  standalone: false
})
export class HomeComponent {
  // Same default the dashboard passes to <ngx-query-engine [visibility]="'private'">
  // on the "private" tab of the Data Space page. In the dashboard this comes
  // from which tab you're on (private/shared/public are 3 separate page
  // instances); here there's a single query engine, so it's a plain select.
  visibilities = ['private', 'shared', 'public'];
  visibility = this.visibilities[0];

  generalSharedBucketObjects: any[] = [];
  pilotSharedBucketObjects: any[] = [];
  userBucketObjects: any[] = [];
  extractedElements: any[] = [];

  // The intro card's mode chips drive the query engine's own tabs: they call
  // its setMode() and read its `mode`, so chips and tabs can't disagree.
  @ViewChild('queryEngine', { static: true }) queryEngine?: QueryEngineComponent;

  // `mode` must match QueryEngineComponent.modes exactly (it's also the
  // translation key, like the tabs); `icon` is the `d` of a 24x24 stroke path.
  introModes = [
    { mode: 'Simple search', hint: 'Intro hint simple', icon: 'M11 18a7 7 0 1 1 0-14 7 7 0 0 1 0 14zM21 21l-4.3-4.3' },
    { mode: 'Advanced search', hint: 'Intro hint advanced', icon: 'M4 7h9M17 7h3M4 17h3M11 17h9M15 5v4M9 15v4' },
    { mode: 'Query SQL', hint: 'Intro hint sql', icon: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3' },
    { mode: 'Query GraphQL', hint: 'Intro hint graphql', icon: 'M12 3l8 4.5v9L12 21l-8-4.5v-9zM12 3v18M4 7.5l16 9M20 7.5l-16 9' },
  ];

  // Shown until dismissed; the choice is remembered per browser. localStorage
  // can be unavailable (privacy mode, blocked storage) - then it just shows.
  showIntro = HomeComponent.readIntroVisible();

  // public: read directly from the template (theme.dark, theme.toggle()).
  constructor(public theme: ThemeService) {}

  setIntroVisible(visible: boolean): void {
    this.showIntro = visible;
    try {
      if (visible) {
        localStorage.removeItem(INTRO_HIDDEN_KEY);
      } else {
        localStorage.setItem(INTRO_HIDDEN_KEY, '1');
      }
    } catch {
      // not persisted - fine
    }
  }

  private static readIntroVisible(): boolean {
    try {
      return localStorage.getItem(INTRO_HIDDEN_KEY) !== '1';
    } catch {
      return true;
    }
  }

  formatSize(bytes: number): string {
    if (bytes === null || bytes === undefined || isNaN(bytes)) return '';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
}
