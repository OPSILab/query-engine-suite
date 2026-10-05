import { AfterViewInit, Component, ElementRef, HostListener, Injector, NgZone, OnDestroy, ViewChild, afterNextRender } from '@angular/core';
import { ThemeService } from '../services/theme.service';

const RESULTS_VIEW_KEY = 'data-space-results-view';

// This used to be AppComponent's own content, before AppComponent became a
// bare <router-outlet> to make room for the /keycloak-auth routes.
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
  standalone: false
})
export class HomeComponent implements AfterViewInit, OnDestroy {
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

  // True while <ngx-query-engine> has a query in flight (its loadingChange
  // output): the results below fade out until the new ones arrive.
  searching = false;

  // The results start below the query panel, and with the intro open (or the
  // Advanced search filters) that's often just past the bottom edge of the
  // screen - a query "succeeds" and nothing seems to happen. Two remedies:
  // - when a query brings results and they're out of sight, the page scrolls
  //   down to them by itself (onSearchingChange);
  // - while there are results below the visible area, a floating pill says
  //   how many and scrolls to them on click (resultsBelow, kept up to date by
  //   an IntersectionObserver on the results area).
  @ViewChild('resultsArea', { static: true }) resultsArea!: ElementRef<HTMLElement>;
  resultsBelow = false;
  // The query engine's action row is floating at the bottom of the window
  // (see QueryEngineComponent.actionsStuck): the pill moves up above it.
  actionsStuck = false;
  // Set when the query engine hands over results (they're emitted before
  // loadingChange(false)), so a cancelled or failed query - which emits
  // nothing - doesn't scroll to the previous query's results.
  private resultsArrived = false;
  private resultsObserver?: IntersectionObserver;

  // How "Matched elements" are shown: as tables (JsonTableComponent) or as
  // the raw JSON. Remembered per browser; tables unless chosen otherwise.
  resultsView: 'table' | 'json' = HomeComponent.readResultsView();
  // The card whose "Copy JSON" was just clicked, for the "Copied" feedback.
  copiedElement: any = null;
  private copiedTimer?: ReturnType<typeof setTimeout>;

  // The result shown full screen, if any. Uses the browser's Fullscreen API
  // on the card; where that isn't available (iPhone Safari) or is refused,
  // the card is instead laid over the whole window with CSS
  // (.ds-element-card--max) and Esc closes it.
  fullscreenElement: any = null;
  private fullscreenFallback = false;

  get resultCount(): number {
    return (this.extractedElements?.length || 0)
      + (this.userBucketObjects?.length || 0)
      + (this.pilotSharedBucketObjects?.length || 0)
      + (this.generalSharedBucketObjects?.length || 0);
  }

  // public: read directly from the template (theme.dark, theme.toggle()).
  constructor(public theme: ThemeService, private zone: NgZone, private injector: Injector) {}

  ngAfterViewInit(): void {
    if (typeof IntersectionObserver === 'undefined') {
      return;
    }
    // "Below" = the top of the results area is under the visible part of the
    // window, minus the bottom 96px where the pill itself sits: a results
    // header peeking out from under the pill doesn't count as seen.
    // One element observed, but a batch can hold several entries for it
    // (e.g. the layout settling during load): only the last one is current.
    this.resultsObserver = new IntersectionObserver(entries => {
      const entry = entries[entries.length - 1];
      const below = !entry.isIntersecting && entry.boundingClientRect.top > (entry.rootBounds?.bottom ?? window.innerHeight);
      if (below !== this.resultsBelow) {
        this.zone.run(() => this.resultsBelow = below);
      }
    }, { rootMargin: '0px 0px -96px 0px' });
    this.resultsObserver.observe(this.resultsArea.nativeElement);
  }

  ngOnDestroy(): void {
    clearTimeout(this.copiedTimer);
    this.resultsObserver?.disconnect();
  }

  onResults(): void {
    this.resultsArrived = true;
  }

  onSearchingChange(searching: boolean): void {
    this.searching = searching;
    if (searching) {
      this.resultsArrived = false;
      return;
    }
    if (!this.resultsArrived || !this.resultCount) {
      return;
    }
    // Wait for the new results to be rendered before measuring/scrolling.
    afterNextRender({
      read: () => {
        const top = this.resultsArea.nativeElement.getBoundingClientRect().top;
        if (top > window.innerHeight - 140) {
          this.scrollToResults();
        }
      },
    }, { injector: this.injector });
  }

  scrollToResults(): void {
    const reduce = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
    // scroll-margin-top on .ds-results-area (see the scss) leaves the query
    // panel's buttons in view above the results, so it's clear what they
    // are the answer to.
    this.resultsArea.nativeElement.scrollIntoView({ behavior: reduce ? 'auto' : 'smooth', block: 'start' });
  }

  setResultsView(view: 'table' | 'json'): void {
    this.resultsView = view;
    try {
      localStorage.setItem(RESULTS_VIEW_KEY, view);
    } catch {
      // not persisted - fine
    }
  }

  private static readResultsView(): 'table' | 'json' {
    try {
      return localStorage.getItem(RESULTS_VIEW_KEY) === 'json' ? 'json' : 'table';
    } catch {
      return 'table';
    }
  }

  async copyElement(el: { element: any }): Promise<void> {
    const v = el.element;
    const text = typeof v === 'string' ? v : JSON.stringify(v, null, 2);
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // navigator.clipboard only exists in secure contexts (https or
      // localhost): over plain http fall back to the old execCommand way.
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
    this.copiedElement = el;
    clearTimeout(this.copiedTimer);
    this.copiedTimer = setTimeout(() => this.copiedElement = null, 1600);
  }

  toggleFullscreen(card: HTMLElement, el: any): void {
    if (this.fullscreenElement === el) {
      this.exitFullscreen();
      return;
    }
    this.fullscreenElement = el;
    this.fullscreenFallback = !card.requestFullscreen;
    card.requestFullscreen?.().catch(() => this.fullscreenFallback = true);
  }

  private exitFullscreen(): void {
    if (document.fullscreenElement) {
      // fullscreenchange (below) clears the state
      document.exitFullscreen();
    } else {
      this.fullscreenElement = null;
      this.fullscreenFallback = false;
    }
  }

  // Leaving native full screen by any route: our button, Esc, F11, or the
  // card being removed because a new query replaced the results.
  @HostListener('document:fullscreenchange')
  onFullscreenChange(): void {
    if (!document.fullscreenElement && !this.fullscreenFallback) {
      this.fullscreenElement = null;
    }
  }

  @HostListener('document:keydown.escape')
  onEscape(): void {
    if (this.fullscreenFallback && this.fullscreenElement) {
      this.exitFullscreen();
    }
  }

  formatSize(bytes: number): string {
    if (bytes === null || bytes === undefined || isNaN(bytes)) return '';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
}
