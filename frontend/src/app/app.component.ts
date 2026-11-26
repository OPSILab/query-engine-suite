import { Component, ChangeDetectionStrategy } from '@angular/core';
import { TranslateService } from '@ngx-translate/core';
import { ThemeService } from './services/theme.service';

/**
 * The root of the app: the pages and the toasts. The Keycloak login is configured in app.module.ts (provideAuth,
 * from assets/config.json) and checked by AuthService when a page first needs it.
 */
@Component({
  selector: 'app-root',
  // Toasts, tooltips, the key/value autocomplete and the date filter are plain DOM (ToastContainerComponent,
  // TooltipDirective, AutocompleteComponent, the native <input type="datetime-local">): no UI library, no overlay
  // container to anchor here.
  template: '<router-outlet></router-outlet><ds-toast-container></ds-toast-container>',
  changeDetection: ChangeDetectionStrategy.Eager,
  standalone: false
})
export class AppComponent {

  constructor(
    private translate: TranslateService,
    // Injected here (rather than only in HomeComponent, which also uses
    // it for the toggle button) so the theme attribute is applied as
    // early as possible on boot instead of only once HomeComponent mounts.
    themeService: ThemeService) {
    this.translate.setDefaultLang('en');
    this.translate.use('en');
  }
}
