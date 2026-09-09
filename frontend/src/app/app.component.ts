import { Component } from '@angular/core';
import { NbAuthService, NbOAuth2AuthStrategy, NbOAuth2ClientAuthMethod, NbOAuth2GrantType, NbOAuth2ResponseType } from '@nebular/auth';
import { v4 as uuidv4 } from 'uuid';
import { environment } from '../environments/environment';
import { OidcJWTToken } from './auth/oidc';
import { ConfigService } from '@ngx-config/core';
import { TranslateService } from '@ngx-translate/core';
import { ThemeService } from './services/theme.service';

/**
 * Adapted from the dashboard's AppComponent.
 *
 * Same runtime-patching pattern: the OAuth2 strategy is registered with
 * placeholder options in AppModule (see NbAuthModule.forRoot in
 * app.module.ts) and then given its real endpoints/redirect URIs here, once
 * ConfigService has loaded assets/config.json - that's the only way to
 * build URLs like `${dashboardBaseURL}/keycloak-auth/callback` from
 * runtime config instead of a value baked in at build time.
 */
@Component({
  selector: 'app-root',
  // This used to also render an invisible <nb-layout class="ds-overlay-host">
  // here, purely as an anchor for Nebular's CDK overlay container (used by
  // NbToastrService, nbPopover, nb-autocomplete and the datepicker) - without
  // one *somewhere* in the app, that overlay adapter's container reference
  // stayed null and every toast/popover/dropdown threw
  // `Cannot read properties of null (reading 'appendChild')`. Toasts,
  // tooltips, the key/value autocomplete and the date filter have all since
  // been rebuilt as plain DOM (see ToastContainerComponent, TooltipDirective,
  // AutocompleteComponent, and the native <input type="datetime-local"> in
  // query-engine.component.html) - nothing WE render uses Nebular's CDK
  // overlay anymore, so this anchor (and the bug class it existed to work
  // around) is gone. @nebular/theme itself is still imported in
  // app.module.ts, though - see the comment there on why NbThemeModule
  // turned out to be a real @nebular/auth runtime dependency, not just
  // overlay-anchor wiring.
  template: '<router-outlet></router-outlet><ds-toast-container></ds-toast-container>',
})
export class AppComponent {

  constructor(
    authService: NbAuthService,
    oauthStrategy: NbOAuth2AuthStrategy,
    private configs: ConfigService,
    private translate: TranslateService,
    // Injected here (rather than only in HomeComponent, which also uses
    // it for the toggle button) so the theme attribute is applied as
    // early as possible on boot instead of only once HomeComponent mounts.
    themeService: ThemeService) {

    oauthStrategy.setOptions({
      name: 'oidc',
      clientId: environment.keycloak.client_id,
      clientSecret: environment.keycloak.client_secret,
      baseEndpoint: `${this.configs.getSettings('keycloak.baseURL')}`,
      clientAuthMethod: NbOAuth2ClientAuthMethod.NONE,
      token: {
        endpoint: '/token',
        redirectUri: `${this.configs.getSettings('dashboardBaseURL')}/keycloak-auth/callback`,
        class: OidcJWTToken,
        key: 'access_token',
      },
      authorize: {
        endpoint: '/auth',
        scope: 'openid',
        state: uuidv4(),
        redirectUri: `${this.configs.getSettings('dashboardBaseURL')}/keycloak-auth/callback`,
        responseType: NbOAuth2ResponseType.CODE,
      },
      redirect: {
        success: '/',
        failure: null,
      },
      refresh: {
        endpoint: '/token',
        grantType: NbOAuth2GrantType.REFRESH_TOKEN,
        class: OidcJWTToken,
      },
    });

    this.translate.setDefaultLang('en');
    this.translate.use('en');
  }
}
