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
  // <nb-layout> is invisible on purpose (see the .ds-overlay-host rule in
  // styles.scss): Nebular's NbOverlayContainerAdapter._createContainer()
  // appends the CDK overlay container (used by NbToastrService, popovers,
  // the datepicker, ...) as a child of *whatever <nb-layout> element last
  // called setContainer() on it - if none exists anywhere in the app, that
  // adapter's `this.container` stays undefined and every toast/popover
  // throws `Cannot read properties of undefined (reading 'appendChild')`.
  // The redesigned HomeComponent template dropped the <nb-layout> wrapper
  // it used to render (it's all custom .ds-* markup now), so it's declared
  // once here at the root instead - present on every route, empty (no
  // projected header/sidebar/column/footer content), and zero-sized so it
  // never affects visible layout; only the overlay container it hosts is
  // meant to be seen.
  template: '<nb-layout class="ds-overlay-host" aria-hidden="true"></nb-layout><router-outlet></router-outlet>',
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
