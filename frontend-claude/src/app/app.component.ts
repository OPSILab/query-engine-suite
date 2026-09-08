import { Component } from '@angular/core';
import { NbAuthService, NbOAuth2AuthStrategy, NbOAuth2ClientAuthMethod, NbOAuth2GrantType, NbOAuth2ResponseType } from '@nebular/auth';
import { v4 as uuidv4 } from 'uuid';
import { environment } from '../environments/environment';
import { OidcJWTToken } from './auth/oidc';
import { ConfigService } from '@ngx-config/core';
import { TranslateService } from '@ngx-translate/core';

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
  template: '<router-outlet></router-outlet>',
})
export class AppComponent {

  constructor(
    authService: NbAuthService,
    oauthStrategy: NbOAuth2AuthStrategy,
    private configs: ConfigService,
    private translate: TranslateService) {

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
