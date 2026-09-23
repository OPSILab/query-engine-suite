import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgModule } from '@angular/core';
import { HttpClientModule, HttpClient, HTTP_INTERCEPTORS } from '@angular/common/http';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { NbAuthModule, NbOAuth2AuthStrategy } from '@nebular/auth';
// @nebular/auth (which stays) turns out to still need @nebular/theme at
// runtime, independent of anything our own code renders:
// NbOAuth2AuthStrategy injects NB_WINDOW directly, and - more surprisingly -
// NbAuthService.authenticate() (called by AuthLoginComponent) dynamically
// creates an NbLayoutComponent instance of its own via
// ComponentFactoryResolver while the redirect is in flight, whose
// constructor injects NbThemeService. Without NbThemeModule.forRoot()
// providing both, that redirect throws `NullInjectorError: No provider for
// NbThemeService!` the moment a real login happens - a real dependency, not
// leftover CDK-overlay-anchor wiring, so this stays even though our own
// <nb-layout> (see app.component.ts) and every nb-* component we used to
// render are gone. (This also finally explains an old open question from
// earlier in this project: the Keycloak login redirect intermittently
// leaving NbOverlayContainerAdapter's container null after a real login -
// that dynamically-created NbLayoutComponent registers itself as the CDK
// overlay container and unregisters when it's torn down, stealing the
// registration our own anchor held. Moot now: nothing we render uses that
// overlay anymore, see AutocompleteComponent/TooltipDirective/
// ToastContainerComponent and the native date input.)
import { NbThemeModule } from '@nebular/theme';
// Same story as NbThemeModule right above: NbOAuth2AuthStrategy's redirect
// renders an <nb-icon> (as part of the same internally-created
// NbLayoutComponent/spinner) that throws "Default pack is not registered."
// without an icon pack registered - eva-icons is the pack the dashboard
// this was ported from uses everywhere else.
import { NbEvaIconsModule } from '@nebular/eva-icons';

import { ConfigHttpLoader } from '@ngx-config/http-loader';
import { ConfigModule, ConfigLoader } from '@ngx-config/core';
import { TranslateModule, TranslateLoader } from '@ngx-translate/core';
import { TranslateHttpLoader } from '@ngx-translate/http-loader';

import { AppComponent } from './app.component';
import { AppRoutingModule } from './app-routing.module';

import { HomeComponent } from './home/home.component';
import { QueryEngineComponent } from './data-space/query-engine/query-engine.component';
import { AutocompleteComponent } from './data-space/query-engine/autocomplete/autocomplete.component';
import { SqlEditorComponent } from './data-space/query-engine/sql-editor/sql-editor.component';

import { AuthLoginComponent } from './auth/login/auth-login.component';
import { AuthCallbackComponent } from './auth/callback/auth-callback.component';
import { AuthLogoutComponent } from './auth/logout/auth-logout.component';
import { TokenInterceptor } from './auth/services/token.interceptor';
import { OidcJWTToken } from './auth/oidc';
import { ToastContainerComponent } from './shared/toast-container/toast-container.component';
import { TooltipDirective } from './shared/tooltip/tooltip.directive';

export function configFactory(http: HttpClient): ConfigLoader {
  return new ConfigHttpLoader(http, './assets/config.json');
}

export function translateLoaderFactory(http: HttpClient) {
  return new TranslateHttpLoader(http, './assets/i18n/', '.json');
}

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent,
    QueryEngineComponent,
    AutocompleteComponent,
    SqlEditorComponent,
    AuthLoginComponent,
    AuthCallbackComponent,
    AuthLogoutComponent,
    ToastContainerComponent,
    TooltipDirective,
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    ReactiveFormsModule,
    HttpClientModule,
    AppRoutingModule,

    NbThemeModule.forRoot({ name: 'default' }),
    NbEvaIconsModule,

    // Same OAuth2 strategy shape the dashboard registers in
    // @core/core.module.ts. Real endpoints/redirect URIs/client id are only
    // known at runtime (they depend on assets/config.json), so this is just
    // a placeholder - AppComponent's constructor calls
    // oauthStrategy.setOptions(...) with the real values once ConfigService
    // has loaded.
    NbAuthModule.forRoot({
      strategies: [
        NbOAuth2AuthStrategy.setup({
          name: 'oidc',
          clientId: '',
          token: {
            class: OidcJWTToken,
          },
        }),
      ],
    }),

    ConfigModule.forRoot({
      provide: ConfigLoader,
      useFactory: configFactory,
      deps: [HttpClient],
    }),

    TranslateModule.forRoot({
      defaultLanguage: 'en',
      loader: {
        provide: TranslateLoader,
        useFactory: translateLoaderFactory,
        deps: [HttpClient],
      },
    }),
  ],
  providers: [
    {
      provide: HTTP_INTERCEPTORS,
      useClass: TokenInterceptor,
      multi: true,
    },
  ],
  bootstrap: [AppComponent],
})
export class AppModule { }
