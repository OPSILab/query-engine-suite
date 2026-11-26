import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgModule } from '@angular/core';
import { HttpClient, HTTP_INTERCEPTORS, provideHttpClient, withInterceptorsFromDi, withXhr } from '@angular/common/http';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { AbstractSecurityStorage, DefaultLocalStorageService, StsConfigLoader, provideAuth } from 'angular-auth-oidc-client';
import { environment } from '../environments/environment';

// Was @ngx-config/core + @ngx-config/http-loader: abandoned at v9, peer deps
// pinned to Angular 9, which blocked ng update. See services/config.service.ts.
import { CONFIG_PROVIDERS, ConfigService } from './services/config.service';
import { TranslateModule, TranslateLoader } from '@ngx-translate/core';
import { TranslateHttpLoader } from '@ngx-translate/http-loader';

import { AppComponent } from './app.component';
import { AppRoutingModule } from './app-routing.module';

import { HomeComponent } from './home/home.component';
import { IntroComponent } from './home/intro/intro.component';
import { QueryEngineComponent } from './data-space/query-engine/query-engine.component';
import { AutocompleteComponent } from './data-space/query-engine/autocomplete/autocomplete.component';
import { SqlEditorComponent } from './data-space/query-engine/sql-editor/sql-editor.component';
import { GraphqlEditorComponent } from './data-space/query-engine/graphql-editor/graphql-editor.component';

import { AuthLoginComponent } from './auth/login/auth-login.component';
import { AuthCallbackComponent } from './auth/callback/auth-callback.component';
import { AuthLogoutComponent } from './auth/logout/auth-logout.component';
import { TokenInterceptor } from './auth/services/token.interceptor';
import { ConfigSettingsStsLoader } from './auth/oidc';
import { ToastContainerComponent } from './shared/toast-container/toast-container.component';
import { TooltipDirective } from './shared/tooltip/tooltip.directive';
import { JsonTableComponent } from './shared/json-table/json-table.component';
import { ThemePickerComponent } from './shared/theme-picker/theme-picker.component';
import { TopbarComponent } from './shared/topbar/topbar.component';
import { LayoutComponent } from './shared/layout/layout.component';
import { SiteNoticesComponent } from './shared/site-notices/site-notices.component';
import { StatusLogComponent } from './status-log/status-log.component';
import { AuthLinkComponent } from './shared/auth-link/auth-link.component';
import { SourceConnectorComponent } from './source-connector/source-connector.component';
import { CacheComponent } from './cache/cache.component';
import { ConfigurationComponent } from './configuration/configuration.component';
import { ApiBackupsComponent } from './source-connector/api-backups/api-backups.component';

export function translateLoaderFactory(http: HttpClient) {
  return new TranslateHttpLoader(http, './assets/i18n/', '.json');
}

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent,
    IntroComponent,
    QueryEngineComponent,
    AutocompleteComponent,
    SqlEditorComponent,
    GraphqlEditorComponent,
    AuthLoginComponent,
    AuthCallbackComponent,
    AuthLogoutComponent,
    ToastContainerComponent,
    TooltipDirective,
    JsonTableComponent,
    ThemePickerComponent,
    TopbarComponent,
    LayoutComponent,
    SiteNoticesComponent,
    StatusLogComponent,
    AuthLinkComponent,
    SourceConnectorComponent,
    CacheComponent,
    ConfigurationComponent,
    ApiBackupsComponent,
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    ReactiveFormsModule,
    AppRoutingModule,

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
    // The Keycloak login (angular-auth-oidc-client): its configuration read from assets/config.json when the login
    // is first checked (see auth/oidc.ts, auth/services/auth.service.ts); the tokens in localStorage, shared by the
    // tabs (as with the previous library).
    provideAuth({
      loader: {
        provide: StsConfigLoader,
        useFactory: (config: ConfigService) => new ConfigSettingsStsLoader((key, defaultValue) => config.getSettings(key, defaultValue), environment.keycloak.client_id),
        deps: [ConfigService],
      },
    }),
    { provide: AbstractSecurityStorage, useClass: DefaultLocalStorageService },
    // App initializer that loads assets/config.json, replacing what
    // ConfigModule.forRoot() used to do - see services/config.service.ts.
    ...CONFIG_PROVIDERS,
    {
      provide: HTTP_INTERCEPTORS,
      useClass: TokenInterceptor,
      multi: true,
    },
    // Was HttpClientModule in `imports`, rewritten by the Angular 18 migration:
    // the module is deprecated, and withInterceptorsFromDi() is what keeps the
    // HTTP_INTERCEPTORS-based TokenInterceptor above working.
    provideHttpClient(withXhr(), withInterceptorsFromDi()),
  ],
  bootstrap: [AppComponent],
})
export class AppModule { }
