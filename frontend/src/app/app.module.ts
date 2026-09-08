import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgModule } from '@angular/core';
import { HttpClientModule, HttpClient, HTTP_INTERCEPTORS } from '@angular/common/http';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import {
  NbThemeModule,
  NbLayoutModule,
  NbAccordionModule,
  NbButtonModule,
  NbIconModule,
  NbSelectModule,
  NbOptionModule,
  NbInputModule,
  NbPopoverModule,
  NbDatepickerModule,
  NbToastrModule,
  NbAutocompleteModule,
} from '@nebular/theme';
import { NbEvaIconsModule } from '@nebular/eva-icons';
import { NbAuthModule, NbOAuth2AuthStrategy } from '@nebular/auth';

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
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    ReactiveFormsModule,
    HttpClientModule,
    AppRoutingModule,

    NbThemeModule.forRoot({ name: 'default' }),
    NbLayoutModule,
    NbEvaIconsModule,
    NbAccordionModule,
    NbButtonModule,
    NbIconModule,
    NbSelectModule,
    NbOptionModule,
    NbInputModule,
    NbPopoverModule,
    NbDatepickerModule.forRoot(),
    NbToastrModule.forRoot(),
    NbAutocompleteModule,

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
