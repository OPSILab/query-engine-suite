import { APP_INITIALIZER, Injectable, Provider } from '@angular/core';
import { HttpClient } from '@angular/common/http';

/**
 * Drop-in replacement for `ConfigService` from `@ngx-config/core`.
 *
 * Why this exists: `@ngx-config` was abandoned at v9 and its peer deps are
 * hard-pinned to `@angular/* >=9.0.0 <10.0.0`, so it blocks every `ng update`
 * past Angular 9 (and Angular 13 drops ViewEngine libraries entirely, so
 * `--force` would only have bought one version). The library did exactly two
 * things - fetch a JSON file from an APP_INITIALIZER, and expose it through
 * `getSettings()` - both reproduced here.
 *
 * `getSettings()` keeps the original semantics on purpose, including the
 * parts that are easy to trip over:
 *  - no key (or `[]`) returns the whole settings object, which is what
 *    BeopenAPIService relies on (`getSettings().queryEngineBaseUrl`);
 *  - a dotted string or a string[] walks nested keys;
 *  - a missing key with no `defaultValue` THROWS rather than returning
 *    undefined. TokenInterceptor would hit this on every request if the
 *    config hadn't loaded yet - it doesn't, because it short-circuits
 *    `/assets/` URLs (which is what the config request itself is) before
 *    ever calling getSettings.
 */
@Injectable({ providedIn: 'root' })
export class ConfigService {

  private settings: any;

  constructor(private readonly http: HttpClient) { }

  init(endpoint: string = './assets/config.json'): Promise<any> {
    return new Promise<any>((resolve, reject) => {
      this.http.get(endpoint).subscribe({
        next: (res: any) => resolve(this.settings = res),
        error: () => reject('Endpoint unreachable!'),
      });
    });
  }

  getSettings(key?: string | string[], defaultValue?: any): any {
    if (!key || (Array.isArray(key) && !key[0])) {
      return this.settings;
    }

    const paths: string[] = !Array.isArray(key) ? key.split('.') : key;
    let result = paths.reduce((acc: any, current: string) => acc && acc[current], this.settings);

    if (result === undefined) {
      result = defaultValue;

      if (result === undefined) {
        throw new Error(`No setting found with the specified key [${paths.join('/')}]!`);
      }
    }

    return result;
  }
}

export function configInitializerFactory(config: ConfigService): () => Promise<any> {
  return () => config.init('./assets/config.json');
}

/**
 * Replaces `ConfigModule.forRoot({ provide: ConfigLoader, ... })` in AppModule.
 * Registered as an APP_INITIALIZER, so assets/config.json is guaranteed to be
 * loaded before AppComponent's constructor runs `oauthStrategy.setOptions(...)`.
 */
export const CONFIG_PROVIDERS: Provider[] = [
  {
    provide: APP_INITIALIZER,
    useFactory: configInitializerFactory,
    deps: [ConfigService],
    multi: true,
  },
];
