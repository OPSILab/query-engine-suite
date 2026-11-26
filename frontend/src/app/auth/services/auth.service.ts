import { Injectable } from '@angular/core';
import { OidcSecurityService } from 'angular-auth-oidc-client';
import { Observable, defer, of } from 'rxjs';
import { catchError, distinctUntilChanged, map, shareReplay, switchMap, take } from 'rxjs/operators';
import { ConfigService } from '../../services/config.service';
import { UserClaims, decodeJwtPayload, keycloakUrls } from '../oidc';

/**
 * The Keycloak login of the app (on angular-auth-oidc-client): what the guards, the interceptor, the session and
 * the pages need. With "enableAuthentication": false nothing is asked to Keycloak: nobody is logged in.
 *
 * The login is checked once per page load (checkAuth): on /keycloak-auth/callback it exchanges the code Keycloak
 * sent back for the tokens; elsewhere it reads the tokens stored (localStorage: shared by the tabs). While the app
 * is open the access token is renewed with the refresh token before it expires; when that is no longer possible
 * (Keycloak session ended) the user is logged out, and the next page that needs the login goes to Keycloak.
 */
@Injectable({ providedIn: 'root' })
export class AuthService {
  // Built at the first use, not in the constructor: the interceptor (so this service) is created by the request that
  // loads assets/config.json, before the settings are there.
  private check$?: Observable<boolean>;
  private auth$?: Observable<boolean>;

  constructor(private oidc: OidcSecurityService, private config: ConfigService) {
    // the token stored by the previous login library (Nebular), no longer read
    try { localStorage.removeItem('auth_app_token'); } catch { /* no storage */ }
  }

  /** "enableAuthentication" of assets/config.json. */
  get enabled(): boolean {
    return !!this.config.getSettings('enableAuthentication', false);
  }

  /** The login checked (true: logged in at the start); every caller shares the same check. */
  private get checked$(): Observable<boolean> {
    return this.check$ ??= defer(() => this.oidc.checkAuth()).pipe(
      map(result => {
        if (result?.errorMessage) console.error('Login:', result.errorMessage);
        return !!result?.isAuthenticated;
      }),
      catchError(error => {
        console.error('Login check failed', error);
        return of(false);
      }),
      shareReplay({ bufferSize: 1, refCount: false }),
    );
  }

  /** Whether someone is logged in, now and at every change (logout, renewal failed). */
  get authenticated$(): Observable<boolean> {
    if (!this.enabled) return of(false);
    return this.auth$ ??= this.checked$.pipe(
      switchMap(() => this.oidc.isAuthenticated$.pipe(map(result => !!result?.isAuthenticated))),
      catchError(() => of(false)),
      distinctUntilChanged(),
      shareReplay({ bufferSize: 1, refCount: false }),
    );
  }

  /** The claims of the access token of the user logged in (null: nobody), at every login change. */
  get claims$(): Observable<UserClaims | null> {
    return this.authenticated$.pipe(
      map(authenticated => authenticated ? decodeJwtPayload<UserClaims>(this.accessTokenNow()) ?? null : null),
    );
  }

  /** Whether someone is logged in now (after the login has been checked). */
  isAuthenticated(): Observable<boolean> {
    return this.authenticated$.pipe(take(1));
  }

  /** The access token of the user logged in (null: nobody), once the login has been checked. */
  accessToken(): Observable<string | null> {
    if (!this.enabled) return of(null);
    return this.checked$.pipe(
      take(1),
      switchMap(() => this.oidc.getAccessToken()),
      map(token => token || null),
      catchError(() => of(null)),
    );
  }

  /**
   * The access token now, without waiting (null: nobody, or the login not checked yet). The library reads it from
   * its storage synchronously once its configuration is loaded.
   */
  accessTokenNow(): string | null {
    if (!this.enabled) return null;
    let token: string | null = null;
    try {
      this.oidc.getAccessToken().pipe(take(1)).subscribe(value => token = value || null).unsubscribe();
    } catch {
      token = null;
    }
    return token;
  }

  /** To the Keycloak login page; back on /keycloak-auth/callback. */
  login(): void {
    this.oidc.authorize();
  }

  /** Logged out here and on Keycloak, then back to the app (dashboardBaseURL). */
  logout(): Observable<unknown> {
    return this.oidc.logoff();
  }

  /** The requests to Keycloak itself (token, certificates): no Bearer token added, never waiting for the login. */
  isKeycloakUrl(url: string): boolean {
    if (url.indexOf('/oauth2/token') > -1 || url.indexOf('/openid-connect/token') > -1) return true;
    const { realm } = keycloakUrls(this.config.getSettings('keycloak.baseURL', ''));
    return !!realm && url.startsWith(realm + '/');
  }
}
