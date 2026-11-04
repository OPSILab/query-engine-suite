import { Injectable, Optional } from '@angular/core';
import { Router } from '@angular/router';
import { NbAuthService } from '@nebular/auth';
import { Observable, defer, merge, of } from 'rxjs';
import { catchError, distinctUntilChanged, map, shareReplay } from 'rxjs/operators';
import { ConfigService } from './config.service';
import { saveReturnUrl } from '../auth/services/return-url';

/** How the login works, from assets/config.json. */
export type AuthMode = 'disabled' | 'required' | 'optional';

/**
 * The login of the user, as the pages and the side menu need it.
 * assets/config.json:
 *  - "enableAuthentication": false - authDisabled: no login at all (no Login / Logout links);
 *  - "enableAuthentication": true - the login is required to open the app (AuthGuard), Logout in the bar;
 *  - with "authNotRequired": true as well - the app opens without logging in, the bar shows Login (or Logout once
 *    logged in): something to show to users without an account. The Query-Engine is not changed by it: what it
 *    answers without a token is up to it.
 * The pages for logged users only (Source connector, Cache) are hidden from the menu otherwise (loggedIn$) and their
 * routes guarded (LoggedInGuard).
 */
@Injectable({ providedIn: 'root' })
export class SessionService {
  readonly mode: AuthMode;
  /** Whether someone is logged in (always false with authentication disabled); updated at login / logout. */
  readonly loggedIn$: Observable<boolean>;

  constructor(config: ConfigService, @Optional() private auth?: NbAuthService, @Optional() private router?: Router) {
    this.mode = SessionService.modeOf(config.getSettings('enableAuthentication', false), config.getSettings('authNotRequired', false));
    this.loggedIn$ = this.mode === 'disabled' || !auth
      ? of(false)
      : merge(defer(() => auth.isAuthenticated()), auth.onAuthenticationChange()).pipe(
        map(value => !!value),
        catchError(() => of(false)),
        distinctUntilChanged(),
        shareReplay({ bufferSize: 1, refCount: false }),
      );
  }

  static modeOf(enableAuthentication: any, authNotRequired: any): AuthMode {
    if (!enableAuthentication) return 'disabled';
    return authNotRequired ? 'optional' : 'required';
  }

  get authEnabled(): boolean {
    return this.mode !== 'disabled';
  }

  /** To the Keycloak login, then back to `returnUrl` (default: the page open now). */
  login(returnUrl?: string): void {
    saveReturnUrl(returnUrl ?? this.router?.url ?? '/');
    this.router?.navigate(['/keycloak-auth']);
  }
}
