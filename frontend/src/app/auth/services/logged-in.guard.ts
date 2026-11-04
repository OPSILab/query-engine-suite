import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Router, RouterStateSnapshot, UrlTree } from '@angular/router';
import { Observable, of } from 'rxjs';
import { map, take } from 'rxjs/operators';
import { SessionService } from '../../services/session.service';
import { saveReturnUrl } from './return-url';

/**
 * The pages for logged users only (Source connector, Cache), whatever the login mode: logged in, the page opens;
 * not logged in, to the Keycloak login and back to the page; authentication disabled (nobody can log in), to the
 * home page.
 */
@Injectable({ providedIn: 'root' })
export class LoggedInGuard {
  constructor(private session: SessionService, private router: Router) { }

  canActivate(route?: ActivatedRouteSnapshot, state?: RouterStateSnapshot): Observable<boolean | UrlTree> {
    if (!this.session.authEnabled)
      return of(this.router.parseUrl('/'));
    return this.session.loggedIn$.pipe(
      take(1),
      map(loggedIn => {
        if (loggedIn) return true;
        if (state?.url) saveReturnUrl(state.url);
        return this.router.parseUrl('/keycloak-auth');
      }),
    );
  }
}
