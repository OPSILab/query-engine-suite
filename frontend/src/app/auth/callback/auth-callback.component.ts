import { Component, OnDestroy, ChangeDetectionStrategy } from '@angular/core';
import { Subject } from 'rxjs';
import { Router } from '@angular/router';
import { takeUntil } from 'rxjs/operators';
import { BeopenAPIService } from '../../services/be-open.service';
import { OidcUserInformationService } from '../services/oidc-user-information.service';
import { AuthService } from '../services/auth.service';
import { UserClaims } from '../oidc';
import { SharedService } from '../../services/shared.service';
import { takeReturnUrl } from '../services/return-url';

/**
 * /keycloak-auth/callback, where Keycloak sends the user back after the login: the code exchanged for the tokens
 * (AuthService), the roles of the access token propagated (SharedService: QueryEngineComponent's isAdmin), the
 * BeOpen user record too when there is one, then the page asked before the login (AuthGuard), else the home page.
 * The callback url (with the code, used once) is replaced in the history.
 */
@Component({
    selector: 'ngx-oauth2-callback',
    template: ``,
    changeDetection: ChangeDetectionStrategy.Eager,
    standalone: false
})
export class AuthCallbackComponent implements OnDestroy {

  private destroy$ = new Subject<void>();

  constructor(private authService: AuthService,
    private router: Router,
    private userService: OidcUserInformationService,
    private sharedService: SharedService,
    private beopenAPIService: BeopenAPIService) {
    this.authService.isAuthenticated()
      .pipe(takeUntil(this.destroy$))
      .subscribe(authenticated => {
        if (!authenticated) {
          // the reason is in the console (AuthService); no new attempt here: it could loop on Keycloak
          console.error('Login failed');
          return;
        }
        this.propagateRolesFromToken();

        const returnUrl = takeReturnUrl();
        this.beopenAPIService.getUser().subscribe(beopenUser => {
          this.sharedService.propagateUser(beopenUser);
          this.router.navigateByUrl(returnUrl, { replaceUrl: true });
        }, err => {
          console.warn('No BeOpen user record for this Keycloak account yet', err);
          this.router.navigateByUrl(returnUrl, { replaceUrl: true });
        });
      });
  }

  private propagateRolesFromToken() {
    this.userService.onUserChange()
      .pipe(takeUntil(this.destroy$))
      .subscribe((claims: UserClaims) => {
        if (claims != undefined && claims.realm_access?.roles != undefined) {
          let rolesArray = claims.realm_access.roles;

          const unusedRoles = ['offline_access', 'uma_authorization'];
          rolesArray = rolesArray.filter(role => !unusedRoles.includes(role));
          rolesArray = rolesArray.filter(str => !str.startsWith('default'));

          this.sharedService.propagateUserClaims(claims);
          this.sharedService.propagateUserRoles(rolesArray);
        }
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
