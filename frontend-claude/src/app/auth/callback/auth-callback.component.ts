import { Component, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { NbAuthService, NbAuthResult } from '@nebular/auth';
import { Router } from '@angular/router';
import { takeUntil } from 'rxjs/operators';
import { ConfigService } from '@ngx-config/core';
import { BeopenAPIService } from '../../services/be-open.service';
import { OidcUserInformationService } from '../services/oidc-user-information.service';
import { UserClaims } from '../oidc';
import { SharedService } from '../../services/shared.service';

/**
 * Adapted from the dashboard's AuthCallbackComponent.
 *
 * Differences from the original:
 * - The dashboard redirects to a /register-user page when
 *   beopenAPI.getUser() 404s (the user authenticated with Keycloak but has
 *   no BeOpen user record yet), and only reads role claims off the JWT in
 *   that registration flow. This standalone project has no registration
 *   page, so it always reads the roles straight off the JWT via
 *   OidcUserInformationService and propagates them through SharedService -
 *   that's the only thing QueryEngineComponent actually needs
 *   (userRoles$, used to gate isAdmin). If the BeOpen user record exists it
 *   is still propagated too, in case you build more on top of this later.
 */
@Component({
  selector: 'ngx-oauth2-callback',
  template: ``,
})
export class AuthCallbackComponent implements OnDestroy {

  private destroy$ = new Subject<void>();

  constructor(private authService: NbAuthService,
    private router: Router,
    private configService: ConfigService,
    private userService: OidcUserInformationService,
    private sharedService: SharedService,
    private beopenAPIService: BeopenAPIService) {
    this.authService.authenticate(this.configService.getSettings('keycloak.authProfile'))
      .pipe(takeUntil(this.destroy$))
      .subscribe((authResult: NbAuthResult) => {
        this.propagateRolesFromToken();

        this.beopenAPIService.getUser().subscribe(beopenUser => {
          this.sharedService.propagateUser(beopenUser);
          this.router.navigateByUrl('/');
        }, err => {
          console.warn('No BeOpen user record for this Keycloak account yet', err);
          this.router.navigateByUrl('/');
        });
      }, (error) => {
        console.error(error);
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
