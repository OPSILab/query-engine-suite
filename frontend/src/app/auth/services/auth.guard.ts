import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Router, RouterStateSnapshot } from '@angular/router';
import { NbAuthService } from '@nebular/auth';
import { ConfigService } from '../../services/config.service';
import { tap } from 'rxjs/operators';
import { saveReturnUrl } from './return-url';

// Direct copy of the dashboard's AuthGuard.
@Injectable({
  providedIn: 'root'
})
export class AuthGuard {

  constructor(private authService: NbAuthService, private router: Router, private configService: ConfigService) {
  }

  canActivate(route?: ActivatedRouteSnapshot, state?: RouterStateSnapshot) {
    // authentication disabled, or not required ("authNotRequired": the app opens without logging in, Login in the bar)
    if (!this.configService.getSettings('enableAuthentication') || this.configService.getSettings('authNotRequired', false)) {
      return true;
    } else {
      return this.authService.isAuthenticated()
        .pipe(
          tap(authenticated => {
            if (!authenticated) {
              // e.g. a link with Advanced search filters: opened again after the login (AuthCallbackComponent)
              if (state?.url) saveReturnUrl(state.url);
              this.router.navigate(['/keycloak-auth']);
            }
          }),
        );
    }
  }

}
