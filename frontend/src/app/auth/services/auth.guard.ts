import { Injectable } from '@angular/core';
import { CanActivate, Router } from '@angular/router';
import { NbAuthService } from '@nebular/auth';
import { ConfigService } from '@ngx-config/core';
import { tap } from 'rxjs/operators';

// Direct copy of the dashboard's AuthGuard.
@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {

  constructor(private authService: NbAuthService, private router: Router, private configService: ConfigService) {
  }

  canActivate() {
    if (!this.configService.getSettings('enableAuthentication')) {
      return true;
    } else {
      return this.authService.isAuthenticated()
        .pipe(
          tap(authenticated => {
            if (!authenticated) {
              this.router.navigate(['/keycloak-auth']);
            }
          }),
        );
    }
  }

}
