import { Component, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { NbAuthResult, NbAuthService, NbAuthOAuth2JWTToken } from '@nebular/auth';
import { ConfigService } from '@ngx-config/core';

// Direct copy of the dashboard's AuthLoginComponent: kicks off the redirect
// to Keycloak as soon as the route is activated.
@Component({
  selector: 'ngx-oauth2-login',
  template: ``,
})
export class AuthLoginComponent implements OnDestroy {
  token: NbAuthOAuth2JWTToken;
  private destroy$ = new Subject<void>();

  constructor(private authService: NbAuthService, private configService: ConfigService) {
    this.login();
    this.authService.onTokenChange()
      .pipe(takeUntil(this.destroy$))
      .subscribe((token: NbAuthOAuth2JWTToken) => {
        this.token = null;
        if (token && token.isValid()) {
          this.token = token;
        }
      });
  }

  login() {
    this.authService.authenticate(this.configService.getSettings('keycloak.authProfile'))
      .pipe(takeUntil(this.destroy$))
      .subscribe((authResult: NbAuthResult) => {
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
