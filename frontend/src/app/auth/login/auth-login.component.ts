import { Component, ChangeDetectionStrategy } from '@angular/core';
import { AuthService } from '../services/auth.service';

// /keycloak-auth: straight to the Keycloak login page.
@Component({
    selector: 'ngx-oauth2-login',
    template: ``,
    changeDetection: ChangeDetectionStrategy.Eager,
    standalone: false
})
export class AuthLoginComponent {

  constructor(authService: AuthService) {
    authService.login();
  }

}
