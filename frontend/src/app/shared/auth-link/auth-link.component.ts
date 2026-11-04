import { Component } from '@angular/core';
import { SessionService } from '../../services/session.service';

/**
 * Login / Logout in the bar at the top, by the login mode (SessionService): nothing with authentication disabled,
 * Logout once logged in, Login otherwise (back to this page after it).
 */
@Component({
  selector: 'app-auth-link',
  template: `
    @if (session.authEnabled) {
      @if (session.loggedIn$ | async) {
        <a class="ds-link" routerLink="/keycloak-auth/logout">{{ 'Logout' | translate }}</a>
      } @else {
        <a class="ds-link" href="/keycloak-auth" (click)="$event.preventDefault(); session.login()">{{ 'Log in' | translate }}</a>
      }
    }
  `,
  standalone: false
})
export class AuthLinkComponent {
  constructor(public session: SessionService) { }
}
