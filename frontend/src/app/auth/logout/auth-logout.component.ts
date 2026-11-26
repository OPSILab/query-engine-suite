import { Component, OnInit, OnDestroy, ChangeDetectionStrategy } from '@angular/core';
import { Router } from '@angular/router';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { AuthService } from '../services/auth.service';

// /keycloak-auth/logout: the tokens removed here, the Keycloak session ended (its logout page, with id_token_hint,
// client_id and post_logout_redirect_uri = dashboardBaseURL), then back to the app. Authentication disabled: home.
@Component({
    selector: 'ngx-oauth2-logout',
    template: ``,
    changeDetection: ChangeDetectionStrategy.Eager,
    standalone: false
})
export class AuthLogoutComponent implements OnInit, OnDestroy {

  private destroy$ = new Subject<void>();

  constructor(private authService: AuthService, private router: Router) {
  }

  ngOnInit(): void {
    if (!this.authService.enabled) {
      this.router.navigateByUrl('');
      return;
    }
    this.authService.logout()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        error: (error) => {
          console.error(error);
          this.router.navigateByUrl('');
        },
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
