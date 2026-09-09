import { Component, Inject, OnInit, OnDestroy } from '@angular/core';
import { DOCUMENT } from '@angular/common';
import { Router } from '@angular/router';
import { NbAuthResult, NbAuthService } from '@nebular/auth';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { ConfigService } from '@ngx-config/core';
import { environment } from '../../../environments/environment';

// Direct copy of the dashboard's AuthLogoutComponent, parameterized on
// environment.keycloak.client_id instead of the hardcoded
// "client_id=beopen-dashboard" the original had.
@Component({
  selector: 'ngx-oauth2-logout',
  template: ``,
})
export class AuthLogoutComponent implements OnInit, OnDestroy {

  private destroy$ = new Subject<void>();

  constructor(private authService: NbAuthService,
    private router: Router,
    private configs: ConfigService,
    // Angular's own SSR-safe window/document access, replacing Nebular's
    // NB_WINDOW token (@nebular/theme) - this was the last thing in the app
    // still pulling in that package outside of the invisible overlay-host
    // <nb-layout>, which is gone too now (see app.component.ts).
    @Inject(DOCUMENT) private document: Document) {
  }

  ngOnInit(): void {
    this.authService.logout(this.configs.getSettings('keycloak.authProfile'))
      .pipe(takeUntil(this.destroy$))
      .subscribe((authResult: NbAuthResult) => {
        if (authResult.isSuccess()) {
          this.document.defaultView.location.href =
            `${this.configs.getSettings('keycloak.baseURL')}/logout?post_logout_redirect_uri=${this.configs.getSettings('dashboardBaseURL')}&client_id=${environment.keycloak.client_id}`;
        } else {
          this.router.navigateByUrl('');
        }
      }, (error) => {
        console.error(error);
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
