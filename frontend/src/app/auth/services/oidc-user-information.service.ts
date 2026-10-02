import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { OidcJWTToken, UserClaims } from '../oidc';
import { NbAuthService } from '@nebular/auth';

// Direct copy of the dashboard's OidcUserInformationService: decodes the
// Keycloak access token's payload into UserClaims whenever Nebular's token
// changes.
@Injectable({
  providedIn: 'root'
})
export class OidcUserInformationService {

  user: UserClaims;
  protected user$: BehaviorSubject<any> = new BehaviorSubject(null);

  constructor(private authService: NbAuthService) {
    this.authService.onTokenChange()
      .subscribe((token: OidcJWTToken) => {
        if (token.isValid()) {
          this.user = token.getAccessTokenPayload();
          this.publishUser(this.user);
        }
      });
  }

  getRole(): Observable<string[]> {
    return this.user ? of(this.user.roles.map(role => role.toUpperCase())) : of(['USER']);
  }

  getUser(): Observable<UserClaims> {
    return of(this.user);
  }

  private publishUser(user: any) {
    this.user$.next(user);
  }

  onUserChange(): Observable<any> {
    return this.user$;
  }

}
