import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { UserClaims } from '../oidc';
import { AuthService } from './auth.service';

// The claims of the Keycloak access token of the user logged in (UserClaims), published at every login change.
@Injectable({
  providedIn: 'root'
})
export class OidcUserInformationService {

  user: UserClaims;
  protected user$: BehaviorSubject<any> = new BehaviorSubject(null);

  constructor(private authService: AuthService) {
    this.authService.claims$
      .subscribe((claims: UserClaims | null) => {
        if (claims) {
          this.user = claims;
          this.publishUser(this.user);
        }
      });
  }

  getRole(): Observable<string[]> {
    return this.user?.roles ? of(this.user.roles.map(role => role.toUpperCase())) : of(['USER']);
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
