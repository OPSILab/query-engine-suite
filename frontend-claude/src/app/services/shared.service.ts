import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { UserClaims } from '../auth/oidc';
import { BeopenUser } from '../model/beopen-user';

// Trimmed copy of the dashboard's SharedService: only the pieces of state the
// query engine reads (userRoles$) are kept, plus the ones it was already
// wired to so this stays a drop-in match for the original component code.
@Injectable({
  providedIn: 'root'
})
export class SharedService {

  constructor() { }

  private user = new BehaviorSubject<BeopenUser>(null);
  user$ = this.user.asObservable();

  private userClaims = new BehaviorSubject<UserClaims>(null);
  userClaims$ = this.userClaims.asObservable();

  // Defaults to [] rather than null: the dashboard's own AppComponent normally
  // populates this after login, which this standalone project doesn't
  // replicate, and query-engine.component.ts calls `.includes(...)` on it.
  private userRoles = new BehaviorSubject<string[]>([]);
  userRoles$ = this.userRoles.asObservable();

  propagateUser(userV: BeopenUser) {
    this.user.next(userV);
  }

  propagateUserClaims(userV: UserClaims) {
    this.userClaims.next(userV);
  }

  propagateUserRoles(userRoles: string[]) {
    this.userRoles.next(userRoles);
  }
}
