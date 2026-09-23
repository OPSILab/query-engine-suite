import { Injectable } from '@angular/core';
import {
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpInterceptor
} from '@angular/common/http';
import { Observable } from 'rxjs';
import { NbAuthOAuth2JWTToken, NbAuthService } from '@nebular/auth';
import { ConfigService } from '@ngx-config/core';

// Direct copy of the dashboard's TokenInterceptor: attaches the Keycloak
// access token as a Bearer header on outgoing API calls.
//
// NB: `this.auth.getToken().subscribe(...)` right before reading
// `this.token` relies on NbTokenService's token observable emitting
// synchronously on subscribe (it's backed by a value already held in
// storage, not an actual async call) - that's how the original dashboard
// code works too, but it's worth knowing if this ever stops behaving
// synchronously after a Nebular upgrade.
@Injectable()
export class TokenInterceptor implements HttpInterceptor {

  token: NbAuthOAuth2JWTToken;

  constructor(public auth: NbAuthService, public config: ConfigService) { }

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    if (req.url.indexOf('/assets/') > -1) {
      return next.handle(req);
    }

    if (req.url.indexOf('/oauth2/token') > -1
      || req.url.indexOf('/openid-connect/token') > -1
    ) {
      return next.handle(req);
    }

    let newHeaders = req.headers;

    if (this.config.getSettings('enableAuthentication')) {
      this.auth.getToken().subscribe((x: NbAuthOAuth2JWTToken) => this.token = x);
      if (this.token && this.token.getPayload() != null) {
        newHeaders = newHeaders.append('Authorization', 'Bearer ' + this.token.getPayload().access_token);
      }
    }

    const authReq = req.clone({ headers: newHeaders });
    return next.handle(authReq);
  }

}
