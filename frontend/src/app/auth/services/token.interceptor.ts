import { Injectable } from '@angular/core';
import { HttpRequest, HttpHandler, HttpEvent, HttpInterceptor } from '@angular/common/http';
import { Observable } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { AuthService } from './auth.service';

// Adds the Keycloak access token (Authorization: Bearer) to the requests of the app, when someone is logged in.
// Not to the files of the app (/assets/: config.json, translations) nor to Keycloak itself (the login uses those
// requests: waiting for the login there would never end). A request that already has an Authorization header keeps it.
@Injectable()
export class TokenInterceptor implements HttpInterceptor {

  constructor(private auth: AuthService) { }

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    // /assets/ first: the request of assets/config.json comes before the settings
    if (req.url.indexOf('/assets/') > -1 || !this.auth.enabled || this.auth.isKeycloakUrl(req.url) || req.headers.has('Authorization')) {
      return next.handle(req);
    }
    return this.auth.accessToken().pipe(
      switchMap(token => next.handle(token ? req.clone({ setHeaders: { Authorization: `Bearer ${token}` } }) : req)),
    );
  }

}
