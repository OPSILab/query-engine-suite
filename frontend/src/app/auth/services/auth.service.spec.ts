import { describe, expect, it, vi } from 'vitest';
import { BehaviorSubject, Subject, firstValueFrom, of, throwError } from 'rxjs';
import { HttpHeaders, HttpRequest } from '@angular/common/http';
import { AuthService } from './auth.service';
import { TokenInterceptor } from './token.interceptor';
import { decodeJwtPayload, keycloakUrls, oidcConfig } from '../oidc';

const BASE = 'https://kc.example/auth/realms/smartera/protocol/openid-connect';
const settingsOf = (values: Record<string, any>) => (key: string, def?: any) => key in values ? values[key] : def;
const config = (values: Record<string, any>) => ({ getSettings: settingsOf(values) });
const b64 = (o: any) => btoa(String.fromCharCode(...new TextEncoder().encode(JSON.stringify(o)))).replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
const jwt = (claims: any) => b64({ alg: 'RS256' }) + '.' + b64(claims) + '.sig';

// angular-auth-oidc-client's OidcSecurityService, as AuthService uses it
function fakeOidc({ authenticated = true, token = 'tok', checkError = false } = {}) {
  const state = new BehaviorSubject({ isAuthenticated: authenticated, allConfigsAuthenticated: [] });
  let current = authenticated ? token : '';
  return {
    state,
    setToken(value: string) { current = value; },
    checkAuth: vi.fn(() => checkError ? throwError(() => new Error('network')) : of({ isAuthenticated: authenticated, accessToken: current } as any)),
    isAuthenticated$: state,
    getAccessToken: vi.fn(() => of(current)),
    authorize: vi.fn(),
    logoff: vi.fn(() => of(null)),
  };
}
const auth = (values: Record<string, any>, oidc = fakeOidc()) => ({ service: new AuthService(oidc as any, config({ 'keycloak.baseURL': BASE, ...values }) as any), oidc });

describe('oidc configuration', () => {
  it('the realm and the endpoints from keycloak.baseURL; back to {dashboardBaseURL}/keycloak-auth/callback', () => {
    expect(keycloakUrls(BASE + '/')).toEqual({ endpoints: BASE, realm: 'https://kc.example/auth/realms/smartera' });
    const c = oidcConfig(settingsOf({ 'keycloak.baseURL': BASE, dashboardBaseURL: 'https://app.example/' }), 'query-engine');
    expect([c.authority, c.clientId, c.responseType, c.scope]).toEqual(['https://kc.example/auth/realms/smartera', 'query-engine', 'code', 'openid']);
    expect(c.authWellknownEndpoints).toMatchObject({
      issuer: 'https://kc.example/auth/realms/smartera', authorizationEndpoint: BASE + '/auth', tokenEndpoint: BASE + '/token',
      endSessionEndpoint: BASE + '/logout', jwksUri: BASE + '/certs',
    });
    expect([c.redirectUrl, c.postLogoutRedirectUri]).toEqual(['https://app.example/keycloak-auth/callback', 'https://app.example']);
    expect([c.useRefreshToken, c.silentRenew, c.autoUserInfo, c.disablePkce]).toEqual([true, true, false, undefined]);
    expect(c.customParamsEndSessionRequest).toEqual({ client_id: 'query-engine' });
    // keycloak.issuer: when the tokens carry another issuer
    expect(oidcConfig(settingsOf({ 'keycloak.baseURL': BASE, 'keycloak.issuer': 'https://public/auth/realms/smartera' }), 'x').authWellknownEndpoints.issuer)
      .toBe('https://public/auth/realms/smartera');
  });

  it('decodeJwtPayload: the claims (UTF-8), undefined when it is not a JWT', () => {
    expect(decodeJwtPayload(jwt({ name: 'Nicolò', realm_access: { roles: ['admin'] } }))).toEqual({ name: 'Nicolò', realm_access: { roles: ['admin'] } });
    for (const token of [null, '', 'abc', 'a.b.c', 'a.' + b64('x') + '.c'])
      expect(decodeJwtPayload(token)).toBeUndefined();
  });
});

describe('AuthService', () => {
  it('authentication disabled: nothing asked to Keycloak, nobody logged in', async () => {
    const { service, oidc } = auth({ enableAuthentication: false });
    expect([await firstValueFrom(service.isAuthenticated()), await firstValueFrom(service.accessToken()), service.accessTokenNow()]).toEqual([false, null, null]);
    expect(oidc.checkAuth).not.toHaveBeenCalled();
  });

  it('the login checked once for everyone; the token, the claims, the changes', async () => {
    const token = jwt({ email: 'anna@demetrix.it', realm_access: { roles: ['admin'] } });
    const oidc = fakeOidc({ token });
    const { service } = auth({ enableAuthentication: true }, oidc);
    const [a, b, t] = await Promise.all([firstValueFrom(service.isAuthenticated()), firstValueFrom(service.authenticated$), firstValueFrom(service.accessToken())]);
    expect([a, b, t]).toEqual([true, true, token]);
    expect(oidc.checkAuth).toHaveBeenCalledTimes(1);
    expect((await firstValueFrom(service.claims$))?.email).toBe('anna@demetrix.it');
    expect(service.accessTokenNow()).toBe(token);
    const seen: boolean[] = [];
    service.authenticated$.subscribe(value => seen.push(value));
    oidc.state.next({ isAuthenticated: true, allConfigsAuthenticated: [] }); // renewal: no change
    oidc.state.next({ isAuthenticated: false, allConfigsAuthenticated: [] }); // logged out / renewal failed
    expect(seen).toEqual([true, false]);
  });

  it('created before assets/config.json is loaded (by the interceptor of that request): the settings read later', async () => {
    const settings: Record<string, any> = {};
    const oidc = fakeOidc();
    const service = new AuthService(oidc as any, { getSettings: settingsOf(settings) } as any);
    expect(service.enabled).toBe(false);
    Object.assign(settings, { enableAuthentication: true, 'keycloak.baseURL': BASE });
    expect([service.enabled, await firstValueFrom(service.isAuthenticated()), service.isKeycloakUrl(BASE + '/certs')]).toEqual([true, true, true]);
    expect(oidc.checkAuth).toHaveBeenCalledTimes(1);
  });

  it('the check failing: nobody logged in, no exception', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => { });
    const { service } = auth({ enableAuthentication: true }, fakeOidc({ checkError: true, authenticated: false }));
    expect(await firstValueFrom(service.isAuthenticated())).toBe(false);
  });

  it('login, logout through the library; the urls of Keycloak recognised', () => {
    const { service, oidc } = auth({ enableAuthentication: true });
    service.login();
    service.logout().subscribe();
    expect([oidc.authorize.mock.calls.length, oidc.logoff.mock.calls.length]).toEqual([1, 1]);
    expect(service.isKeycloakUrl(BASE + '/token')).toBe(true);
    expect(service.isKeycloakUrl('https://kc.example/auth/realms/smartera/protocol/openid-connect/certs')).toBe(true);
    expect(service.isKeycloakUrl('https://kc.example/auth/realms/smarteraX/x')).toBe(false);
    expect(service.isKeycloakUrl('https://qe.example/api/query')).toBe(false);
  });
});

describe('TokenInterceptor', () => {
  const handler = () => {
    const seen: HttpRequest<any>[] = [];
    return { seen, handle: (req: HttpRequest<any>) => { seen.push(req); return of({} as any); } };
  };
  const run = async (interceptor: TokenInterceptor, req: HttpRequest<any>) => {
    const next = handler();
    await firstValueFrom(interceptor.intercept(req, next as any));
    return next.seen[0];
  };

  it('Bearer on the requests of the app; not on /assets/, Keycloak, nor a request with its own Authorization', async () => {
    const { service } = auth({ enableAuthentication: true });
    const interceptor = new TokenInterceptor(service);
    expect((await run(interceptor, new HttpRequest('GET', 'https://qe.example/api/query'))).headers.get('Authorization')).toBe('Bearer tok');
    expect((await run(interceptor, new HttpRequest('GET', './assets/config.json'))).headers.has('Authorization')).toBe(false);
    expect((await run(interceptor, new HttpRequest('POST', BASE + '/token', null))).headers.has('Authorization')).toBe(false);
    const own = new HttpRequest('GET', 'https://qe.example/api/x', { headers: new HttpHeaders({ Authorization: 'Bearer other' }) });
    expect((await run(interceptor, own)).headers.get('Authorization')).toBe('Bearer other');
  });

  it('nobody logged in or authentication disabled: the request as it is', async () => {
    const anonymous = new TokenInterceptor(auth({ enableAuthentication: true }, fakeOidc({ authenticated: false })).service);
    expect((await run(anonymous, new HttpRequest('GET', 'https://qe.example/api/query'))).headers.has('Authorization')).toBe(false);
    const disabled = new TokenInterceptor(auth({ enableAuthentication: false }).service);
    expect((await run(disabled, new HttpRequest('GET', 'https://qe.example/api/query'))).headers.has('Authorization')).toBe(false);
  });

  it('a request sent while the login is being checked waits for it (the Keycloak requests of the check do not)', async () => {
    const pending = new Subject<any>();
    const oidc = fakeOidc();
    oidc.checkAuth.mockReturnValue(pending as any);
    const interceptor = new TokenInterceptor(auth({ enableAuthentication: true }, oidc).service);
    const next = handler();
    interceptor.intercept(new HttpRequest('GET', 'https://qe.example/api/query'), next as any).subscribe();
    interceptor.intercept(new HttpRequest('POST', BASE + '/token', null), next as any).subscribe();
    expect(next.seen.map(r => r.url)).toEqual([BASE + '/token']);
    pending.next({ isAuthenticated: true });
    pending.complete();
    expect(next.seen.map(r => r.headers.get('Authorization'))).toEqual([null, 'Bearer tok']);
  });
});
