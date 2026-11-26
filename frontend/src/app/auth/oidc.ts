import { LogLevel, OpenIdConfiguration, StsConfigLoader } from 'angular-auth-oidc-client';
import { Observable, of } from 'rxjs';

// The Keycloak login (OIDC authorization code flow with PKCE, angular-auth-oidc-client), configured from
// assets/config.json at runtime: keycloak.baseURL (the openid-connect endpoint of the realm) and dashboardBaseURL.

// The claims of the Keycloak access token used by the app.
export interface RealmAccess {
  roles: string[];
}

export interface UserClaims {
  email: string;
  email_verified: boolean;
  family_name?: string;
  given_name?: string;
  locale?: string;
  name: string;
  preferred_username: string;
  picture: string;
  sub: string;
  updated_at: string;
  roles: string[];
  realm_access: RealmAccess;
}

type Settings = (key: string, defaultValue?: any) => any;

/**
 * The urls of the realm from keycloak.baseURL (".../realms/<realm>/protocol/openid-connect"): `endpoints`, the
 * baseURL itself (where /auth, /token, /logout, /certs are); `realm`, the realm (the issuer of the tokens).
 */
export function keycloakUrls(baseURL: string): { endpoints: string, realm: string } {
  const endpoints = String(baseURL || '').replace(/\/+$/, '');
  return { endpoints, realm: endpoints.replace(/\/protocol\/openid-connect$/, '') };
}

/**
 * The configuration of the login. The endpoints are given (no request to .well-known); redirect back to
 * {dashboardBaseURL}/keycloak-auth/callback (the same as before: nothing to change in the Keycloak client).
 * keycloak.issuer: only when the tokens carry another issuer than the realm url of baseURL (e.g. Keycloak reached
 * through another host name).
 */
export function oidcConfig(settings: Settings, clientId: string): OpenIdConfiguration {
  const { endpoints, realm } = keycloakUrls(settings('keycloak.baseURL', ''));
  const app = String(settings('dashboardBaseURL', '') || '').replace(/\/+$/, '');
  return {
    configId: 'keycloak',
    authority: realm,
    authWellknownEndpoints: {
      issuer: settings('keycloak.issuer', '') || realm,
      authorizationEndpoint: `${endpoints}/auth`,
      tokenEndpoint: `${endpoints}/token`,
      userInfoEndpoint: `${endpoints}/userinfo`,
      endSessionEndpoint: `${endpoints}/logout`,
      jwksUri: `${endpoints}/certs`,
      revocationEndpoint: `${endpoints}/revoke`,
      introspectionEndpoint: `${endpoints}/token/introspect`,
    },
    clientId,
    responseType: 'code',
    scope: 'openid',
    redirectUrl: `${app}/keycloak-auth/callback`,
    postLogoutRedirectUri: app || undefined,
    // the access token renewed with the refresh token before it expires, while the app is open
    silentRenew: true,
    useRefreshToken: true,
    renewTimeBeforeTokenExpiresInSeconds: 30,
    disableRefreshTokenOfflineAccessScopeWarning: true,
    // the user is read from the access token: no request to userinfo
    autoUserInfo: false,
    // Keycloak logout: with client_id as well (accepted also without id_token_hint)
    customParamsEndSessionRequest: { client_id: clientId },
    logLevel: LogLevel.Warn,
  };
}

/** Gives the library the configuration once assets/config.json is loaded (read when the login is first checked). */
export class ConfigSettingsStsLoader implements StsConfigLoader {
  constructor(private settings: Settings, private clientId: string) { }

  loadConfigs(): Observable<OpenIdConfiguration[]> {
    return of([oidcConfig(this.settings, this.clientId)]);
  }
}

/** The claims of a JWT (not verified: only to read them), undefined if it is not one. */
export function decodeJwtPayload<T = any>(token: string | null | undefined): T | undefined {
  try {
    const part = String(token || '').split('.')[1];
    if (!part) return undefined;
    const base64 = part.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(part.length / 4) * 4, '=');
    const bytes = Uint8Array.from(atob(base64), c => c.charCodeAt(0));
    const payload = JSON.parse(new TextDecoder().decode(bytes));
    return payload && typeof payload === 'object' ? payload : undefined;
  } catch {
    return undefined;
  }
}
