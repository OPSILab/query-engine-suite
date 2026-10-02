import { NbAuthOAuth2JWTToken } from '@nebular/auth';

// Direct copy of the dashboard's src/app/auth/oidc/oidc.ts: the decoded
// Keycloak access-token shape, and the NbAuthOAuth2JWTToken subclass Nebular
// uses to read the bearer token out of the OAuth2 token response.
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

export interface OidcToken {
  user: UserClaims;
  id_token: string;
  access_token: string;
}

export class OidcJWTToken extends NbAuthOAuth2JWTToken {
  static NAME = 'nb:auth:oidc:token';

  protected readonly token: OidcToken;

  getValue(): string {
    return this.token.access_token;
  }
}
