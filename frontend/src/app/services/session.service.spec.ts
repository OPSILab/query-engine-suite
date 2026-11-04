import { describe, expect, it } from 'vitest';
import { Subject, firstValueFrom, of } from 'rxjs';
import { SessionService } from './session.service';
import { LoggedInGuard } from '../auth/services/logged-in.guard';
import { AuthGuard } from '../auth/services/auth.guard';

const config = (settings: Record<string, any>) => ({ getSettings: (key: string, def?: any) => key in settings ? settings[key] : def });
const fakeAuth = (authenticated: boolean) => {
  const changes = new Subject<boolean>();
  return { auth: { isAuthenticated: () => of(authenticated), onAuthenticationChange: () => changes }, changes };
};
const fakeRouter = () => ({ url: '/cache', navigated: [] as any[], navigate(to: any) { this.navigated.push(to); }, parseUrl: (url: string) => ({ url }) });

describe('SessionService', () => {
  it('login mode: disabled, required, optional (authNotRequired)', () => {
    expect(SessionService.modeOf(false, true)).toBe('disabled');
    expect(SessionService.modeOf(true, false)).toBe('required');
    expect(SessionService.modeOf(true, true)).toBe('optional');
  });

  it('loggedIn$: false with authentication disabled; follows login / logout otherwise', async () => {
    expect(await firstValueFrom(new SessionService(config({ enableAuthentication: false }) as any, fakeAuth(true).auth as any).loggedIn$)).toBe(false);
    const { auth, changes } = fakeAuth(false);
    const session = new SessionService(config({ enableAuthentication: true, authNotRequired: true }) as any, auth as any);
    const seen: boolean[] = [];
    session.loggedIn$.subscribe(value => seen.push(value));
    changes.next(true);
    changes.next(true);
    changes.next(false);
    expect(seen).toEqual([false, true, false]);
  });

  it('login: back to the page open now', () => {
    const router = fakeRouter();
    new SessionService(config({ enableAuthentication: true }) as any, fakeAuth(false).auth as any, router as any).login();
    expect(router.navigated).toEqual([['/keycloak-auth']]);
    expect(sessionStorage.getItem('qe.returnUrl')).toBe('/cache');
  });
});

describe('guards', () => {
  it('AuthGuard: the app opens without login when it is not required', () => {
    const guard = (settings: any) => new AuthGuard({ isAuthenticated: () => of(false) } as any, fakeRouter() as any, config(settings) as any);
    expect(guard({ enableAuthentication: false }).canActivate()).toBe(true);
    expect(guard({ enableAuthentication: true, authNotRequired: true }).canActivate()).toBe(true);
    expect(guard({ enableAuthentication: true }).canActivate()).not.toBe(true); // the observable of the login
  });

  it('LoggedInGuard: logged in opens; not logged to the login (back here after it); auth disabled to the home', async () => {
    const run = async (settings: any, authenticated: boolean) => {
      const router = fakeRouter();
      const session = new SessionService(config(settings) as any, fakeAuth(authenticated).auth as any, router as any);
      return firstValueFrom(new LoggedInGuard(session, router as any).canActivate(undefined, { url: '/source-connector' } as any));
    };
    expect(await run({ enableAuthentication: true, authNotRequired: true }, true)).toBe(true);
    expect(await run({ enableAuthentication: true, authNotRequired: true }, false)).toEqual({ url: '/keycloak-auth' });
    expect(sessionStorage.getItem('qe.returnUrl')).toBe('/source-connector');
    expect(await run({ enableAuthentication: false }, true)).toEqual({ url: '/' });
  });
});
