// The page asked before the Keycloak login (e.g. a link with the filters of an Advanced search: /?key=...&value=...),
// opened again after the login instead of the home page. Kept in sessionStorage: the login leaves the app.
// Only paths of this app ("/..."), never another site.

const STORAGE_KEY = 'qe.returnUrl';

export function saveReturnUrl(url: string): void {
  try {
    if (isAppPath(url) && url !== '/')
      sessionStorage.setItem(STORAGE_KEY, url);
  } catch {
    // storage not available: back to the home page after the login
  }
}

/** The saved page (removed), or "/" */
export function takeReturnUrl(): string {
  try {
    const url = sessionStorage.getItem(STORAGE_KEY);
    sessionStorage.removeItem(STORAGE_KEY);
    return url && isAppPath(url) ? url : '/';
  } catch {
    return '/';
  }
}

// "/x" yes; "//other.site", "/\\other.site", "https://..." no
function isAppPath(url: string): boolean {
  return typeof url === 'string' && url.startsWith('/') && !url.startsWith('//') && !url.startsWith('/\\');
}
