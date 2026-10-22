import { beforeEach, describe, expect, it } from 'vitest';
import { saveReturnUrl, takeReturnUrl } from './return-url';

beforeEach(() => sessionStorage.clear());

describe('page to open again after the login', () => {
  it('the saved page is returned once, with its query params', () => {
    saveReturnUrl('/?key=source&value=https%3A%2F%2Fapi.example%2Fdata%3Fa%3D1%26b%3D2');
    expect(takeReturnUrl()).toBe('/?key=source&value=https%3A%2F%2Fapi.example%2Fdata%3Fa%3D1%26b%3D2');
    expect(takeReturnUrl()).toBe('/');
  });

  it('nothing saved: the home page', () => {
    expect(takeReturnUrl()).toBe('/');
  });

  it('only pages of this app: never another site', () => {
    for (const url of ['https://other.site/x', '//other.site/x', '/\\other.site', 'javascript:alert(1)']) {
      saveReturnUrl(url);
      expect(takeReturnUrl()).toBe('/');
    }
    sessionStorage.setItem('qe.returnUrl', '//other.site');
    expect(takeReturnUrl()).toBe('/');
  });
});
