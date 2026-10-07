import { describe, expect, it } from 'vitest';
import { QueryRequest, SNIPPET_LANGS, fullUrl, toAxios, toCurl, toFetch, toSnippet } from './request-snippets';

const post: QueryRequest = {
  method: 'POST',
  url: 'http://localhost:5500/api/query',
  params: { format: 'JSON', city: "Reggio nell'Emilia" },
  headers: { visibility: 'public' },
  body: { mongoQuery: { city: "Reggio nell'Emilia" } },
};
const get: QueryRequest = {
  method: 'GET',
  url: 'http://localhost:5500/api/query',
  params: { value: 'a b&c' },
  headers: { visibility: 'private', isRawQuery: 'yes' },
};

// Checks that the generated JavaScript at least parses (top-level await -> async function body).
const AsyncFunction = Object.getPrototypeOf(async function () { }).constructor;
const parses = (code: string) => new AsyncFunction(code.replace(/^import .*$/m, ''));

describe('fullUrl', () => {
  it('encodes the query string', () => {
    expect(fullUrl(get)).toBe('http://localhost:5500/api/query?value=a+b%26c');
  });
  it('no "?" without params', () => {
    expect(fullUrl({ ...get, params: {} })).toBe('http://localhost:5500/api/query');
  });
});

describe('toCurl', () => {
  it('method, url, headers and body, single quotes escaped for the shell', () => {
    const curl = toCurl(post, 'Bearer <TOKEN>');
    expect(curl).toContain(`curl -X POST 'http://localhost:5500/api/query?format=JSON&city=Reggio+nell%27Emilia'`);
    expect(curl).toContain(`-H 'visibility: public'`);
    expect(curl).toContain(`-H 'Content-Type: application/json'`);
    expect(curl).toContain(`-H 'Authorization: Bearer <TOKEN>'`);
    expect(curl).toContain(`"city": "Reggio nell'\\''Emilia"`);
    expect(curl.endsWith('\n')).toBe(true);
  });

  it('no Authorization when auth is null, no body / Content-Type on GET', () => {
    const curl = toCurl(get, null);
    expect(curl).not.toContain('Authorization');
    expect(curl).not.toContain('Content-Type');
    expect(curl).not.toContain('--data-raw');
    expect(curl).toContain(`-H 'isRawQuery: yes'`);
  });

  it('the body sent by curl is the JSON body', () => {
    const quoted = toCurl(post, null).split('--data-raw ')[1].trim();
    // undo the shell quoting: '...' with '\'' for each single quote
    const unquoted = quoted.slice(1, -1).replace(/'\\''/g, "'");
    expect(JSON.parse(unquoted)).toEqual(post.body);
  });
});

describe('toFetch', () => {
  it('valid JavaScript with url, method, headers and body', () => {
    const code = toFetch(post, 'Bearer abc');
    expect(() => parses(code)).not.toThrow();
    expect(code).toContain('await fetch("http://localhost:5500/api/query?format=JSON&city=Reggio+nell%27Emilia"');
    expect(code).toContain('"Authorization": "Bearer abc"');
    expect(code).toContain('body: JSON.stringify(');
  });

  it('GET without body', () => {
    const code = toFetch(get, null);
    expect(() => parses(code)).not.toThrow();
    expect(code).not.toContain('body:');
  });

  it('really sends the described request', async () => {
    const calls: any[] = [];
    const fakeFetch = async (url: string, init: any) => {
      calls.push({ url, init });
      return { ok: true, json: async () => ({ ok: 1 }) };
    };
    const run = new AsyncFunction('fetch', 'console', toFetch(post, 'Bearer abc'));
    await run(fakeFetch, { log: () => { } });
    expect(calls[0].url).toBe(fullUrl(post));
    expect(calls[0].init.method).toBe('POST');
    expect(calls[0].init.headers).toEqual({ visibility: 'public', 'Content-Type': 'application/json', Authorization: 'Bearer abc' });
    expect(JSON.parse(calls[0].init.body)).toEqual(post.body);
  });
});

describe('toAxios', () => {
  it('valid JavaScript, params kept separate from the url', () => {
    const code = toAxios(post, null);
    expect(code.startsWith('import axios from "axios";')).toBe(true);
    expect(() => parses(code)).not.toThrow();
    expect(code).toContain('method: "post"');
    expect(code).toContain('url: "http://localhost:5500/api/query"');
    expect(code).toContain('"format": "JSON"');
    expect(code).toContain('data: {');
  });

  it('really sends the described request', async () => {
    let config: any;
    const axios = { request: async (c: any) => { config = c; return { data: [] }; } };
    const run = new AsyncFunction('axios', 'console', toAxios(get, 'Bearer t').replace(/^import .*$/m, ''));
    await run(axios, { log: () => { } });
    expect(config).toEqual({
      method: 'get',
      url: get.url,
      params: get.params,
      headers: { ...get.headers, Authorization: 'Bearer t' },
    });
  });
});

describe('toSnippet', () => {
  it('dispatches on the language; every language has a file extension', () => {
    expect(toSnippet('curl', get, null)).toBe(toCurl(get, null));
    expect(toSnippet('fetch', get, null)).toBe(toFetch(get, null));
    expect(toSnippet('axios', get, null)).toBe(toAxios(get, null));
    expect(SNIPPET_LANGS.map(l => [l.id, l.extension])).toEqual([['curl', 'sh'], ['fetch', 'mjs'], ['axios', 'mjs']]);
  });

  it('does not modify the request', () => {
    const copy = JSON.parse(JSON.stringify(post));
    toSnippet('curl', post, 'Bearer x');
    toSnippet('fetch', post, 'Bearer x');
    toSnippet('axios', post, 'Bearer x');
    expect(post).toEqual(copy);
  });
});
