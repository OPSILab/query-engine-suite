/**
 * Describes an HTTP request to the Query-Engine and turns it into code that replays it elsewhere
 * (cURL, fetch, axios). BeopenAPIService builds the description and sends the real request from
 * the same object, so the generated code can't drift from what the app actually sends.
 */
export interface QueryRequest {
  method: 'GET' | 'POST';
  /** Without query string. */
  url: string;
  /** Query string parameters. */
  params: Record<string, string>;
  /** Without Authorization (added by TokenInterceptor in the app, by `auth` here). */
  headers: Record<string, string>;
  /** JSON body (POST only). */
  body?: any;
}

export type SnippetLang = 'curl' | 'fetch' | 'axios';

export const SNIPPET_LANGS: { id: SnippetLang; label: string; extension: string }[] = [
  { id: 'curl', label: 'cURL', extension: 'sh' },
  // .mjs: top-level await works as-is with `node file.mjs` (Node 18+ has fetch built in)
  { id: 'fetch', label: 'fetch', extension: 'mjs' },
  { id: 'axios', label: 'axios', extension: 'mjs' },
];

export function fullUrl(req: QueryRequest): string {
  const qs = new URLSearchParams(req.params).toString();
  return qs ? `${req.url}?${qs}` : req.url;
}

/** `auth`: the Authorization header value (e.g. "Bearer <TOKEN>"), or null when the backend needs none. */
function allHeaders(req: QueryRequest, auth: string | null): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const key in req.headers)
    if (req.headers[key] !== undefined && req.headers[key] !== null) headers[key] = String(req.headers[key]);
  if (req.body !== undefined) headers['Content-Type'] = 'application/json';
  if (auth) headers['Authorization'] = auth;
  return headers;
}

/** Indents every line but the first (for values placed after "key: "). */
function indentTail(text: string, spaces: number): string {
  const pad = ' '.repeat(spaces);
  return text.split('\n').map((line, i) => (i ? pad + line : line)).join('\n');
}

function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

export function toCurl(req: QueryRequest, auth: string | null): string {
  const lines = [`curl -X ${req.method} ${shellQuote(fullUrl(req))}`];
  for (const [key, value] of Object.entries(allHeaders(req, auth)))
    lines.push(`  -H ${shellQuote(`${key}: ${value}`)}`);
  if (req.body !== undefined)
    lines.push(`  --data-raw ${shellQuote(JSON.stringify(req.body, null, 2))}`);
  return lines.join(' \\\n') + '\n';
}

export function toFetch(req: QueryRequest, auth: string | null): string {
  const options = [
    `  method: ${JSON.stringify(req.method)}`,
    `  headers: ${indentTail(JSON.stringify(allHeaders(req, auth), null, 2), 2)}`,
  ];
  if (req.body !== undefined)
    options.push(`  body: JSON.stringify(${indentTail(JSON.stringify(req.body, null, 2), 2)})`);
  return [
    `const response = await fetch(${JSON.stringify(fullUrl(req))}, {`,
    options.join(',\n'),
    `});`,
    `if (!response.ok) throw new Error(\`HTTP \${response.status}: \${await response.text()}\`);`,
    `const data = await response.json();`,
    `console.log(data);`,
    ``,
  ].join('\n');
}

export function toAxios(req: QueryRequest, auth: string | null): string {
  const config = [
    `  method: ${JSON.stringify(req.method.toLowerCase())}`,
    `  url: ${JSON.stringify(req.url)}`,
  ];
  if (Object.keys(req.params).length)
    config.push(`  params: ${indentTail(JSON.stringify(req.params, null, 2), 2)}`);
  config.push(`  headers: ${indentTail(JSON.stringify(allHeaders(req, auth), null, 2), 2)}`);
  if (req.body !== undefined)
    config.push(`  data: ${indentTail(JSON.stringify(req.body, null, 2), 2)}`);
  return [
    `import axios from "axios";`,
    ``,
    `const { data } = await axios.request({`,
    config.join(',\n'),
    `});`,
    `console.log(data);`,
    ``,
  ].join('\n');
}

export function toSnippet(lang: SnippetLang, req: QueryRequest, auth: string | null): string {
  switch (lang) {
    case 'curl': return toCurl(req, auth);
    case 'fetch': return toFetch(req, auth);
    case 'axios': return toAxios(req, auth);
  }
}
