import { HttpClient, HttpHeaders, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { ConfigService } from "./config.service";
import { Observable, Subject, firstValueFrom, map } from "rxjs";
import { BeopenUser } from "../model/beopen-user";
import { QueryRequest } from "../data-space/query-engine/request-snippets";

// Trimmed copy of the dashboard's BeopenAPIService: only the calls used by
// the query engine (Simple search / Advanced search / Query SQL) are kept.
// See the main dashboard's src/app/pages/services/be-open.service.ts for the
// full API surface (dataset management, file upload, superset, ...).
/** Drops headers without a value (e.g. visibility not chosen yet) instead of sending "undefined". */
export function definedHeaders(headers: Record<string, any>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key in headers)
    if (headers[key] !== undefined && headers[key] !== null) out[key] = String(headers[key]);
  return out;
}

/**
 * What a query did not search or returned incomplete, as sent by the Query-Engine (Simple search): in the
 * X-Query-Warnings header of a query, or from /api/query/simple/limits. "config": left out by the configuration
 * (e.g. Orion disabled); "runtime": happened during this query (an API unreachable, results truncated...).
 */
export interface QueryWarning {
  kind: "config" | "runtime";
  code: string;
  source?: string;
  /** The collection the warning is about (minio / api / orion), when it is about one. */
  collection?: string;
  message: string;
}

/** Page size of the keys / values / entries suggestions. */
export const SUGGESTIONS_PAGE_SIZE = 100;

/**
 * Default page size of the Advanced search results: config.json "advancedSearchPageSize" overrides it (see
 * BeopenAPIService.advancedSearchPageSize). The Query-Engine refuses pages above its queryOptions.advancedSearchMaxResults.
 */
export const ADVANCED_SEARCH_PAGE_SIZE = 50;

/**
 * A page of results: { limit, skip } in the request, { results, hasMore, next } in the response. The limit applies
 * to each collection; skip is a number (every collection) or the `next` of the previous page ({ <collection>: n }).
 */
export interface ResultsPage {
  limit: number;
  skip: number | Record<string, number>;
}

/**
 * The collections of the Query-Engine (one per Source-Connector connector): api (apiConnector records), orion
 * (Orion datasets, datapoints), minio (MinIO files). advancedSearch / simpleSearch: searched by that mode.
 */
export interface CollectionInfo {
  id: string;
  advancedSearch: boolean;
  simpleSearch: boolean;
  /** Selected for a user who has not chosen yet (the Query-Engine's queryOptions.defaultCollections). */
  default?: boolean;
}

/** Names shown for the collections, unless config.json has "collectionLabels". */
export const DEFAULT_COLLECTION_LABELS: Record<string, string> = { api: "Sources", orion: "Datapoints", minio: "Files" };

export interface SuggestionsPage<T> {
  items: T[];
  /** More pages after this one. */
  hasMore: boolean;
}

export interface SuggestionEntry {
  key: string;
  value: string;
}

/** X-Query-Warnings: URI-encoded JSON array. Anything unreadable counts as no warning. */
export function parseQueryWarnings(header: string | null | undefined): QueryWarning[] {
  if (!header) return [];
  try {
    const warnings = JSON.parse(decodeURIComponent(header));
    return Array.isArray(warnings) ? warnings.filter(w => w && typeof w.code === "string") : [];
  } catch {
    return [];
  }
}

@Injectable({
  providedIn: "root",
})
export class BeopenAPIService {
  /** Warnings sent with the response of a query (see QueryWarning). */
  readonly queryWarnings$ = new Subject<QueryWarning[]>();

  private apiBaseUrl: string;
  queryEngineBaseUrl: string;
  /**
   * Apollo's endpoint on the Query-Engine. Mounted at the app root
   * (applyMiddleware({ path: '/graphql' }) in Query-Engine/index.js), not
   * under the REST basePath, hence not "/api/graphql". Overridable with an
   * optional "graphqlUrl" in config.json, for deployments where a reverse
   * proxy exposes it somewhere else.
   */
  graphqlEndpoint: string;

  constructor(private http: HttpClient, private configService: ConfigService) {
    this.apiBaseUrl = this.configService.getSettings("beopenApiBaseUrl");
    this.queryEngineBaseUrl =
      this.configService.getSettings().queryEngineBaseUrl ||
      this.configService.getSettings().beopenApiBaseUrl;
    this.graphqlEndpoint =
      this.configService.getSettings("graphqlUrl", null) ||
      `${this.queryEngineBaseUrl}/graphql`;
  }

  /**
   * Sends a GraphQL document as-is, with the `visibility` header: with authentication
   * on, the resolvers scope the results like the REST modes (Private/Shared/Public).
   */
  public graphqlQuery(query: string, visibility?: string): Observable<any> {
    return this.send(this.buildGraphqlRequest(query, visibility));
  }

  public buildGraphqlRequest(query: string, visibility?: string): QueryRequest {
    return { method: "POST", url: this.graphqlEndpoint, params: {}, headers: { visibility }, body: { query } };
  }

  public getUser(): Observable<BeopenUser> {
    return this.http.get<BeopenUser>(`${this.apiBaseUrl}/api/user`);
  }

  public minioQuery(
    mode: string,
    value,
    mongoQuery,
    sqlQuery: string,
    visibility: string,
    type: string,
    page?: ResultsPage,
    collections?: string[]
  ): Observable<any> {
    const request = this.buildQueryRequest(mode, value, mongoQuery, sqlQuery, visibility, type, page, collections);
    return request ? this.send(request) : undefined;
  }

  /**
   * The request each REST mode sends, as plain data: minioQuery() sends it and the
   * "Generate curl / fetch / axios" dialog turns it into code, so the two always match.
   * Advanced search with a page: the response is { results, hasMore, skip, limit, next } instead of the list.
   * collections: the collections searched by Advanced / Simple search (undefined: all of them).
   */
  public buildQueryRequest(
    mode: string,
    value,
    mongoQuery,
    sqlQuery: string,
    visibility: string,
    type: string,
    page?: ResultsPage,
    collections?: string[]
  ): QueryRequest | undefined {
    const url = `${this.queryEngineBaseUrl}/api/query`;
    switch (mode) {
      case "Advanced search": {
        const params: Record<string, string> = {};
        if (type !== undefined && type !== null) params["format"] = String(type);
        for (let key in mongoQuery) params[key] = String(mongoQuery[key] ?? "");
        return { method: "POST", url, params, headers: { visibility }, body: { mongoQuery, ...(page ? { page } : {}), ...(collections ? { collections } : {}) } };
      }
      case "Query SQL":
        return { method: "POST", url, params: {}, headers: { visibility }, body: { query: sqlQuery } };
      case "Simple search":
        return { method: "GET", url, params: { value: String(value ?? ""), ...(collections ? { collections: collections.join(",") } : {}) }, headers: { visibility, isRawQuery: "yes" } };
    }
  }

  private send(request: QueryRequest): Observable<any> {
    return this.http.request<any>(request.method, request.url, {
      body: request.body,
      params: new HttpParams({ fromObject: request.params }),
      headers: new HttpHeaders(definedHeaders(request.headers)),
      observe: "response",
    }).pipe(map(response => {
      const warnings = parseQueryWarnings(response.headers?.get("X-Query-Warnings"));
      if (warnings.length) this.queryWarnings$.next(warnings);
      return response.body;
    }));
  }

  /** What the configuration leaves out of the Simple search ([] with a Query-Engine that doesn't report it). */
  getSimpleSearchLimits(): Promise<QueryWarning[]> {
    return firstValueFrom(this.http.get<{ warnings: QueryWarning[] }>(`${this.queryEngineBaseUrl}/api/query/simple/limits`))
      .then(res => (Array.isArray(res?.warnings) ? res.warnings : []))
      .catch(() => []);
  }


  // ---- keys / values / entries suggestions, one page at a time (the Query-Engine never reads them all)

  // collections: only the suggestions of those collections (undefined: all of them)

  /** Keys starting with `prefix` (case insensitive), sorted. */
  getKeys(prefix = "", skip = 0, limit = SUGGESTIONS_PAGE_SIZE, collections?: string[]): Promise<SuggestionsPage<string>> {
    return this.suggestionsPage("keys", { key: prefix }, skip, limit, (row: any) => row?.key, collections);
  }

  /** Values starting with `prefix` (case insensitive), sorted. */
  getValues(prefix = "", skip = 0, limit = SUGGESTIONS_PAGE_SIZE, collections?: string[]): Promise<SuggestionsPage<string>> {
    return this.suggestionsPage("values", { value: prefix }, skip, limit, (row: any) => row?.value, collections);
  }

  /**
   * Key / value pairs whose key starts with `key` and value with `value` (case insensitive); exact.key /
   * exact.value: that field must be the whole text instead.
   */
  getEntries(key = "", value = "", skip = 0, limit = SUGGESTIONS_PAGE_SIZE, exact: { key?: boolean; value?: boolean } = {}, collections?: string[]): Promise<SuggestionsPage<SuggestionEntry>> {
    const params: Record<string, string> = { key, value };
    if (exact.key) params["exactKey"] = "true";
    if (exact.value) params["exactValue"] = "true";
    return this.suggestionsPage("entries", params, skip, limit, (row: any) => row && row.key !== undefined ? { key: String(row.key), value: String(row.value) } : undefined, collections);
  }

  /**
   * Keys whose values are not (all) suggested: the Source-Connector does not index them for some sources (e.g. the
   * datapoints' `value`, hundreds of thousands of distinct numbers). [] when unavailable (older Query-Engine).
   */
  getKeysWithValuesNotIndexed(collections?: string[]): Promise<string[]> {
    if (collections && !collections.length) return Promise.resolve([]); // no collection chosen: nothing to warn about
    const params = new HttpParams({ fromObject: collections ? { collections: collections.join(",") } : {} });
    return firstValueFrom(this.http.get<any>(`${this.queryEngineBaseUrl}/api/keys/notIndexed`, { params }))
      .then(res => (Array.isArray(res?.keys) ? res.keys.filter((k: any) => typeof k === "string") : []))
      .catch(() => []);
  }

  /**
   * The collections the Query-Engine offers (GET /api/collections); [] for an older Query-Engine without them (the
   * frontend then shows no choice and sends no `collections`).
   */
  getCollections(): Promise<CollectionInfo[]> {
    return firstValueFrom(this.http.get<any>(`${this.queryEngineBaseUrl}/api/collections`))
      .then(res => (Array.isArray(res?.collections) ? res.collections : [])
        .filter((c: any) => typeof c?.id === "string")
        .map((c: any) => ({ id: c.id, advancedSearch: c.advancedSearch !== false, simpleSearch: c.simpleSearch !== false, ...(typeof c.default === "boolean" ? { default: c.default } : {}) })))
      .catch(() => []);
  }

  /** Page size of the Advanced search: config.json "advancedSearchPageSize" (a positive integer), else ADVANCED_SEARCH_PAGE_SIZE. */
  advancedSearchPageSize(): number {
    const size = Number(this.configService.getSettings("advancedSearchPageSize", null));
    return Number.isInteger(size) && size > 0 ? size : ADVANCED_SEARCH_PAGE_SIZE;
  }

  /** Label of a collection: config.json "collectionLabels", else DEFAULT_COLLECTION_LABELS, else its id. */
  collectionLabel(id: string): string {
    const labels = this.configService.getSettings("collectionLabels", null);
    return (labels && typeof labels[id] === "string" && labels[id]) || DEFAULT_COLLECTION_LABELS[id] || id;
  }

  // collections: [] (no collection chosen) -> no suggestions, without asking (the Query-Engine would read an empty
  // list as "not given", i.e. its default collections); undefined -> all of them
  private async suggestionsPage<T>(path: string, search: Record<string, string>, skip: number, limit: number, pick: (row: any) => T | undefined, collections?: string[]): Promise<SuggestionsPage<T>> {
    if (collections && !collections.length) return { items: [], hasMore: false };
    const params = new HttpParams({ fromObject: { ...search, limit: String(limit), skip: String(skip), ...(collections ? { collections: collections.join(",") } : {}) } });
    const res: any = await firstValueFrom(this.http.get<any>(`${this.queryEngineBaseUrl}/api/${path}`, { params }));
    // a Query-Engine without pages answers the whole list, or ["Too many suggestions..."]
    const rows: any[] = Array.isArray(res) ? res.filter(row => typeof row !== "string") : (res?.items ?? []);
    const items = rows.map(pick).filter((item): item is T => item !== undefined && item !== null);
    return { items, hasMore: Array.isArray(res) ? res.some(row => typeof row === "string") : !!res?.hasMore };
  }
}
