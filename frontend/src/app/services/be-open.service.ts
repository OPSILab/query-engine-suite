import { HttpClient, HttpHeaders, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { ConfigService } from "./config.service";
import { Observable } from "rxjs";
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

@Injectable({
  providedIn: "root",
})
export class BeopenAPIService {
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
    type: string
  ): Observable<any> {
    const request = this.buildQueryRequest(mode, value, mongoQuery, sqlQuery, visibility, type);
    return request ? this.send(request) : undefined;
  }

  /**
   * The request each REST mode sends, as plain data: minioQuery() sends it and the
   * "Generate curl / fetch / axios" dialog turns it into code, so the two always match.
   */
  public buildQueryRequest(
    mode: string,
    value,
    mongoQuery,
    sqlQuery: string,
    visibility: string,
    type: string
  ): QueryRequest | undefined {
    const url = `${this.queryEngineBaseUrl}/api/query`;
    switch (mode) {
      case "Advanced search": {
        const params: Record<string, string> = {};
        if (type !== undefined && type !== null) params["format"] = String(type);
        for (let key in mongoQuery) params[key] = String(mongoQuery[key] ?? "");
        return { method: "POST", url, params, headers: { visibility }, body: { mongoQuery } };
      }
      case "Query SQL":
        return { method: "POST", url, params: {}, headers: { visibility }, body: { query: sqlQuery } };
      case "Simple search":
        return { method: "GET", url, params: { value: String(value ?? "") }, headers: { visibility, isRawQuery: "yes" } };
    }
  }

  private send(request: QueryRequest): Observable<any> {
    return this.http.request<any>(request.method, request.url, {
      body: request.body,
      params: new HttpParams({ fromObject: request.params }),
      headers: new HttpHeaders(definedHeaders(request.headers)),
    });
  }


  getKeys(value?: string) {
    if (value && value[0] == "[") value = value.replace("[", "\\[");
    return this.http
      .get<any[]>(this.queryEngineBaseUrl + "/api/keys?key=" + (value || ""))
      .toPromise();
  }

  getValues(value?: string) {
    if (value && value[0] == "[") value = value.replace("[", "\\[");
    return this.http
      .get<any[]>(
        this.queryEngineBaseUrl + "/api/values?value=" + (value || "")
      )
      .toPromise();
  }

  getEntries(key?, value?) {
    if (value && value[0] == "[") value = value.replace("[", "\\[");
    if (key && key[0] == "[") key = key.replace("[", "\\[");
    return this.http
      .get<any[]>(
        this.queryEngineBaseUrl +
          "/api/entries?key=" +
          (key || "") +
          "&value=" +
          (value || "")
      )
      .toPromise();
  }
}
