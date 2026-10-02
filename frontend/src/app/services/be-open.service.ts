import { HttpClient, HttpHeaders, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { ConfigService } from "./config.service";
import { Observable } from "rxjs";
import { BeopenUser } from "../model/beopen-user";

// Trimmed copy of the dashboard's BeopenAPIService: only the calls used by
// the query engine (Simple search / Advanced search / Query SQL) are kept.
// See the main dashboard's src/app/pages/services/be-open.service.ts for the
// full API surface (dataset management, file upload, superset, ...).
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
   * Sends a GraphQL document as-is. No `visibility` header: the GraphQL
   * resolvers don't read it (the Private/Shared/Public switch applies to the
   * REST modes only).
   */
  public graphqlQuery(query: string): Observable<any> {
    return this.http.post<any>(this.graphqlEndpoint, { query });
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
    let params = new HttpParams();
    params = params.set("format", type);
    for (let key in mongoQuery) params = params.set(key, mongoQuery[key]);
    switch (mode) {
      case "Advanced search":
        return this.http.post<any>(
          `${this.queryEngineBaseUrl}/api/query`,
          { mongoQuery },
          { params, headers: new HttpHeaders({ visibility }) }
        );
      case "Query SQL":
        return this.http.post<any>(
          `${this.queryEngineBaseUrl}/api/query`,
          { query: sqlQuery },
          { headers: new HttpHeaders({ visibility }) }
        );
      case "Simple search":
        return this.http.get<any>(
          `${this.queryEngineBaseUrl}/api/query?value=${value}`,
          { headers: new HttpHeaders({ visibility, isRawQuery: "yes" }) }
        );
    }
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
