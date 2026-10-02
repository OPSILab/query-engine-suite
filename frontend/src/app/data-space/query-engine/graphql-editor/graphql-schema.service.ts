import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom, timeout } from 'rxjs';
import { GqlSchema, schemaFromIntrospection } from './graphql-schema';

// Deliberately trimmed compared to the canonical introspection query used by
// GraphiQL & co.: no descriptions, directives, enum values, input fields or
// deprecation info - only what the suggestions list and the "unknown field"
// check actually read. Four levels of ofType cover every wrapper this schema
// uses ([DataPoint!]! is NON_NULL > LIST > NON_NULL > OBJECT).
const INTROSPECTION_QUERY = `
query DataSpaceEditorIntrospection {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types {
      kind
      name
      fields {
        name
        type { ...TypeRef }
        args { name type { ...TypeRef } }
      }
    }
  }
}
fragment TypeRef on __Type {
  kind name
  ofType { kind name ofType { kind name ofType { kind name } } }
}`;

const INTROSPECTION_TIMEOUT_MS = 8000;

/**
 * Fetches the GraphQL schema through introspection, once per session.
 *
 * Failure is an expected state, not an error: Apollo Server switches
 * introspection off when NODE_ENV=production, and the backend may just be
 * unreachable. Every failure - HTTP error, timeout, a response without
 * `data.__schema` - resolves to `null` rather than rejecting, and the
 * editor falls back to syntax colouring only. Nothing here inspects Apollo's
 * error message text, so it doesn't break if that wording changes.
 *
 * Only a success is cached. After a failure the next load() tries again,
 * which in practice means the next time the GraphQL tab is opened (the
 * editor is destroyed and recreated by the tab's @if).
 */
@Injectable({ providedIn: 'root' })
export class GraphqlSchemaService {

  private cache = new Map<string, Promise<GqlSchema | null>>();

  constructor(private http: HttpClient) { }

  load(endpoint: string): Promise<GqlSchema | null> {
    const cached = this.cache.get(endpoint);
    if (cached) {
      return cached;
    }

    const request = firstValueFrom(
      this.http.post<any>(endpoint, { query: INTROSPECTION_QUERY }).pipe(timeout(INTROSPECTION_TIMEOUT_MS))
    )
      .then(res => schemaFromIntrospection(res))
      .catch(err => {
        console.info('GraphQL introspection unavailable - editor falls back to highlighting only.', err);
        return null;
      })
      .then(schema => {
        if (!schema) {
          this.cache.delete(endpoint);
        }
        return schema;
      });

    this.cache.set(endpoint, request);
    return request;
  }

}
