/**
 * Schema model used by the GraphQL editor, and the pure helpers around it.
 * Kept free of Angular imports so the analyzer that depends on it can be
 * exercised on its own.
 */

/** A type reference as introspection returns it: wrappers (NON_NULL, LIST) around a named type. */
export interface GqlTypeRef {
  kind: string;
  name: string | null;
  ofType?: GqlTypeRef | null;
}

export interface GqlArg {
  name: string;
  type: GqlTypeRef;
}

export interface GqlField {
  name: string;
  type: GqlTypeRef;
  args: GqlArg[];
}

export interface GqlType {
  name: string;
  kind: string;
  fields: Map<string, GqlField>;
}

/** The slice of the backend's schema the editor needs: object types, their fields and arguments. */
export interface GqlSchema {
  queryType: string | null;
  mutationType: string | null;
  subscriptionType: string | null;
  types: Map<string, GqlType>;
}

/**
 * Turns an introspection response into the editor's schema model, or null
 * when the response doesn't carry one.
 */
export function schemaFromIntrospection(res: any): GqlSchema | null {
  const raw = res?.data?.__schema;
  if (!raw || !Array.isArray(raw.types)) {
    return null;
  }

  const types = new Map<string, GqlType>();
  for (const t of raw.types) {
    if (!t?.name || !Array.isArray(t.fields)) {
      continue; // scalars, enums, input objects: nothing to suggest inside them
    }
    const fields = new Map<string, GqlField>();
    for (const f of t.fields) {
      fields.set(f.name, { name: f.name, type: f.type, args: Array.isArray(f.args) ? f.args : [] });
    }
    types.set(t.name, { name: t.name, kind: t.kind, fields });
  }

  return {
    queryType: raw.queryType?.name ?? null,
    mutationType: raw.mutationType?.name ?? null,
    subscriptionType: raw.subscriptionType?.name ?? null,
    types,
  };
}

/** Innermost named type: [DataPoint!]! -> "DataPoint". */
export function namedType(ref: GqlTypeRef | null | undefined): string | null {
  let r = ref;
  while (r && !r.name) {
    r = r.ofType;
  }
  return r?.name ?? null;
}

/** SDL spelling of a type reference: NON_NULL(LIST(NON_NULL(DataPoint))) -> "[DataPoint!]!". */
export function typeToString(ref: GqlTypeRef | null | undefined): string {
  if (!ref) {
    return '?';
  }
  if (ref.kind === 'NON_NULL') {
    return typeToString(ref.ofType) + '!';
  }
  if (ref.kind === 'LIST') {
    return '[' + typeToString(ref.ofType) + ']';
  }
  return ref.name ?? '?';
}
