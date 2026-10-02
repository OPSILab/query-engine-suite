import { GqlField, GqlSchema, namedType, typeToString } from './graphql-schema';

/**
 * Hand-written GraphQL tokenizer + a single-pass structural walker, used by
 * GraphqlEditorComponent for three things: colouring, the suggestions list,
 * and refusing mutations/subscriptions. No external library on purpose.
 *
 * What this is NOT: a validator. It knows which fields and arguments exist
 * (when the schema is available) and flags names that don't, but it does not
 * check argument types, required arguments, variable usage or fragment
 * definitions - the backend (Apollo) already answers those with readable
 * errors, which the query engine shows as they come.
 *
 * Everything here is written to survive half-typed input, since it runs on
 * every keystroke: unterminated strings, unbalanced braces and missing
 * values must never throw, only degrade.
 */

export type TokKind = 'ws' | 'comment' | 'punct' | 'string' | 'number' | 'name' | 'variable' | 'spread' | 'other';

export type TokRole =
  | 'keyword'   // query, mutation, subscription, fragment, on
  | 'opname'    // operation / fragment name
  | 'field'
  | 'alias'
  | 'arg'       // argument name (also input-object field names inside argument values)
  | 'type'      // type names in variable definitions and type conditions
  | 'directive'
  | 'literal'   // true, false, null
  | 'enum';     // any other bare name used as a value

export interface GqlToken {
  text: string;
  start: number;
  end: number;
  kind: TokKind;
  role?: TokRole;
  /** A field or argument that the schema says does not exist at this position. */
  unknown?: boolean;
}

export interface UnknownName {
  name: string;
  /** Type (for fields) or field (for arguments) the name was looked up in. */
  owner: string;
  what: 'field' | 'arg';
}

export interface Analysis {
  tokens: GqlToken[];
  /** Set when the document contains an operation this mode must not send. */
  blocked: 'mutation' | 'subscription' | null;
  unknown: UnknownName[];
}

export interface SuggestionItem {
  name: string;
  /** Text inserted when the item is picked. */
  insert: string;
  /** Right-hand hint: the SDL type, e.g. "[DataPoint!]!". */
  detail: string;
}

export interface Suggestions {
  kind: 'fields' | 'args';
  /** Type whose fields, or field whose arguments, are being suggested. */
  owner: string;
  items: SuggestionItem[];
  /** Range of the source the picked item replaces (the partially typed word). */
  from: number;
  to: number;
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

// One alternative per lexical token of the GraphQL spec, tried left to right
// at each position. Block strings come before plain strings (both start with
// a quote); both accept a missing closing quote so a string still being
// typed colours as a string instead of breaking everything after it. The
// last alternative is a catch-all so no typed character is ever dropped from
// the highlighted output. The comma is its own punctuator here even though
// GraphQL treats it as insignificant whitespace - the walker skips it.
const TOKEN_RE = /([ \t\r\n\uFEFF]+)|(#[^\r\n]*)|("""(?:\\"""|[\s\S])*?(?:"""|$))|("(?:[^"\\\r\n]|\\.)*"?)|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|(\$[_A-Za-z][_0-9A-Za-z]*|\$)|([_A-Za-z][_0-9A-Za-z]*)|(\.\.\.)|([!&():=@\[\]{|},])|([\s\S])/g;

const KIND_BY_GROUP: TokKind[] = ['ws', 'ws', 'comment', 'string', 'string', 'number', 'variable', 'name', 'spread', 'punct', 'other'];

export function tokenize(src: string): GqlToken[] {
  const tokens: GqlToken[] = [];
  if (!src) {
    return tokens;
  }
  TOKEN_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = TOKEN_RE.exec(src)) !== null) {
    let group = 1;
    while (group < m.length && m[group] === undefined) {
      group++;
    }
    tokens.push({ text: m[0], start: m.index, end: m.index + m[0].length, kind: KIND_BY_GROUP[group] });
    // No alternative can match the empty string, but never loop forever if that changes.
    if (m[0].length === 0) {
      TOKEN_RE.lastIndex++;
    }
  }
  return tokens;
}

// ---------------------------------------------------------------------------
// Walker
// ---------------------------------------------------------------------------

interface SelFrame {
  kind: 'sel';
  /** Object type whose fields are valid here; null when unknown (no schema, unknown parent field). */
  type: string | null;
  used: Set<string>;
  lastField: GqlField | null;
  lastWasDirective: boolean;
  expect: null | 'directive' | 'spread' | 'inlineType';
  inlineType: string | null;
}

interface ArgsFrame {
  kind: 'args';
  /** Field whose arguments these are; null for directive arguments or an unknown field. */
  field: GqlField | null;
  used: Set<string>;
  expect: 'name' | 'colon' | 'value';
}

interface ValueFrame {
  kind: 'value';
  close: ']' | '}';
}

interface VarDefsFrame {
  kind: 'vardefs';
}

type Frame = SelFrame | ArgsFrame | ValueFrame | VarDefsFrame;

interface WalkState {
  stack: Frame[];
  blocked: 'mutation' | 'subscription' | null;
  unknown: UnknownName[];
}

const OPERATION_KEYWORDS = new Set(['query', 'mutation', 'subscription']);
const LITERALS = new Set(['true', 'false', 'null']);

function isSignificant(t: GqlToken): boolean {
  return t.kind !== 'ws' && t.kind !== 'comment' && !(t.kind === 'punct' && t.text === ',');
}

function selFrame(type: string | null): SelFrame {
  return { kind: 'sel', type, used: new Set(), lastField: null, lastWasDirective: false, expect: null, inlineType: null };
}

/**
 * Walks the significant tokens once, assigning roles and flagging unknown
 * names in place. Returns the stack as it stands after the last token, which
 * is what the suggestions list reads: "what is valid at this position".
 */
function walk(tokens: GqlToken[], schema: GqlSchema | null): WalkState {
  const sig = tokens.filter(isSignificant);
  const state: WalkState = { stack: [], blocked: null, unknown: [] };
  const stack = state.stack;

  // Top-level (outside any braces) state: which root type the next "{" opens.
  let rootType: string | null = null;
  let topExpect: null | 'opname' | 'fragname' | 'on' | 'fragtype' | 'directive' = null;

  const resetTop = () => {
    rootType = null;
    topExpect = null;
  };

  // A value just finished inside an argument list: the next thing is another argument name.
  const valueDone = () => {
    const top = stack[stack.length - 1];
    if (top && top.kind === 'args') {
      top.expect = 'name';
    }
  };

  for (let i = 0; i < sig.length; i++) {
    const t = sig[i];
    const next = sig[i + 1];
    const top = stack[stack.length - 1];

    // ----- outside any braces --------------------------------------------
    if (!top) {
      if (t.kind === 'name') {
        if (topExpect === 'opname' || topExpect === 'fragname') {
          t.role = 'opname';
          topExpect = topExpect === 'fragname' ? 'on' : null;
        } else if (topExpect === 'on' && t.text === 'on') {
          t.role = 'keyword';
          topExpect = 'fragtype';
        } else if (topExpect === 'fragtype') {
          t.role = 'type';
          rootType = t.text;
          topExpect = null;
        } else if (topExpect === 'directive') {
          t.role = 'directive';
          topExpect = null;
        } else if (OPERATION_KEYWORDS.has(t.text)) {
          t.role = 'keyword';
          if (t.text === 'mutation' || t.text === 'subscription') {
            state.blocked = state.blocked ?? t.text;
          }
          const key = (t.text + 'Type') as 'queryType' | 'mutationType' | 'subscriptionType';
          rootType = schema ? schema[key] : null;
          topExpect = 'opname';
        } else if (t.text === 'fragment') {
          t.role = 'keyword';
          topExpect = 'fragname';
        }
      } else if (t.text === '(') {
        stack.push({ kind: 'vardefs' });
        topExpect = null;
      } else if (t.text === '@') {
        topExpect = 'directive';
      } else if (t.text === '{') {
        // "{ ... }" with no keyword is the query shorthand.
        const opened = rootType ?? (schema ? schema.queryType : null);
        stack.push(selFrame(opened));
        topExpect = null;
      }
      continue;
    }

    // ----- variable definitions: ($id: ID!, $n: Int = 10) ----------------
    if (top.kind === 'vardefs') {
      if (t.text === ')') {
        stack.pop();
      } else if (t.kind === 'name') {
        t.role = LITERALS.has(t.text) ? 'literal' : 'type';
      }
      continue;
    }

    // ----- list / object values inside arguments -------------------------
    if (top.kind === 'value') {
      if (t.text === '[') {
        stack.push({ kind: 'value', close: ']' });
      } else if (t.text === '{') {
        stack.push({ kind: 'value', close: '}' });
      } else if (t.text === top.close) {
        stack.pop();
        if (stack[stack.length - 1]?.kind === 'args') {
          valueDone();
        }
      } else if (t.kind === 'name') {
        t.role = next?.text === ':' ? 'arg' : (LITERALS.has(t.text) ? 'literal' : 'enum');
      }
      continue;
    }

    // ----- argument list: (name: value, ...) -----------------------------
    if (top.kind === 'args') {
      if (t.text === ')') {
        stack.pop();
      } else if (t.text === ':') {
        top.expect = 'value';
      } else if (top.expect === 'value') {
        if (t.text === '[') {
          stack.push({ kind: 'value', close: ']' });
        } else if (t.text === '{') {
          stack.push({ kind: 'value', close: '}' });
        } else {
          if (t.kind === 'name') {
            t.role = LITERALS.has(t.text) ? 'literal' : 'enum';
          }
          top.expect = 'name';
        }
      } else if (t.kind === 'name') {
        t.role = 'arg';
        top.used.add(t.text);
        if (top.field && !top.field.args.some(a => a.name === t.text)) {
          t.unknown = true;
          state.unknown.push({ name: t.text, owner: top.field.name, what: 'arg' });
        }
        top.expect = 'colon';
      }
      continue;
    }

    // ----- selection set: { field(args) { ... } ... } --------------------
    if (t.kind === 'name') {
      if (top.expect === 'directive') {
        t.role = 'directive';
        top.expect = null;
        top.lastWasDirective = true;
        continue;
      }
      if (top.expect === 'spread') {
        if (t.text === 'on') {
          t.role = 'keyword';
          top.expect = 'inlineType';
        } else {
          t.role = 'opname'; // ...NamedFragment
          top.expect = null;
        }
        continue;
      }
      if (top.expect === 'inlineType') {
        t.role = 'type';
        top.inlineType = t.text;
        top.expect = null;
        continue;
      }
      top.lastWasDirective = false;
      if (next?.text === ':') {
        t.role = 'alias'; // alias: realField
        continue;
      }
      t.role = 'field';
      top.used.add(t.text);
      const typeDef = schema && top.type ? schema.types.get(top.type) : undefined;
      const fieldDef = typeDef?.fields.get(t.text) ?? null;
      if (typeDef && !fieldDef && t.text !== '__typename') {
        t.unknown = true;
        state.unknown.push({ name: t.text, owner: typeDef.name, what: 'field' });
      }
      top.lastField = fieldDef;
    } else if (t.kind === 'spread') {
      top.expect = 'spread';
    } else if (t.text === '@') {
      top.expect = 'directive';
    } else if (t.text === '(') {
      stack.push({ kind: 'args', field: top.lastWasDirective ? null : top.lastField, used: new Set(), expect: 'name' });
      top.lastWasDirective = false;
    } else if (t.text === '{') {
      if (top.inlineType) {
        stack.push(selFrame(top.inlineType));
        top.inlineType = null;
      } else {
        stack.push(selFrame(top.lastField ? namedType(top.lastField.type) : null));
      }
    } else if (t.text === '}') {
      stack.pop();
      if (stack.length === 0) {
        resetTop();
      }
    }
  }

  return state;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export function analyze(src: string, schema: GqlSchema | null): Analysis {
  const tokens = tokenize(src);
  const { blocked, unknown } = walk(tokens, schema);
  return { tokens, blocked, unknown };
}

/**
 * What can be typed at `cursor`: the fields of the enclosing selection set,
 * or the remaining arguments of the enclosing field. Null when there is
 * nothing meaningful to suggest (no schema, inside a string or comment,
 * typing a value, unknown enclosing type, ...).
 */
export function suggestAt(src: string, cursor: number, schema: GqlSchema | null): Suggestions | null {
  if (!schema) {
    return null;
  }
  const tokens = tokenize(src);

  // The word being typed (if the cursor touches one) is excluded from the
  // walk and becomes the filter prefix; picking an item replaces it whole.
  let from = cursor;
  let to = cursor;
  let prefix = '';
  for (const t of tokens) {
    if (t.start < cursor && cursor <= t.end) {
      if (t.kind === 'string' || t.kind === 'comment') {
        return null;
      }
      if (t.kind === 'name') {
        from = t.start;
        to = t.end;
        prefix = src.slice(t.start, cursor).toLowerCase();
      }
      break;
    }
  }

  const before = tokens.filter(t => t.end <= from);
  const { stack } = walk(before, schema);
  const top = stack[stack.length - 1];
  if (!top) {
    return null;
  }

  const matches = (name: string) => name.toLowerCase().startsWith(prefix);

  if (top.kind === 'sel' && top.type && top.expect === null) {
    const typeDef = schema.types.get(top.type);
    if (!typeDef) {
      return null;
    }
    const items = [...typeDef.fields.values()]
      .filter(f => !top.used.has(f.name) && matches(f.name))
      .map(f => ({
        name: f.name,
        insert: f.name,
        detail: (f.args.length ? '(…): ' : '') + typeToString(f.type),
      }));
    return { kind: 'fields', owner: typeDef.name, items, from, to };
  }

  if (top.kind === 'args' && top.field && top.expect === 'name') {
    const items = top.field.args
      .filter(a => !top.used.has(a.name) && matches(a.name))
      .map(a => ({ name: a.name, insert: a.name + ': ', detail: typeToString(a.type) }));
    return { kind: 'args', owner: top.field.name, items, from, to };
  }

  return null;
}
