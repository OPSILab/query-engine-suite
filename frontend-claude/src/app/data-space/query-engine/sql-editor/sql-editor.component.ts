import { Component, EventEmitter, Input, Output } from '@angular/core';

interface SqlExample {
  label: string;
  sql: string;
}

interface SqlToken {
  text: string;
  color: string;
  weight: string;
}

const KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'AS', 'LATERAL', 'UNION', 'ALL', 'ON', 'JOIN',
  'AND', 'OR', 'ORDER', 'BY', 'LIMIT',
]);

// One master regex, alternatives tried left to right at each position:
// whitespace, a quoted string, a number, a word (captures a trailing "("
// separately so it can be colored as a function call), one of the common
// SQL operators, then a catch-all single character so nothing typed is
// ever silently dropped from the highlighted output.
const TOKEN_RE = /(\s+)|('(?:[^']|'')*')|(\d+(?:\.\d+)?)|([A-Za-z_][A-Za-z0-9_]*)(\()?|(->>|->|<=|>=|<>|!=|[(),.;*=<>+/-])|([\s\S])/g;

/**
 * Native Angular replacement for the old vanilla HTML/JS/CSS "autocomplete"
 * editor that used to live in src/assets/autocomplete (loaded through an
 * <iframe> + window.postMessage bridge in the original QueryEngineComponent).
 *
 * Two things kept from that widget, done properly this time:
 * - The hardcoded example queries, now with a short description instead of
 *   showing raw SQL as the chip label (several of the real examples all
 *   start with "SELECT *" on their own first line, which made the old
 *   first-line-preview approach show near-identical, useless chip labels).
 * - Some way to see SQL structure at a glance: real syntax highlighting via
 *   a small regex tokenizer (SqlEditorComponent.tokenize below), rendered
 *   as a <pre> of colored spans sitting behind a transparent-text
 *   <textarea> - a standard technique for a lightweight code editor without
 *   pulling in CodeMirror/Monaco for four sample queries.
 */
@Component({
  selector: 'bx-sql-editor',
  templateUrl: './sql-editor.component.html',
  styleUrls: ['./sql-editor.component.scss'],
})
export class SqlEditorComponent {

  @Input() value = '';
  @Output() valueChange = new EventEmitter<string>();

  @Input() placeholder = "SELECT * FROM bucketName WHERE name = 'email/Data model mapper/file.json'";

  @Input() examples: SqlExample[] = [
    { label: 'Tutti i record — CARTAGENA', sql: 'SELECT * FROM CARTAGENA' },
    { label: 'Elementi annidati per id_amat', sql: `SELECT *
FROM cartagena,
    LATERAL (
      SELECT jsonb_array_elements(data) AS element
      WHERE jsonb_typeof(data) = 'array'
      UNION ALL SELECT data AS element
      WHERE jsonb_typeof(data) = 'object'
    ) AS subquery
WHERE subquery.element->>'id_amat' = '9001'` },
    { label: 'Coppie chiave/valore annidate', sql: `SELECT *
FROM example_table, jsonb_array_elements(data) AS array_element,
 jsonb_each(array_element) AS nested_object
WHERE nested_object.value->>'a' = 'a3'` },
    { label: 'Feature GeoJSON per fid', sql: `SELECT *
FROM cartagena,
     LATERAL (
         SELECT jsonb_array_elements(data->'features') AS element
         WHERE jsonb_typeof(data->'features') = 'array'
     ) AS subquery
WHERE subquery.element->'properties'->>'fid' = '11';` },
  ];

  onInput(text: string): void {
    this.value = text;
    this.valueChange.emit(this.value);
  }

  useExample(sql: string): void {
    this.onInput(sql);
  }

  tokenize(sql: string): SqlToken[] {
    const tokens: SqlToken[] = [];
    if (!sql) return tokens;

    TOKEN_RE.lastIndex = 0;
    let m: RegExpExecArray | null;
    while ((m = TOKEN_RE.exec(sql)) !== null) {
      if (m[1] !== undefined) {
        tokens.push({ text: m[1], color: 'inherit', weight: '400' });
      } else if (m[2] !== undefined) {
        tokens.push({ text: m[2], color: 'var(--ds-str)', weight: '400' });
      } else if (m[3] !== undefined) {
        tokens.push({ text: m[3], color: 'var(--ds-num)', weight: '400' });
      } else if (m[4] !== undefined) {
        const word = m[4];
        if (m[5] === '(') {
          tokens.push({ text: word, color: 'var(--ds-fn)', weight: '400' });
          tokens.push({ text: '(', color: 'var(--ds-op)', weight: '400' });
        } else if (KEYWORDS.has(word.toUpperCase())) {
          tokens.push({ text: word, color: 'var(--ds-kw)', weight: '600' });
        } else {
          tokens.push({ text: word, color: 'inherit', weight: '400' });
        }
      } else if (m[6] !== undefined) {
        tokens.push({ text: m[6], color: 'var(--ds-op)', weight: '400' });
      } else if (m[7] !== undefined) {
        tokens.push({ text: m[7], color: 'var(--ds-op)', weight: '400' });
      }
      // TOKEN_RE has no alternative that can match a zero-length string,
      // but guard against an infinite loop if that ever changes.
      if (m[0].length === 0) {
        TOKEN_RE.lastIndex += 1;
      }
    }
    return tokens;
  }
}
