import { Component, Input, OnChanges } from '@angular/core';

// Nesting levels rendered open; deeper tables start collapsed behind a
// "{…} 5 keys" / "[…] 12 items" button, so a deep document can't turn the
// card into a table ten levels wide. Compact values (arrays of primitives,
// numeric matrices) are shown at any depth: they take one line anyway.
const MAX_OPEN_DEPTH = 3;
// Rows (array items, object keys) rendered at a time; more are added on
// request, PAGE at a time. Results can hold thousands of datapoints.
const PAGE = 50;
// Values of an array of primitives shown inline before "+N".
const INLINE_ITEMS = 20;
// Longer strings are cut (the JSON view always has the full value).
const MAX_STRING = 500;
// Characters of a numeric matrix (GeoJSON coordinates, ...) shown inline.
const MAX_NUMERIC_TEXT = 120;
// Columns of an array-of-objects table: the union of the items' keys, capped.
const MAX_COLUMNS = 40;

/** A primitive, rendered in place: its text and a jt-* class for its type. */
export type JsonPrim = { prim: true; text: string; cls: string };
/** A table cell: either a primitive or a nested value. */
export type JsonCell = JsonPrim | { prim: false; value: any };

type Kind = 'primitive' | 'empty' | 'object' | 'list' | 'rows' | 'inline' | 'numeric';

/**
 * Renders any JSON value as tables, recursively:
 * - object -> two-column key/value table;
 * - array of objects (CSV rows, datapoints, ...) -> one table, columns = the
 *   union of the items' keys, one row per item, missing cells left empty;
 * - array of primitives -> the values inline;
 * - array of arrays of numbers (GeoJSON coordinates, ...) -> compact text plus
 *   a count, since as nested tables it would be an unreadable grid;
 * - any other array -> index/value table.
 * Primitives are rendered by the parent's template, not by a nested instance
 * of this component, to keep the component count down on large results.
 */
@Component({
  selector: 'ds-json-table',
  templateUrl: './json-table.component.html',
  styleUrls: ['./json-table.component.scss'],
  standalone: false
})
export class JsonTableComponent implements OnChanges {
  @Input() value: any;
  @Input() depth = 0;

  kind: Kind = 'primitive';
  prim?: JsonPrim;
  emptyText = '';

  collapsed = false;
  isArray = false;
  size = 0;

  // object / list
  entries: { key: string; cell: JsonCell }[] = [];
  // rows
  columns: string[] = [];
  hiddenColumns = 0;
  rows: JsonCell[][] = [];
  // inline
  inlineItems: JsonPrim[] = [];
  // numeric
  numericText = '';
  numericCount = 0;
  numericArePoints = false;

  // Paging, for object / list / rows / inline.
  total = 0;
  shown = 0;

  get remaining(): number {
    return this.total - this.shown;
  }

  get nextCount(): number {
    return Math.min(PAGE, this.remaining);
  }

  ngOnChanges(): void {
    const v = this.value;
    this.isArray = Array.isArray(v);
    this.size = isComposite(v) ? (this.isArray ? v.length : Object.keys(v).length) : 0;
    this.build();
    this.collapsed = this.depth >= MAX_OPEN_DEPTH
      && (this.kind === 'object' || this.kind === 'list' || this.kind === 'rows');
  }

  expand(): void {
    this.collapsed = false;
  }

  showMore(): void {
    this.shown = Math.min(this.total, this.shown + PAGE);
    this.fill();
  }

  private build(): void {
    const v = this.value;
    this.entries = [];
    this.rows = [];
    this.inlineItems = [];
    if (!isComposite(v)) {
      this.kind = 'primitive';
      this.prim = cell(v) as JsonPrim;
      return;
    }
    if (this.size === 0) {
      this.kind = 'empty';
      this.emptyText = this.isArray ? '[ ]' : '{ }';
      return;
    }
    if (!this.isArray) {
      this.kind = 'object';
      this.total = this.size;
      this.shown = Math.min(PAGE, this.total);
      this.fill();
      return;
    }
    const arr: any[] = v;
    if (isNumericMatrix(arr)) {
      this.kind = 'numeric';
      const json = JSON.stringify(arr).replace(/,/g, ', ');
      this.numericText = json.length > MAX_NUMERIC_TEXT ? json.slice(0, MAX_NUMERIC_TEXT) + ' …' : json;
      const inner = innermostArrays(arr);
      this.numericArePoints = inner.length > 0 && inner.every(a => a.length === 2 || a.length === 3);
      this.numericCount = this.numericArePoints ? inner.length : countNumbers(arr);
      return;
    }
    if (arr.every(isPlainObject)) {
      this.kind = 'rows';
      const cols: string[] = [];
      const seen = new Set<string>();
      for (const item of arr) {
        for (const k of Object.keys(item)) {
          if (!seen.has(k)) {
            seen.add(k);
            cols.push(k);
          }
        }
      }
      this.columns = cols.slice(0, MAX_COLUMNS);
      this.hiddenColumns = cols.length - this.columns.length;
      this.total = arr.length;
      this.shown = Math.min(PAGE, this.total);
      this.fill();
      return;
    }
    if (arr.every(x => !isComposite(x))) {
      this.kind = 'inline';
      this.total = arr.length;
      this.shown = Math.min(INLINE_ITEMS, this.total);
      this.fill();
      return;
    }
    this.kind = 'list';
    this.total = arr.length;
    this.shown = Math.min(PAGE, this.total);
    this.fill();
  }

  // (Re)computes the cells for the first `shown` rows only.
  private fill(): void {
    const v = this.value;
    switch (this.kind) {
      case 'object':
        this.entries = Object.keys(v).slice(0, this.shown).map(k => ({ key: k, cell: cell(v[k]) }));
        break;
      case 'list':
        this.entries = (v as any[]).slice(0, this.shown).map((x, i) => ({ key: String(i), cell: cell(x) }));
        break;
      case 'rows':
        this.rows = (v as any[]).slice(0, this.shown).map(item =>
          this.columns.map(c => (Object.prototype.hasOwnProperty.call(item, c) ? cell(item[c]) : MISSING)));
        break;
      case 'inline':
        this.inlineItems = (v as any[]).slice(0, this.shown).map(x => cell(x) as JsonPrim);
        break;
    }
  }
}

const MISSING: JsonPrim = { prim: true, text: '', cls: 'jt-missing' };

function isComposite(v: any): boolean {
  return v !== null && typeof v === 'object';
}

function isPlainObject(v: any): boolean {
  return isComposite(v) && !Array.isArray(v);
}

function cell(v: any): JsonCell {
  if (isComposite(v)) {
    return { prim: false, value: v };
  }
  if (v === null || v === undefined) {
    return { prim: true, text: 'null', cls: 'jt-null' };
  }
  switch (typeof v) {
    case 'number':
    case 'bigint':
      return { prim: true, text: String(v), cls: 'jt-num' };
    case 'boolean':
      return { prim: true, text: String(v), cls: 'jt-bool' };
    default: {
      const s = String(v);
      if (s === '') {
        return { prim: true, text: '""', cls: 'jt-null' };
      }
      return { prim: true, text: s.length > MAX_STRING ? s.slice(0, MAX_STRING) + ' …' : s, cls: 'jt-str' };
    }
  }
}

// An array that contains at least one array and whose leaves are all numbers.
function isNumericMatrix(arr: any[]): boolean {
  let hasArray = false;
  const walk = (a: any[]): boolean => a.every(x => {
    if (Array.isArray(x)) {
      hasArray = true;
      return walk(x);
    }
    return typeof x === 'number';
  });
  return walk(arr) && hasArray;
}

function innermostArrays(arr: any[]): any[][] {
  const out: any[][] = [];
  const walk = (a: any[]) => {
    if (a.every(x => typeof x === 'number')) {
      out.push(a);
    } else {
      a.forEach(x => Array.isArray(x) && walk(x));
    }
  };
  walk(arr);
  return out;
}

function countNumbers(arr: any[]): number {
  return arr.reduce((n, x) => n + (Array.isArray(x) ? countNumbers(x) : 1), 0);
}
