import { Component, ElementRef, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { GqlSchema } from './graphql-schema';
import { GraphqlSchemaService } from './graphql-schema.service';
import { Analysis, GqlToken, SuggestionItem, Suggestions, UnknownName, analyze, suggestAt } from './graphql-analyzer';

interface GqlExample {
  label: string;
  query: string;
}

interface RenderedToken {
  text: string;
  color: string;
  weight: string;
  italic: string;
  unknown: boolean;
}

export type SchemaState = 'loading' | 'ready' | 'unavailable';

/** How many "unknown field/argument" warnings to list under the box before summarizing. */
const MAX_UNKNOWN_SHOWN = 3;

/**
 * GraphQL counterpart of SqlEditorComponent, built the same way and on
 * purpose without an editor library: a highlighted <pre> behind a
 * transparent-text <textarea>, plus a suggestions list UNDER the box (not a
 * popup at the caret - a textarea doesn't expose caret coordinates, and the
 * list below matches the example chips the SQL editor already has).
 *
 * The suggestions and the "unknown field" warnings come from the backend's
 * own schema, fetched through introspection by GraphqlSchemaService. When
 * introspection isn't available the editor keeps working with colours only,
 * plus the mutation guard (which is purely syntactic) - see SchemaState.
 */
@Component({
  selector: 'bx-graphql-editor',
  templateUrl: './graphql-editor.component.html',
  styleUrls: ['./graphql-editor.component.scss'],
  standalone: false
})
export class GraphqlEditorComponent implements OnInit {

  @ViewChild('area', { static: true }) area!: ElementRef<HTMLTextAreaElement>;
  @ViewChild('highlight', { static: true }) highlight!: ElementRef<HTMLPreElement>;

  /** GraphQL endpoint to introspect (the same one queries are sent to). */
  @Input() endpoint = '';

  @Input() placeholder = `query {
  datapoints(survey: "nama_10r_3gdp", limit: 5) {
    survey
    dimensions
    value
  }
}`;

  // Taken from Query-Engine/examples/GraphQL (ExampleQuery1, 2 and 5), the
  // last one with its $pilot_nuts3 variable inlined - this editor has no
  // variables panel yet. Every datapoints example passes `survey`: see the
  // note in the backend's buildCachePrefix() for why a datapoints query with
  // neither `source` nor `survey` (nor `dimensions`) currently fails there.
  @Input() examples: GqlExample[] = [
    { label: 'Sorgenti disponibili', query: `query {
  sources {
    id
    name
  }
}` },
    { label: 'Acquisti online — Severozapaden', query: `query {
  datapoints(
    survey: "ISOC_R_BLT12_I"
    sortBy: ["year"]
    sortOrder: "desc"
    dimensions: [
      "Severozapaden"
      "Last online purchase: in the last 3 months"
      "Percentage of individuals"
    ]
    limit: 1
  ) {
    region
    source
    timestamp
    survey
    dimensions
    value
  }
}` },
    { label: 'PIL per abitante — Lovech', query: `query {
  datapoints(
    survey: "nama_10r_3gdp"
    sortBy: "year"
    sortOrder: "asc"
    dimensions: ["Lovech", "Euro per inhabitant"]
  ) {
    region
    survey
    dimensions
    value
  }
}` },
    { label: 'Variazione popolazione — Trento', query: `query {
  datapoints(
    survey: "demo_r_gind3"
    sortBy: "year"
    sortOrder: "asc"
    dimensions: ["Trento", "Total population change"]
  ) {
    region
    survey
    dimensions
    value
  }
}` },
  ];

  @Output() valueChange = new EventEmitter<string>();
  /** Emitted from user edits only, so the parent never changes state mid change-detection. */
  @Output() blockedChange = new EventEmitter<boolean>();

  schemaState: SchemaState = 'loading';
  tokens: RenderedToken[] = [];
  blocked: 'mutation' | 'subscription' | null = null;
  unknownShown: UnknownName[] = [];
  unknownHidden = 0;
  suggestions: Suggestions | null = null;

  private schema: GqlSchema | null = null;
  private _value = '';

  constructor(private schemaService: GraphqlSchemaService) { }

  // Same reasoning as SqlEditorComponent.value: everything derived from the
  // text is computed here, once per change, never from the template.
  @Input()
  set value(v: string) {
    this._value = v ?? '';
    this.reanalyze();
  }
  get value(): string {
    return this._value;
  }

  ngOnInit(): void {
    if (!this.endpoint) {
      this.schemaState = 'unavailable';
      return;
    }
    this.schemaService.load(this.endpoint).then(schema => {
      this.schema = schema;
      this.schemaState = schema ? 'ready' : 'unavailable';
      this.reanalyze();
      this.updateSuggestions();
    });
  }

  onInput(text: string): void {
    const wasBlocked = !!this.blocked;
    this.value = text;
    this.valueChange.emit(this._value);
    if (!!this.blocked !== wasBlocked) {
      this.blockedChange.emit(!!this.blocked);
    }
    this.updateSuggestions();
  }

  useExample(query: string): void {
    this.onInput(query);
    this.suggestions = null;
  }

  onKeyup(event: KeyboardEvent): void {
    // Text changes already go through onInput; this catches caret moves
    // (arrows, Home/End, PageUp/Down) that change what is valid to suggest.
    if (event.key.startsWith('Arrow') || event.key === 'Home' || event.key === 'End' || event.key.startsWith('Page')) {
      this.updateSuggestions();
    } else if (event.key === 'Escape') {
      this.suggestions = null;
    }
  }

  updateSuggestions(): void {
    const el = this.area?.nativeElement;
    if (!el || this.schemaState !== 'ready' || el.selectionStart !== el.selectionEnd) {
      this.suggestions = null;
      return;
    }
    this.suggestions = suggestAt(this._value, el.selectionStart, this.schema);
  }

  pick(item: SuggestionItem): void {
    if (!this.suggestions) {
      return;
    }
    const { from, to } = this.suggestions;
    const text = this._value.slice(0, from) + item.insert + this._value.slice(to);
    const caret = from + item.insert.length;
    this.onInput(text);
    // ngModel writes the new text into the textarea on the next change
    // detection pass; place the caret after it has.
    setTimeout(() => {
      const el = this.area.nativeElement;
      el.focus();
      el.setSelectionRange(caret, caret);
      this.updateSuggestions();
    });
  }

  /** The <pre> can't scroll on its own (pointer-events: none) - it follows the textarea. */
  syncScroll(): void {
    this.highlight.nativeElement.scrollTop = this.area.nativeElement.scrollTop;
    this.highlight.nativeElement.scrollLeft = this.area.nativeElement.scrollLeft;
  }

  private reanalyze(): void {
    const analysis: Analysis = analyze(this._value, this.schema);
    this.blocked = analysis.blocked;
    this.tokens = analysis.tokens.map(render);
    // A textarea ending in a newline is one line taller than the <pre>
    // rendering the same text (a trailing \n has no height of its own), which
    // would throw the two out of step at the bottom once scrolled.
    if (this._value.endsWith('\n')) {
      this.tokens.push({ text: ' ', color: 'inherit', weight: '400', italic: 'normal', unknown: false });
    }

    const seen = new Set<string>();
    const unique = analysis.unknown.filter(u => {
      const key = u.what + ':' + u.owner + ':' + u.name;
      return !seen.has(key) && !!seen.add(key);
    });
    this.unknownShown = unique.slice(0, MAX_UNKNOWN_SHOWN);
    this.unknownHidden = Math.max(0, unique.length - MAX_UNKNOWN_SHOWN);
  }
}

function render(t: GqlToken): RenderedToken {
  let color = 'inherit';
  let weight = '400';
  let italic = 'normal';

  switch (t.kind) {
    case 'comment': color = 'var(--ds-text-faint)'; italic = 'italic'; break;
    case 'string': color = 'var(--ds-str)'; break;
    case 'number': color = 'var(--ds-num)'; break;
    case 'variable': color = 'var(--ds-num)'; weight = '500'; break;
    case 'punct':
    case 'spread': color = 'var(--ds-op)'; break;
  }
  switch (t.role) {
    case 'keyword': color = 'var(--ds-kw)'; weight = '600'; break;
    case 'type': color = 'var(--ds-kw)'; break;
    case 'opname':
    case 'arg':
    case 'directive': color = 'var(--ds-fn)'; break;
    case 'alias': color = 'var(--ds-text-muted)'; break;
    case 'literal':
    case 'enum': color = 'var(--ds-num)'; break;
  }
  return { text: t.text, color, weight, italic, unknown: !!t.unknown };
}
