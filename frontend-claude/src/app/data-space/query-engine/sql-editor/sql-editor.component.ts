import { Component, EventEmitter, Input, Output } from '@angular/core';

/**
 * Native Angular replacement for the old vanilla HTML/JS/CSS "autocomplete"
 * editor that used to live in src/assets/autocomplete (loaded through an
 * <iframe> + window.postMessage bridge in the original QueryEngineComponent).
 *
 * It keeps the one thing worth keeping from that widget: a hardcoded list of
 * example queries, shown as clickable chips, since the underlying data model
 * is not obvious and users need a starting point. Everything else (the
 * ghost-text ahead-of-cursor autocomplete, the iframe, the postMessage
 * bridge) is gone - there's no reason for a same-origin, same-app widget to
 * talk to its host through postMessage instead of an @Output.
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

  @Input() suggestions: string[] = [
    'SELECT * FROM CARTAGENA',
    `SELECT *
FROM cartagena,
    LATERAL (
      SELECT jsonb_array_elements(data) AS element
      WHERE jsonb_typeof(data) = 'array'
      UNION ALL SELECT data AS element
      WHERE jsonb_typeof(data) = 'object'
    ) AS subquery
WHERE subquery.element->>'id_amat' = '9001'`,
    `SELECT *
FROM example_table, jsonb_array_elements(data) AS array_element,
 jsonb_each(array_element) AS nested_object
WHERE nested_object.value->>'a' = 'a3'`,
    `SELECT *
FROM cartagena,
     LATERAL (
         SELECT jsonb_array_elements(data->'features') AS element
         WHERE jsonb_typeof(data->'features') = 'array'
     ) AS subquery
WHERE subquery.element->'properties'->>'fid' = '11';`,
  ];

  onInput(text: string): void {
    this.value = text;
    this.valueChange.emit(this.value);
  }

  useSuggestion(suggestion: string): void {
    this.onInput(suggestion);
  }

  // First line only, so the chip list stays readable for multi-line examples.
  previewOf(suggestion: string): string {
    return suggestion.split('\n')[0];
  }
}
