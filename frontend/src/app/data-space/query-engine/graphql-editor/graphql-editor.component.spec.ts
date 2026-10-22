import { describe, expect, it } from 'vitest';
import { GraphqlEditorComponent } from './graphql-editor.component';
import { analyze } from './graphql-analyzer';

const example = { label: 'All sources, all fields (find all)', query: 'query {\n  sourcesCount\n  sources {\n    id\n  }\n}' };

function create() {
  const editor = new GraphqlEditorComponent({} as any);
  const emitted: string[] = [];
  editor.valueChange.subscribe(v => emitted.push(v));
  return { editor, emitted };
}

describe('examples: their label as a comment above the query', () => {
  it('a click puts "# <label>" and the query in the editor', () => {
    const { editor, emitted } = create();
    editor.useExample(example);
    const text = '# All sources, all fields (find all)\nquery {\n  sourcesCount\n  sources {\n    id\n  }\n}';
    expect(editor.value).toBe(text);
    expect(emitted).toEqual([text]);
  });

  it('still a valid query (a comment, not an operation name): not blocked, nothing to complain about', () => {
    const text = GraphqlEditorComponent.exampleText({ label: 'Online purchase — Città (2026)', query: example.query });
    expect(text.split('\n')[0]).toBe('# Online purchase — Città (2026)');
    expect(analyze(text, null as any).blocked).toBeNull();
  });

  it('the example inserted is the selected one, until the text is edited; a label on several lines stays one comment', () => {
    const { editor } = create();
    editor.useExample(example);
    expect(editor.isActiveExample(example)).toBe(true);
    expect(editor.isActiveExample({ ...example, label: 'Other' })).toBe(false);
    editor.onInput(editor.value + '\n');
    expect(editor.isActiveExample(example)).toBe(false);
    expect(GraphqlEditorComponent.exampleText({ label: 'Two\nlines', query: '{ a }' })).toBe('# Two lines\n{ a }');
  });
});
