import { afterEach, describe, expect, it, vi } from 'vitest';
import { AutocompleteComponent } from './autocomplete.component';
import { SUGGESTIONS_PAGE_SIZE } from '../../../services/be-open.service';

// Paged backend over in-memory keys / values / entries (prefix and exact matches case insensitive, like the
// Query-Engine). Records every call.
function fakeApi(data: { keys?: string[]; values?: string[]; entries?: { key: string; value: string }[] } = {}, fail = false, pageSize = SUGGESTIONS_PAGE_SIZE) {
  const calls: any[] = [];
  const starts = (text: string, prefix: string) => text.toLowerCase().startsWith(prefix.toLowerCase());
  const equals = (a: string, b: string) => a.toLowerCase() === b.toLowerCase();
  const page = <T>(all: T[], skip: number, limit: number) => ({ items: all.slice(skip, skip + limit), hasMore: all.length > skip + limit });
  return {
    calls,
    suggestionsPageSize: () => pageSize,
    getKeys: vi.fn(async (prefix = '', skip = 0, limit = SUGGESTIONS_PAGE_SIZE, collections?: string[], _format?: string) => {
      calls.push(['keys', prefix, skip]);
      if (collections) calls.push(['collections', collections]);
      if (fail) throw new Error('HTTP 500');
      return page((data.keys ?? []).filter(k => starts(k, prefix)), skip, limit);
    }),
    getValues: vi.fn(async (prefix = '', skip = 0, limit = SUGGESTIONS_PAGE_SIZE) => {
      calls.push(['values', prefix, skip]);
      return page((data.values ?? []).filter(v => starts(v, prefix)), skip, limit);
    }),
    getEntries: vi.fn(async (key = '', value = '', skip = 0, limit = SUGGESTIONS_PAGE_SIZE, exact: { key?: boolean; value?: boolean } = {}, collections?: string[], _format?: string) => {
      calls.push(['entries', key, value, skip, exact]);
      if (collections) calls.push(['collections', collections]);
      const match = (field: string, text: string, isExact?: boolean) => isExact ? equals(field, text) : starts(field, text);
      return page((data.entries ?? []).filter(e => match(e.key, key, exact.key) && match(e.value, value, exact.value)), skip, limit);
    }),
  };
}

const created: AutocompleteComponent[] = [];

function create(mode: 'key' | 'value', api = fakeApi(), init: Partial<AutocompleteComponent> = {}) {
  const host = document.createElement('div');
  const textarea = document.createElement('textarea');
  host.appendChild(textarea);
  document.body.appendChild(host);
  const cdr = { markForCheck: vi.fn() };
  const comp = new AutocompleteComponent(api as any, cdr as any, { nativeElement: host } as any);
  comp.input = { nativeElement: textarea };
  comp.mode = mode;
  Object.assign(comp, init);
  created.push(comp);
  const emitted = { output: [] as any[], ver: [] as boolean[] };
  comp.output.subscribe(v => emitted.output.push(v));
  comp.ver.subscribe(v => emitted.ver.push(v));
  return { comp, api, textarea, host, emitted };
}

const settle = () => new Promise(resolve => setTimeout(resolve));

async function type(comp: AutocompleteComponent, textarea: HTMLTextAreaElement, text: string) {
  textarea.value = text;
  comp.onInputEvent();
  await settle();
}

afterEach(() => {
  created.splice(0).forEach(comp => comp.ngOnDestroy());
  document.body.innerHTML = '';
  document.body.className = '';
});

describe('suggestions, other field empty', () => {
  it('focus on the empty field: first page of keys', async () => {
    const { comp, api } = create('key', fakeApi({ keys: ['city', 'country', 'name'] }));
    comp.onFocus();
    expect(comp.loadingSuggestions).toBe(true);
    await settle();
    expect(comp.suggestions).toEqual(['city', 'country', 'name']);
    expect(comp.hasMore).toBe(false);
    expect(comp.loadingSuggestions).toBe(false);
    expect(api.calls).toEqual([['keys', '', 0]]);
  });

  it('typing: keys / values starting with the text; the text is sent to the parent at once', async () => {
    const keys = create('key', fakeApi({ keys: ['city', 'cityCode', 'name'] }));
    await type(keys.comp, keys.textarea, 'ci');
    expect(keys.comp.suggestions).toEqual(['city', 'cityCode']);
    expect(keys.emitted.output).toEqual(['ci']);
    const values = create('value', fakeApi({ values: ['Rome', 'rovigo', 'Milan'] }));
    await type(values.comp, values.textarea, 'RO');
    expect(values.comp.suggestions).toEqual(['Rome', 'rovigo']);
  });

  it('the file type (format) goes with every request: keys, values and entries', async () => {
    const api = fakeApi({ keys: ['city'], values: ['Rome'], entries: [{ key: 'city', value: 'Rome' }] });
    const keys = create('key', api, { format: 'CSV' });
    keys.comp.onFocus();
    await settle();
    const values = create('value', api, { format: 'CSV', key: 'city' });
    values.comp.onFocus();
    await settle();
    expect(api.getKeys.mock.calls[0][4]).toBe('CSV');
    expect(api.getEntries.mock.calls.every(c => c[6] === 'CSV')).toBe(true);
    expect(api.getEntries.mock.calls.length).toBeGreaterThan(0);
  });

  it('pages of the configured size (suggestionsPageSize): each one starts after what was read', async () => {
    const all = Array.from({ length: 7 }, (_, i) => 'k' + i);
    const { comp, api } = create('key', fakeApi({ keys: all }, false, 3));
    comp.onFocus();
    await settle();
    expect(comp.suggestions).toEqual(['k0', 'k1', 'k2']);
    await comp.loadMore();
    await comp.loadMore();
    expect(comp.suggestions).toEqual(all);
    expect(comp.hasMore).toBe(false);
    expect(api.calls.map(c => c[2])).toEqual([0, 3, 6]);
  });

  it('pages: "Load more" appends the next page, until there is no more', async () => {
    const all = Array.from({ length: SUGGESTIONS_PAGE_SIZE * 2 + 5 }, (_, i) => 'k' + String(i).padStart(3, '0'));
    const { comp, api } = create('key', fakeApi({ keys: all }));
    comp.onFocus();
    await settle();
    expect(comp.suggestions.length).toBe(SUGGESTIONS_PAGE_SIZE);
    expect(comp.hasMore).toBe(true);
    await comp.loadMore();
    await comp.loadMore();
    expect(comp.suggestions).toEqual(all);
    expect(comp.hasMore).toBe(false);
    await comp.loadMore(); // nothing more: no request
    expect(api.calls.map(c => c[2])).toEqual([0, SUGGESTIONS_PAGE_SIZE, SUGGESTIONS_PAGE_SIZE * 2]);
  });

  it('a new text starts again from the first page', async () => {
    const all = Array.from({ length: SUGGESTIONS_PAGE_SIZE + 1 }, (_, i) => 'a' + i);
    const { comp, textarea } = create('key', fakeApi({ keys: [...all, 'b1'] }));
    comp.onFocus();
    await settle();
    await comp.loadMore();
    await type(comp, textarea, 'b');
    expect(comp.suggestions).toEqual(['b1']);
  });

  it('answers of an older text are ignored', async () => {
    const api = fakeApi({ keys: ['alpha', 'beta'] });
    let release!: () => void;
    const slow = new Promise<void>(resolve => release = resolve);
    const original = api.getKeys.getMockImplementation()!;
    api.getKeys.mockImplementationOnce(async (...args: any[]) => { await slow; return original(...args); });
    const { comp, textarea } = create('key', api);
    textarea.value = 'a';
    comp.onInputEvent();         // slow
    await type(comp, textarea, 'b');
    release();
    await settle();
    expect(comp.suggestions).toEqual(['beta']);
  });

  it('backend error: no suggestions, no endless loading', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => { });
    const { comp } = create('key', fakeApi({}, true));
    comp.onFocus();
    await settle();
    expect(comp.suggestions).toEqual([]);
    expect(comp.loadingSuggestions).toBe(false);
  });
});

describe('preloaded first page (read by the parent at startup)', () => {
  const preloaded = (items: string[], hasMore = false) => Promise.resolve({ items, hasMore });

  it('focus on the empty field: the preloaded page, without a request; "Load more" goes on from it', async () => {
    const all = Array.from({ length: SUGGESTIONS_PAGE_SIZE + 2 }, (_, i) => 'k' + String(i).padStart(3, '0'));
    const { comp, api } = create('key', fakeApi({ keys: all }), { preloaded: preloaded(all.slice(0, SUGGESTIONS_PAGE_SIZE), true) });
    comp.onFocus();
    await Promise.resolve(); await Promise.resolve(); // microtasks only: ready before the next render
    expect(comp.suggestions.length).toBe(SUGGESTIONS_PAGE_SIZE);
    expect(api.calls).toEqual([]);
    await comp.loadMore();
    expect(comp.suggestions).toEqual(all);
    expect(api.calls).toEqual([['keys', '', SUGGESTIONS_PAGE_SIZE]]);
  });

  it('not used with some text, nor with the other field filled', async () => {
    const typed = create('value', fakeApi({ values: ['Rome', 'Milan'] }), { preloaded: preloaded(['Milan', 'Rome']) });
    await type(typed.comp, typed.textarea, 'R');
    expect(typed.comp.suggestions).toEqual(['Rome']);
    const other = create('value', fakeApi({ entries: [{ key: 'city', value: 'Rome' }] }), { preloaded: preloaded(['Milan']), key: 'city' });
    other.comp.onFocus();
    await settle();
    expect(other.comp.suggestions).toEqual(['Rome']);
  });

  it('a failed preload: the page is read now', async () => {
    const failed = Promise.reject(new Error('HTTP 500'));
    failed.catch(() => { });
    const { comp, api } = create('key', fakeApi({ keys: ['city'] }), { preloaded: failed });
    comp.onFocus();
    await settle();
    expect(comp.suggestions).toEqual(['city']);
    expect(api.calls).toEqual([['keys', '', 0]]);
  });
});

describe('suggestions, other field filled: only pairs that exist', () => {
  const entries = [
    { key: 'source', value: 'https://a' },
    { key: 'sourceId', value: 'https://not-this' },
    { key: 'source', value: 'https://b' },
    { key: 'barbecue', value: 'yes' },
    { key: 'bar', value: 'yes' },
  ];

  it('value field, key chosen and existing as it is: values of exactly that key', async () => {
    const { comp, textarea, api } = create('value', fakeApi({ entries }), { key: 'source' });
    await type(comp, textarea, 'https');
    expect(comp.suggestions).toEqual(['https://a', 'https://b']);
    expect(api.calls).toEqual([['entries', 'source', 'https', 0, { key: true }]]);
  });

  it('other field not existing as it is: prefix matches', async () => {
    const { comp, textarea, api } = create('key', fakeApi({ entries }), { v: 'y' });
    await type(comp, textarea, 'b');
    expect(comp.suggestions).toEqual(['barbecue', 'bar']);
    expect(api.calls).toEqual([['entries', 'b', 'y', 0, { value: true }], ['entries', 'b', 'y', 0, { value: false }]]);
  });

  it('"Load more" keeps the exact / prefix choice of the first page', async () => {
    const many = Array.from({ length: SUGGESTIONS_PAGE_SIZE + 3 }, (_, i) => ({ key: 'source', value: 'v' + String(i).padStart(3, '0') }));
    const { comp, api } = create('value', fakeApi({ entries: [...many, { key: 'sourceId', value: 'v999' }] }), { key: 'source' });
    comp.onFocus();
    await settle();
    await comp.loadMore();
    expect(comp.suggestions.length).toBe(SUGGESTIONS_PAGE_SIZE + 3);
    expect(comp.suggestions).not.toContain('v999');
    expect(api.calls.at(-1)).toEqual(['entries', 'source', '', SUGGESTIONS_PAGE_SIZE, { key: true }]);
  });

  it('the same value under several keys is listed once', async () => {
    const { comp } = create('value', fakeApi({ entries: [{ key: 'a', value: 'x' }, { key: 'ab', value: 'x' }] }), { key: 'a' });
    comp.onFocus();
    await settle();
    expect(comp.suggestions).toEqual(['x']);
  });
});

describe('selection and verification', () => {
  it('selecting fills the field and tells the parent at once', () => {
    const { comp, textarea, emitted, api } = create('key');
    comp.selectOption('city');
    expect(textarea.value).toBe('city');
    expect(emitted.output).toEqual(['city']);
    expect(emitted.ver).toEqual([true]);
    expect(api.calls).toEqual([]);
  });

  it('ver: whether the text is one of the suggestions', async () => {
    const { comp, textarea, emitted } = create('key', fakeApi({ keys: ['city', 'cityCode'] }));
    await type(comp, textarea, 'cit');
    await type(comp, textarea, 'city');
    expect(emitted.ver).toEqual([false, true]);
  });

  it('long values are flattened to one line and shown in the tooltip', () => {
    const { comp, textarea } = create('value');
    comp.selectOption('first line\r\nsecond line');
    expect(textarea.value).toBe('first line second line');
    expect(textarea.title).toBe('first line second line');
  });

  it('the initial value is put in the field', () => {
    const { comp, textarea } = create('key', fakeApi(), { value: 'region' });
    comp.ngAfterViewInit();
    expect(textarea.value).toBe('region');
  });
});

describe('dropdown', () => {
  it('opens on focus, closes on outside click, not on inside click', () => {
    const { comp, textarea } = create('key');
    comp.onFocus();
    expect(comp.dropdownOpen).toBe(true);
    comp.onDocumentClick({ target: textarea } as any);
    expect(comp.dropdownOpen).toBe(true);
    comp.onDocumentClick({ target: document.body } as any);
    expect(comp.dropdownOpen).toBe(false);
  });

  it('body.ds-dropdown-open while at least one dropdown is open (key -> value field)', () => {
    const a = create('key').comp;
    const b = create('value').comp;
    a.onFocus();
    b.onFocus();          // opened before a is closed by the outside click
    a.closeDropdown();
    expect(document.body.classList.contains('ds-dropdown-open')).toBe(true);
    b.ngOnDestroy();
    expect(document.body.classList.contains('ds-dropdown-open')).toBe(false);
  });

  it('opens upwards near the bottom of the viewport', () => {
    const { comp, textarea } = create('key');
    vi.spyOn(textarea, 'getBoundingClientRect').mockReturnValue({ top: window.innerHeight - 60, bottom: window.innerHeight - 30, left: 10, width: 200 } as DOMRect);
    comp.onFocus();
    expect(comp.panelTop).toBeNull();
    expect(comp.panelBottom).toBe(60 + 4);
    expect(comp.panelLeft).toBe(10);
    expect(comp.panelWidth).toBe(200);
  });

  it('"Load more" does not close the dropdown', async () => {
    const all = Array.from({ length: SUGGESTIONS_PAGE_SIZE + 1 }, (_, i) => 'k' + i);
    const { comp } = create('key', fakeApi({ keys: all }));
    comp.onFocus();
    await settle();
    const event = new MouseEvent('click');
    const stop = vi.spyOn(event, 'stopPropagation');
    await comp.loadMore(event);
    expect(stop).toHaveBeenCalled();
    expect(comp.dropdownOpen).toBe(true);
  });
});

describe('values not suggested for the key', () => {
  it('value field: the note opens the dropdown even without suggestions', async () => {
    const { comp } = create('value', fakeApi({ entries: [] }), { key: 'value', valuesNotSuggested: true });
    comp.onFocus();
    await settle();
    expect(comp.suggestions).toEqual([]);
    expect(comp.showNotSuggestedNote).toBe(true);
    expect(comp.dropdownOpen).toBe(true);
  });

  it('never in the key field, nor without the flag', () => {
    expect(create('key', fakeApi(), { valuesNotSuggested: true }).comp.showNotSuggestedNote).toBe(false);
    expect(create('value', fakeApi(), { key: 'city' }).comp.showNotSuggestedNote).toBe(false);
  });
});

describe('collections', () => {
  it('the suggestions are read from the chosen collections', async () => {
    const keys = create('key', fakeApi({ keys: ['city'] }), { collections: ['api'] });
    keys.comp.onFocus();
    await settle();
    expect(keys.api.calls).toEqual([['keys', '', 0], ['collections', ['api']]]);
    const values = create('value', fakeApi({ entries: [{ key: 'city', value: 'Rome' }] }), { key: 'city', collections: ['orion', 'minio'] });
    values.comp.onFocus();
    await settle();
    expect(values.api.calls).toContainEqual(['collections', ['orion', 'minio']]);
  });
});
