import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { firstValueFrom } from 'rxjs';
import { AutocompleteComponent } from './autocomplete.component';

const LOADING = 'loading...';
const TOO_MANY = 'Too much suggestions. Type more characters in order to reduce them';
const BACKEND_TOO_MANY = ['Too many suggestions. Type some characters in order to reduce them'];

// getKeys/getValues/getEntries resolve with what the test sets in `answers`
function fakeApi(answers: { keys?: any; values?: any; entries?: any } = {}) {
  const calls: any[] = [];
  return {
    calls,
    getKeys: vi.fn(async (v?: string) => { calls.push(['getKeys', v]); return answers.keys ?? []; }),
    getValues: vi.fn(async (v?: string) => { calls.push(['getValues', v]); return answers.values ?? []; }),
    getEntries: vi.fn(async (k?: string, v?: string) => { calls.push(['getEntries', k, v]); return answers.entries ?? []; }),
  };
}

const created: AutocompleteComponent[] = [];

function create(mode: 'key' | 'value', api = fakeApi(), init: Partial<AutocompleteComponent> = {}) {
  const host = document.createElement('div');
  const textarea = document.createElement('textarea');
  host.appendChild(textarea);
  document.body.appendChild(host);
  const cdr = { markForCheck: vi.fn() };
  const comp = new AutocompleteComponent({} as any, api as any, {} as any, {} as any, cdr as any, { nativeElement: host } as any);
  comp.input = { nativeElement: textarea };
  comp.mode = mode;
  comp.key = '';
  comp.v = '';
  comp.options = [];
  comp.entries = [];
  Object.assign(comp, init);
  created.push(comp);
  return { comp, api, textarea, host, cdr };
}

const flush = () => new Promise(resolve => setTimeout(resolve, 0));
const shown = (comp: AutocompleteComponent) => firstValueFrom(comp.filteredOptions$);

// Like the template's async pipe: subscribes once to every new filteredOptions$ (filter() runs - and
// queries the backend - at subscription), until the suggestions settle. Returns what is shown last.
async function settle(comp: AutocompleteComponent): Promise<any[]> {
  let subscribed: any;
  let last: any[] = [];
  for (let i = 0; i < 10 && comp.filteredOptions$ !== subscribed; i++) {
    subscribed = comp.filteredOptions$;
    last = await firstValueFrom(subscribed);
    await flush();
  }
  return last;
}

function type(comp: AutocompleteComponent, textarea: HTMLTextAreaElement, text: string) {
  textarea.value = text;
  comp.onInputEvent();
}

afterEach(() => {
  created.splice(0).forEach(comp => comp.ngOnDestroy());
  document.body.innerHTML = '';
  document.body.className = '';
});

describe('suggestions', () => {
  it('empty field: the options loaded at startup, paired with the entries', async () => {
    const { comp } = create('key', fakeApi(), {
      cachedOptions: ['city', 'country', 'name'],
      cachedEntries: [{ key: 'city', value: 'Rome' }, { key: 'name', value: 'x' }],
    });
    comp.onChange();
    expect(await shown(comp)).toEqual(['city', 'name']); // "country" has no entry
  });

  it('"loading..." while waiting for the backend', async () => {
    const { comp, textarea } = create('key', fakeApi());
    type(comp, textarea, 'ci');
    expect(await shown(comp)).toEqual([LOADING]);
  });

  it('typing a key: getKeys, then getEntries, then the filtered keys', async () => {
    const api = fakeApi({ keys: [{ key: 'city' }, { key: 'cityCode' }], entries: [{ key: 'city', value: 'Rome' }, { key: 'cityCode', value: 'RM' }] });
    const { comp, textarea } = create('key', api);
    type(comp, textarea, 'ci');
    expect(await settle(comp)).toEqual(['city', 'cityCode']);
    expect(api.calls).toEqual([['getKeys', 'ci'], ['getEntries', 'ci', '']]);
  });

  it('backend "too many" answer: the too-many row right away, no getEntries', async () => {
    const api = fakeApi({ keys: BACKEND_TOO_MANY });
    const { comp, textarea } = create('key', api);
    type(comp, textarea, 'a');
    expect(await settle(comp)).toEqual([TOO_MANY]);
    expect(api.getEntries).not.toHaveBeenCalled();
  });

  it('startup options that are the "too many" answer: too-many row on the empty field', async () => {
    const { comp } = create('value', fakeApi(), { cachedOptions: BACKEND_TOO_MANY, cachedEntries: [] });
    comp.onChange();
    expect(await shown(comp)).toEqual([TOO_MANY]);
  });

  it('value field with the key chosen: values of exactly that key (no "sourceId" leaks)', async () => {
    const api = fakeApi({
      entries: [
        { key: 'source', value: 'https://a' },
        { key: 'sourceId', value: 'https://not-this' },
        { key: 'source', value: 'https://b' },
        { key: 'source', value: 42 },
      ],
    });
    const { comp, textarea } = create('value', api, { key: 'source' });
    type(comp, textarea, 'https');
    expect(await settle(comp)).toEqual(['https://a', 'https://b']);
    expect(api.calls).toEqual([['getEntries', 'source', 'https']]);
  });

  it('other field not an exact match: keeps the prefix matches', async () => {
    const api = fakeApi({ entries: [{ key: 'barbecue', value: 'yes' }, { key: 'bar', value: 'yes' }] });
    const { comp, textarea } = create('key', api, { v: 'y' });
    type(comp, textarea, 'b');
    expect(await settle(comp)).toEqual(['barbecue', 'bar']);
    expect(api.calls).toEqual([['getEntries', 'b', 'y']]);
  });
});

describe('selection', () => {
  it('placeholders are not selectable', () => {
    const { comp, textarea } = create('key');
    const emitted: any[] = [];
    comp.output.subscribe(v => emitted.push(v));
    expect(comp.isPlaceholderOption(LOADING)).toBe(true);
    expect(comp.isPlaceholderOption(TOO_MANY)).toBe(true);
    comp.selectOption(LOADING);
    expect(textarea.value).toBe('');
    expect(emitted).toEqual([]);
  });

  it('selecting an option fills the field and emits it immediately (before any backend round-trip)', () => {
    const { comp, textarea, api } = create('key', fakeApi(), { options: ['city'] });
    const emitted: any[] = [];
    const verified: any[] = [];
    comp.output.subscribe(v => emitted.push(v));
    comp.ver.subscribe(v => verified.push(v));
    comp.selectOption('city');
    expect(textarea.value).toBe('city');
    expect(emitted).toEqual(['city']);
    expect(verified).toEqual([true]);
    expect(api.calls).toEqual([]);
  });

  it('long values are flattened to one line and shown in the tooltip', () => {
    const { comp, textarea } = create('value');
    comp.selectOption('first line\r\nsecond line');
    expect(textarea.value).toBe('first line second line');
    expect(textarea.title).toBe('first line second line');
  });
});

describe('dropdown', () => {
  it('opens on focus, closes on outside click, not on inside click', () => {
    const { comp, textarea, host } = create('key', fakeApi(), { cachedOptions: [], cachedEntries: [] });
    comp.onFocus();
    expect(comp.dropdownOpen).toBe(true);
    comp.onDocumentClick({ target: textarea } as any);
    expect(comp.dropdownOpen).toBe(true);
    comp.onDocumentClick({ target: document.body } as any);
    expect(comp.dropdownOpen).toBe(false);
    expect(host.isConnected).toBe(true);
  });

  it('body.ds-dropdown-open while at least one dropdown is open (key -> value field)', () => {
    const a = create('key', fakeApi(), { cachedOptions: [], cachedEntries: [] }).comp;
    const b = create('value', fakeApi(), { cachedOptions: [], cachedEntries: [] }).comp;
    a.onFocus();
    b.onFocus();          // opened before a is closed by the outside click
    a.closeDropdown();
    expect(document.body.classList.contains('ds-dropdown-open')).toBe(true);
    b.ngOnDestroy();
    expect(document.body.classList.contains('ds-dropdown-open')).toBe(false);
  });

  it('opens upwards near the bottom of the viewport', () => {
    const { comp, textarea } = create('key', fakeApi(), { cachedOptions: [], cachedEntries: [] });
    vi.spyOn(textarea, 'getBoundingClientRect').mockReturnValue({ top: window.innerHeight - 60, bottom: window.innerHeight - 30, left: 10, width: 200 } as DOMRect);
    comp.onFocus();
    expect(comp.panelTop).toBeNull();
    expect(comp.panelBottom).toBe(60 + 4);
    expect(comp.panelLeft).toBe(10);
    expect(comp.panelWidth).toBe(200);
    comp.closeDropdown();
  });
});

describe('ready flag', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('"loading..." until the parent is ready, then the real options', async () => {
    const { comp } = create('key', fakeApi(), { ready: false });
    comp.ngOnInit();
    expect(await shown(comp)).toEqual([LOADING]);

    comp.options = ['k'];
    comp.entries = [{ key: 'k', value: 'v' }];
    comp.ready = true;
    comp.ngOnChanges({ ready: { currentValue: true } } as any);
    vi.runAllTimers();
    expect(await shown(comp)).toEqual(['k']);
  });

  it('ready with empty data: no endless "loading..."', async () => {
    const { comp } = create('key', fakeApi(), { ready: true, options: [], entries: [] });
    comp.ngOnChanges({ ready: { currentValue: true } } as any);
    vi.runAllTimers();
    expect(await shown(comp)).toEqual([]);
  });
});
