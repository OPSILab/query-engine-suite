import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, HostListener, ViewChild, AfterViewInit, OnDestroy, EventEmitter, Output, Input } from '@angular/core';
import { BeopenAPIService, SUGGESTIONS_PAGE_SIZE, SuggestionEntry, SuggestionsPage } from '../../../services/be-open.service';

// Key / value field of the "Advanced search" rows, with suggestions from the Query-Engine's keys / values /
// entries, read one page at a time ("Load more" at the end of the list): the backend never reads them all.
//  - the other field of the row is empty: keys (or values) starting with the text;
//  - the other field is filled: only the pairs that exist (entries), with the other field matched exactly when
//    its text exists as it is (key "source" chosen: no "sourceId" values), else by prefix.
// The first page for an empty field can be read in advance by the parent ([preloaded]): a click shows it at once.
@Component({
  selector: 'autocomplete',
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './autocomplete.component.html',
  styleUrls: ['./autocomplete.component.scss'],
  standalone: false
})
export class AutocompleteComponent implements AfterViewInit, OnDestroy {

  @Input() placeholder;
  /** Initial text of the field (e.g. set by the "Demo" button). */
  @Input() value;
  @Input() mode: 'key' | 'value';
  /** The row's key, when this is the value field. */
  @Input() key;
  /** The row's value, when this is the key field. */
  @Input() v;
  /**
   * First page of keys (or values) read in advance by the parent, shown when the field and the other field are
   * empty. If it failed, the page is read now.
   */
  @Input() preloaded?: Promise<SuggestionsPage<string>>;
  /** Only the suggestions of these collections (the "Search in" choice); undefined: all of them. */
  @Input() collections?: string[];
  /** Value field: the key has values that are not suggested (not indexed), the dropdown says so. */
  @Input() valuesNotSuggested = false;
  /** Kept for the parent's bindings; not needed any more (the entries are matched by the backend). */
  @Input() otherVerified;
  @ViewChild('autoInput') input;
  /** The text, at every change and at every selection. */
  @Output() output = new EventEmitter<any>();
  /** Whether the text is one of the suggestions. */
  @Output() ver = new EventEmitter<boolean>();

  /** What the dropdown lists. */
  suggestions: string[] = [];
  hasMore = false;
  loadingSuggestions = false;
  private skip = 0;
  // stale responses (the text changed meanwhile) are ignored
  private requestSeq = 0;
  // other field matched exactly (decided on the first page, kept for "Load more")
  private exactOther = false;

  // Plain `position: fixed` markup in this component's own template (no CDK overlay, see the history in
  // app.component.ts). dropdownOpen/panelTop/panelLeft/panelWidth are its state.
  dropdownOpen = false;
  panelTop: number | null = 0;
  panelBottom: number | null = null;
  panelLeft = 0;
  panelWidth = 0;
  panelMaxHeight = AutocompleteComponent.PANEL_MAX_HEIGHT;

  // The panel is position: fixed, so it doesn't add to the page height: near the bottom of a short page
  // it used to run past the viewport with no way to scroll to its end. Now its height is clamped to the
  // space left in the viewport (keeping a visible margin), it opens upwards when below there's almost no
  // room, and while any dropdown is open the page gets extra room at the bottom (body.ds-dropdown-open
  // in styles.scss) so it can be scrolled up to give the panel more space.
  private static readonly PANEL_MAX_HEIGHT = 260;
  private static readonly PANEL_GAP = 4;           // between input and panel
  private static readonly VIEWPORT_MARGIN = 16;    // between panel and viewport edge
  private static readonly MIN_PANEL_HEIGHT = 120;  // below this, open upwards if there's more room there
  private static openDropdowns = 0;                // shared by every instance (key + value fields, all rows)
  private static readonly MAX_ROWS = 6;            // the field grows with long values up to this many lines

  // Grows the field (a textarea) to show the whole value, up to MAX_ROWS lines - beyond that it
  // scrolls instead, so a very long value doesn't take over the view. Also keeps it single-line
  // (pasted line breaks become spaces) and puts the full value in the tooltip.
  private autoResize(): void {
    const el = this.input?.nativeElement as HTMLTextAreaElement;
    if (!el || typeof window === 'undefined')
      return;
    if (/[\r\n]/.test(el.value))
      el.value = el.value.replace(/[\r\n]+/g, ' ');
    const style = window.getComputedStyle(el);
    const px = (v: string) => parseFloat(v) || 0;
    const borders = px(style.borderTopWidth) + px(style.borderBottomWidth);
    const padding = px(style.paddingTop) + px(style.paddingBottom);
    const maxHeight = px(style.lineHeight) * AutocompleteComponent.MAX_ROWS + padding + borders;
    el.style.height = 'auto';
    const needed = el.scrollHeight + borders;
    el.style.height = Math.min(needed, maxHeight) + 'px';
    el.style.overflowY = needed > maxHeight ? 'auto' : 'hidden';
    el.title = el.value;
  }

  constructor(
    private beopenAPI: BeopenAPIService,
    private cdr: ChangeDetectorRef,
    private el: ElementRef<HTMLElement>,
  ) {
  }

  ngAfterViewInit() {
    // The field is uncontrolled (read through input.nativeElement.value, so typing never fights change
    // detection): the initial value is set once here.
    if (this.value) {
      this.input.nativeElement.value = this.value;
    }
    this.autoResize();
  }

  get showNotSuggestedNote(): boolean {
    return this.mode === 'value' && !!this.valuesNotSuggested;
  }

  private get text(): string {
    return this.input?.nativeElement?.value ?? '';
  }

  onFocus(): void {
    this.refresh();
    this.openDropdown();
  }

  onInputEvent(): void {
    this.autoResize();
    this.output.emit(this.text);
    this.refresh();
    this.openDropdown();
  }

  /** First page of suggestions for the current text (and other field). */
  refresh(): Promise<void> {
    this.skip = 0;
    this.suggestions = [];
    this.hasMore = false;
    return this.load(true);
  }

  /** Next page, appended. */
  loadMore(event?: Event): Promise<void> {
    event?.stopPropagation();
    if (!this.hasMore || this.loadingSuggestions) return Promise.resolve();
    this.skip += SUGGESTIONS_PAGE_SIZE;
    return this.load(false);
  }

  private async load(first: boolean): Promise<void> {
    const seq = ++this.requestSeq;
    const text = this.text;
    this.loadingSuggestions = true;
    this.cdr.markForCheck();
    try {
      const page = await this.fetchPage(text, this.skip, first);
      if (seq !== this.requestSeq) return; // the text changed meanwhile
      const seen = new Set(this.suggestions);
      this.suggestions = this.suggestions.concat(page.options.filter(option => !seen.has(option) && seen.add(option)));
      this.hasMore = page.hasMore;
      this.ver.emit(this.suggestions.includes(text));
    } catch (error) {
      if (seq === this.requestSeq) {
        console.error('Suggestions could not be loaded', error);
        this.hasMore = false;
      }
    } finally {
      if (seq === this.requestSeq) {
        this.loadingSuggestions = false;
        this.cdr.markForCheck();
      }
    }
  }

  private async fetchPage(text: string, skip: number, first: boolean): Promise<{ options: string[]; hasMore: boolean }> {
    const other = String((this.mode === 'key' ? this.v : this.key) ?? '');
    if (!other) {
      if (!text && skip === 0 && this.preloaded) {
        try {
          const page = await this.preloaded;
          return { options: page.items.map(String), hasMore: page.hasMore };
        } catch {
          // read it now
        }
      }
      const page = this.mode === 'key'
        ? await this.beopenAPI.getKeys(text, skip, SUGGESTIONS_PAGE_SIZE, this.collections)
        : await this.beopenAPI.getValues(text, skip, SUGGESTIONS_PAGE_SIZE, this.collections);
      return { options: page.items.map(String), hasMore: page.hasMore };
    }
    // pairs that exist; the other field exactly when its text exists as it is, else by prefix
    const query = (exact: boolean) => this.mode === 'key'
      ? this.beopenAPI.getEntries(text, other, skip, SUGGESTIONS_PAGE_SIZE, { value: exact }, this.collections)
      : this.beopenAPI.getEntries(other, text, skip, SUGGESTIONS_PAGE_SIZE, { key: exact }, this.collections);
    let page = first || this.exactOther ? await query(true) : await query(false);
    if (first) {
      this.exactOther = page.items.length > 0;
      if (!this.exactOther) page = await query(false);
    }
    const pick = (entry: SuggestionEntry) => this.mode === 'key' ? entry.key : entry.value;
    return { options: page.items.map(pick), hasMore: page.hasMore };
  }

  private openDropdown(): void {
    if (!this.dropdownOpen)
      AutocompleteComponent.setOpenDropdowns(+1);
    this.dropdownOpen = true;
    this.updatePanelPosition();
    this.cdr.markForCheck();
  }

  closeDropdown(): void {
    if (this.dropdownOpen) {
      this.dropdownOpen = false;
      AutocompleteComponent.setOpenDropdowns(-1);
      this.cdr.markForCheck();
    }
  }

  ngOnDestroy(): void {
    if (this.dropdownOpen) {
      this.dropdownOpen = false;
      AutocompleteComponent.setOpenDropdowns(-1);
    }
  }

  // A counter rather than a toggle: moving from the key field to the value field opens the new
  // dropdown before the old one is closed by the outside click.
  private static setOpenDropdowns(delta: number): void {
    AutocompleteComponent.openDropdowns = Math.max(0, AutocompleteComponent.openDropdowns + delta);
    if (typeof document !== 'undefined')
      document.body.classList.toggle('ds-dropdown-open', AutocompleteComponent.openDropdowns > 0);
  }

  private updatePanelPosition(): void {
    if (!this.input) {
      return;
    }
    const C = AutocompleteComponent;
    const rect = this.input.nativeElement.getBoundingClientRect();
    const spaceBelow = window.innerHeight - rect.bottom - C.PANEL_GAP - C.VIEWPORT_MARGIN;
    const spaceAbove = rect.top - C.PANEL_GAP - C.VIEWPORT_MARGIN;
    const openUpwards = spaceBelow < C.MIN_PANEL_HEIGHT && spaceAbove > spaceBelow;
    if (openUpwards) {
      this.panelTop = null;
      this.panelBottom = window.innerHeight - rect.top + C.PANEL_GAP;
      this.panelMaxHeight = Math.max(0, Math.min(C.PANEL_MAX_HEIGHT, spaceAbove));
    }
    else {
      this.panelTop = rect.bottom + C.PANEL_GAP;
      this.panelBottom = null;
      this.panelMaxHeight = Math.max(0, Math.min(C.PANEL_MAX_HEIGHT, spaceBelow));
    }
    this.panelLeft = rect.left;
    this.panelWidth = rect.width;
  }

  // Mirrors nbAutocomplete's outside-click-closes behavior. The panel is a
  // plain descendant of this component's own host element (position: fixed
  // only changes where it's painted, not where it lives in the DOM), so a
  // simple containment check is enough - no portal to reason about.
  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    if (this.dropdownOpen && !this.el.nativeElement.contains(event.target as Node)) {
      this.closeDropdown();
    }
  }

  // A different width changes where long values wrap.
  @HostListener('window:resize')
  onWindowResize(): void {
    this.autoResize();
  }

  @HostListener('window:scroll')
  @HostListener('window:resize')
  onWindowScrollOrResize(): void {
    if (this.dropdownOpen) {
      this.updatePanelPosition();
      this.cdr.markForCheck(); // OnPush: position/height changed outside any input binding
    }
  }

  selectOption(option: string): void {
    this.input.nativeElement.value = option;
    this.autoResize();
    // the parent gets the chosen value right away (an "Apply Query" right after uses it)
    this.output.emit(option);
    this.ver.emit(true);
    this.closeDropdown();
  }
}
