import { ChangeDetectionStrategy, Component, ViewChild, OnInit, OnChanges, AfterViewInit, EventEmitter, Output, Input, SimpleChanges } from '@angular/core';
import { Observable, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { NbToastrService } from '@nebular/theme';
import { ConfigService } from '@ngx-config/core';
import { TranslateService } from '@ngx-translate/core';
import { BeopenAPIService } from '../../../services/be-open.service';
import { SharedService } from '../../../services/shared.service';

// Direct copy of the dashboard's key/value AutocompleteComponent (used by the
// "Advanced search" mode) - unrelated to the old vanilla-JS iframe editor
// that used to sit under assets/autocomplete, which SqlEditorComponent now
// replaces.
@Component({
  selector: 'autocomplete',
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './autocomplete.component.html',
  styleUrls: ['./autocomplete.component.scss'],
})
export class AutocompleteComponent implements OnInit, AfterViewInit, OnChanges {

  @Input() options;
  @Input() placeholder;
  @Input() keys;
  @Input() value;
  @Input() entries;
  @ViewChild('autoInput') input;
  @Output() output = new EventEmitter<any[]>();
  filteredOptions$;
  @Input() mode;
  @Input() key;
  @Input() v;
  @Output() ver = new EventEmitter<any[]>();
  @Input() otherVerified;
  // True once the parent has finished loading keys/values/entries (see
  // QueryEngineComponent.autocompleteDataReady) - regardless of whether
  // what it loaded turned out to be empty. See ngOnChanges() below for why
  // this replaced polling on `options`/`entries` truthiness.
  @Input() ready = false;
  page = 0;
  log = console;
  cachedOptions: any;
  cachedEntries;

  constructor(
    private configService: ConfigService,
    private beopenAPI: BeopenAPIService,
    public translation: TranslateService,
    private toastrService: NbToastrService,
    private sharedService: SharedService,
  ) {
  }

  ngOnInit() {
    this.filteredOptions$ = of(this.options);
    // If `ready` is already true at mount (e.g. a row added via "+ Add
    // filter" well after the parent's initial load finished), ngOnChanges()
    // below still fires once with the initial value - see its comment -
    // so there's nothing else to do here.
  }

  ngOnChanges(changes: SimpleChanges) {
    // `ready` used to be inferred by polling this.options/this.entries
    // every 2s and treating "first element is truthy" as "data has
    // arrived" - which can never come true when the parent's data has
    // genuinely loaded but is empty (an empty array's [0] is always
    // falsy), so the UI got stuck showing "loading..." forever instead of
    // "no suggestions". It could also under-report readiness the other
    // way: this.options and this.entries are populated by two independent
    // parent fetches that resolve at different times, so whichever one
    // finished first would already look "ready" while the other was still
    // legitimately loading.
    //
    // `ready` is a single explicit flag the parent flips once after both
    // fetches settle (see QueryEngineComponent.ngOnInit's finally block),
    // so it can only mean "keys/values/entries have their final values
    // now, whatever they are" - fixing both problems at once. Angular
    // calls ngOnChanges with the initial value of every @Input on first
    // render too, so a row that mounts after the parent is already ready
    // (e.g. clicking "+ Add filter" later) resolves immediately here
    // rather than waiting on a fresh poll cycle.
    if (changes.ready && this.ready && this.cachedOptions === undefined) {
      this.cachedOptions = JSON.parse(JSON.stringify(this.options || []));
      this.cachedEntries = JSON.parse(JSON.stringify(this.entries || []));
      // `this.input` (the #autoInput ViewChild) doesn't exist yet if
      // `ready` was already true the moment this row was created - e.g. a
      // row added via "+ Add filter" after the parent finished loading:
      // Angular runs ngOnChanges before ngAfterViewInit, so on that first
      // call there's no view yet to read/refresh. ngAfterViewInit() below
      // does this same refresh once the view exists, so it's safe to just
      // skip it here in that case rather than throwing on this.input being
      // undefined.
      if (this.input) {
        // Deferred to a fresh macrotask rather than called inline: this
        // runs from ngOnChanges, i.e. *during* Angular's own change
        // detection pass over QueryEngineComponent's template. onChange()
        // -> verified() synchronously emits `ver`, which the parent
        // handles by mutating keyVerified[i]/valueVerified[i] - a value
        // this same row's *other* <autocomplete> reads via
        // [otherVerified] a few bindings later in that same pass. Doing
        // that mutation synchronously trips Angular's dev-mode
        // ExpressionChangedAfterItHasBeenCheckedError (NG0100), because a
        // binding it already checked this pass changes under it. The old
        // interval-based version never hit this because setInterval
        // callbacks always run in their own macrotask, safely after CD
        // has finished - setTimeout(..., 0) here restores that same
        // boundary without going back to polling.
        setTimeout(() => this.onChange(), 0);
      }
    }
  }

  ngAfterViewInit() {
    // The visible <input #autoInput> is intentionally uncontrolled (see the
    // template): it's read via this.input.nativeElement.value rather than
    // [(ngModel)], because typing shouldn't fight Angular change detection
    // while suggestions stream in. That means it was never seeded from the
    // `value` @Input either - a parent setting item.key/item.value (e.g. the
    // "Demo" button in QueryEngineComponent) updated the model but the
    // field on screen stayed blank. Set it once here, on the instance this
    // *ngFor row actually mounts with; don't rebind it continuously or it
    // would overwrite what the user types afterwards.
    if (this.value) {
      this.input.nativeElement.value = this.value;
    }
    // Covers the case ngOnChanges() above had to skip: `ready` (and so
    // cachedOptions/cachedEntries) was already set before this view -
    // and this.input - existed. Refresh now that it does, so the dropdown
    // still resolves instead of waiting on an @Input change that already
    // happened and won't happen again.
    if (this.value || this.cachedOptions !== undefined) {
      // Same reasoning as the setTimeout in ngOnChanges() above -
      // ngAfterViewInit() also runs as part of Angular's own change
      // detection pass, so onChange()'s synchronous `ver` emission needs
      // the same macrotask boundary to avoid NG0100.
      setTimeout(() => this.onChange(), 0);
    }
  }

  isPaired(optionValue, entries) {
    if (this.mode == "key")
      return this.isKey(optionValue, entries);
    else if (this.mode == "value")
      return this.isValue(optionValue, entries);
    else
      console.error("Invalid mode");
  }

  verified(verify) {
    this.ver.emit(verify);
  }

  onValueChange(event) {
    if (this.options.filter(optionValue => optionValue == this.value)[0])
      this.verified(true);
    else
      this.verified(false);
  }

  otherVerifiedChange(event) {
  }

  isKey(key, entries) {
    const loweredKey = key.toLowerCase();
    const loweredValue = this.v.toLowerCase();

    if (entries[0])
      for (let entry of entries)
        try {
          if (
            entry.key.toLowerCase().includes(loweredKey) && (
              (
                !this.otherVerified &&
                entry.value.toLowerCase().includes(loweredValue)
              )
              || (
                this.otherVerified &&
                entry.value.toLowerCase() == loweredValue
              )
            )
          ) {
            return true;
          }
        }
        catch (error) {
          if (
            entry.key.toLowerCase().includes(loweredKey) && (
              (
                !this.otherVerified &&
                entry.value.toString().toLowerCase().includes(loweredValue)
              )
              || (
                this.otherVerified &&
                entry.value.toString().toLowerCase() == loweredValue
              )
            )
          ) {
            return true;
          }
        }
  }

  isValue(value, entries) {
    const loweredValue = value.toLowerCase();
    const loweredKey = this.key.toLowerCase();

    if (entries[0])
      for (let entry of entries)
        try {
          if (entry.value.toLowerCase().includes(loweredValue) && (
            (
              !this.otherVerified &&
              entry.key.toLowerCase().includes(loweredKey)
            )
            || (
              this.otherVerified &&
              entry.key.toLowerCase() == loweredKey
            ))) {
            return true;
          }
        }
        catch (error) {
          if (entry.value.toString().toLowerCase().includes(loweredValue) && (
            (
              !this.otherVerified &&
              entry.key.toLowerCase().includes(loweredKey)
            )
            || (
              this.otherVerified &&
              entry.key.toLowerCase() == loweredKey
            ))) {
            return true;
          }
        }
  }

  queryKeyOrValues(keyOrValue) {
    this.beopenAPI[this.mode == "key" ? "getKeys" : "getValues"](keyOrValue).then(key => this.onChange(key));
  }

  queryEntries(keyOrValue, keysOrValuesQueriedAgain) {
    this.beopenAPI.getEntries(this.mode == "key" ? keyOrValue : this.key, this.mode == "value" ? keyOrValue : this.v).then(e => this.onChange(keysOrValuesQueriedAgain, e));
  }

  private filter(keyOrValue: string, keysOrValuesQueriedAgain?, entitiesQueriedAgain?) {
    if (keyOrValue) {
      if (!keysOrValuesQueriedAgain)
        this.queryKeyOrValues(keyOrValue);
      else if (keysOrValuesQueriedAgain && !entitiesQueriedAgain)
        this.queryEntries(keyOrValue, keysOrValuesQueriedAgain);
    }
    else if ((this.mode == "key" ? this.v : this.key)) {
      keysOrValuesQueriedAgain = this.cachedOptions;
      if (!entitiesQueriedAgain)
        this.queryEntries(keyOrValue, keysOrValuesQueriedAgain);
    }
    else {
      keysOrValuesQueriedAgain = this.cachedOptions;
      entitiesQueriedAgain = this.cachedEntries;
    }

    if (keysOrValuesQueriedAgain && entitiesQueriedAgain) {
      this.options = keysOrValuesQueriedAgain;
      this.entries = entitiesQueriedAgain;

      if (this.options.length > 500 && this.entries.length > 500)
        return ["Too much suggestions. Type more characters in order to reduce them"];

      const filterValue = keyOrValue?.toLowerCase();
      try {
        let options = this.options[0] && (this.options[0].key || this.options[0].value) ? this.options.map(o => (o.key || o.value)) : this.options;
        let entries = this.entries;

        let filteredValues = options.filter(optionValue => optionValue?.toLowerCase().includes(filterValue));
        let pairedValues = filteredValues.filter(filtered => this.isPaired(filtered, entries));
        return pairedValues;
      }
      catch (error) {
        console.error(error, filterValue);
      }
    }
    return ["loading..."];
  }

  getFilteredOptions(value: string, queried?, ready?) {
    return of(value).pipe(
      map(filterString => this.filter(filterString, queried, ready)),
    );
  }

  onChange(queried?, ready?) {
    this.filteredOptions$ = this.getFilteredOptions(this.input.nativeElement.value, queried, ready);
    this.output.emit(this.input.nativeElement.value);
    if (this.options.filter(optionValue => optionValue == this.input.nativeElement.value)[0])
      this.verified(true);
    else
      this.verified(false);
  }

  onInputChange(event: any) {
    this.output.emit(this.input.nativeElement.value);
    if (this.options.filter(optionValue => optionValue == this.input.nativeElement.value)[0])
      this.verified(true);
    else
      this.verified(false);
  }

  onSelectionChange($event) {
    this.filteredOptions$ = this.getFilteredOptions($event);
    if (this.options.filter(optionValue => optionValue == this.input.nativeElement.value)[0])
      this.verified(true);
    else
      this.verified(false);
  }

  loadPreviousSuggestions() {
    if (this.page > 99) {
      this.page -= 100;
      this.onChange();
    }
  }

  loadNextSuggestions() {
    this.page += 100;
    this.onChange();
  }

}
