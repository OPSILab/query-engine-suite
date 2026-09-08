import { ChangeDetectionStrategy, Component, ViewChild, OnInit, AfterViewInit, EventEmitter, Output, Input } from '@angular/core';
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
export class AutocompleteComponent implements OnInit, AfterViewInit {

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
  page = 0;
  log = console;
  cachedOptions: any;
  cachedEntries;
  interval: ReturnType<typeof setInterval>;
  entriesInterval: ReturnType<typeof setInterval>;

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
    this.interval = setInterval(() => {
      if (this.options && this.options[0]) {
        clearInterval(this.interval);
        this.cachedOptions = JSON.parse(JSON.stringify(this.options));
        this.onChange();
      }
    }, 2000);
    this.entriesInterval = setInterval(() => {
      if (this.entries && this.entries[0]) {
        clearInterval(this.entriesInterval);
        this.cachedEntries = JSON.parse(JSON.stringify(this.entries));
        this.onChange();
      }
    }, 2000);
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
      this.onChange();
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
