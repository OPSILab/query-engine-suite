import { AfterViewInit, Component, ElementRef, EventEmitter, HostListener, Input, NgZone, OnChanges, OnDestroy, OnInit, Output, SimpleChanges, ViewChild } from '@angular/core';
import { Observable, Subscription, defer, finalize, firstValueFrom } from 'rxjs';
import { ToastService, ToastStatus } from '../../services/toast.service';

import { ADVANCED_SEARCH_PAGE_SIZE, BeopenAPIService, CollectionInfo, QueryWarning, ResultsPage, SUGGESTIONS_PAGE_SIZE, SuggestionsPage } from '../../services/be-open.service';
import { BucketObject } from '../../model/BucketObject';
import { TranslateService } from '@ngx-translate/core';
import { UntypedFormGroup, UntypedFormControl } from '@angular/forms';
import { BeopenUser } from '../../model/beopen-user';
import { Router } from '@angular/router';
import { SharedService } from '../../services/shared.service';
import { DataSpaceService } from '../data-space.service';
import { NbAuthService } from '@nebular/auth';
import { ConfigService } from '../../services/config.service';
import { QueryRequest, SNIPPET_LANGS, SnippetLang, toSnippet } from './request-snippets';
import { FALLBACK_GQL_EXAMPLES, GqlExample, SOURCES_DISCOVERY_QUERY, SURVEYS_DISCOVERY_QUERY, buildGqlExamples } from './graphql-editor/graphql-examples';

/**
 * Ported from the main dashboard's QueryEngineComponent
 * (src/app/pages/data-space/query-engine/query-engine.component.ts).
 *
 * Changes made while porting:
 * - The "Query SQL" tab no longer loads an <iframe src="assets/autocomplete/index.html">
 *   talking back to this component through window.postMessage. It now embeds
 *   <bx-sql-editor> directly and binds its value with [(value)]="sqlQuery" -
 *   a real Angular component instead of a vanilla HTML/JS/CSS page copied into
 *   assets/ at build time (see angular.json in the main dashboard).
 * - sqlQuery is a plain instance field now. In the original file it was
 *   actually a module-level `export let sqlQuery` variable shadowing an
 *   unused `this.sqlQuery` class field of the same name (the postMessage
 *   handler wrote to the module-level one, minioQuery() read the same
 *   module-level one) - that indirection is gone along with the iframe.
 * - Fields that were declared but never read anywhere in this component
 *   (pdfSrc, isImage/isText/isPdf/isGeoJson, dialogData, map, ...) were
 *   dropped; they look like leftovers copy-pasted from DataSpaceComponent.
 * - configService is no longer injected: it was only used to build the old
 *   sqlEditorUrl.
 * - On a getUser() failure the original redirects to "/register-user" (a
 *   dashboard-only route). This standalone project has no such route, so it
 *   just surfaces a toast instead - adjust this if you wire the same auth
 *   flow back in.
 */
@Component({
  selector: 'ngx-query-engine',
  templateUrl: './query-engine.component.html',
  styleUrls: ['./query-engine.component.scss'],
  standalone: false
})
export class QueryEngineComponent implements OnInit, OnChanges, AfterViewInit, OnDestroy {

  form: UntypedFormGroup;
  modes: string[] = ["Simple search", "Advanced search", "Query SQL", "Query GraphQL"];
  lines: any[] = [{ key: "", value: "" }];
  type: string;
  value: string = "";
  sqlQuery: string = "";
  graphqlText: string = "";
  // Set by <bx-graphql-editor (blockedChange)> when the document contains a
  // mutation or subscription: this mode only runs queries, so Apply/Find all
  // are disabled while it's true (and runGraphqlQuery() re-checks anyway).
  graphqlBlocked = false;
  graphqlEndpoint: string;
  generalSharedBucketObjects: BucketObject[] = [];
  userBucketObjects: BucketObject[] = [];
  pilotSharedBucketObjects: BucketObject[] = [];
  extractedElements: any[] = [];
  isAdmin: boolean = true;
  beopenUser: BeopenUser;
  userRoles: string[] = [];
  types: string[] = ["CSV", "JSON", "GeoJSON"];
  valueTypes: string[] = ["String", "Date"];
  email: string | null = null;

  @Input() visibility!: string;
  @Output() extractedElementsChange = new EventEmitter<any[]>();
  @Output() generalSharedBucketObjectsChange = new EventEmitter<any[]>();
  @Output() pilotSharedBucketObjectsChange = new EventEmitter<any[]>();
  @Output() userBucketObjectsChange = new EventEmitter<any[]>();
  // true while a query is in flight, false once it has answered, failed or
  // been cancelled - HomeComponent fades the previous results meanwhile.
  @Output() loadingChange = new EventEmitter<boolean>();

  // Which button started the query in flight (it shows the spinner), or null
  // when nothing is running. Both buttons are disabled while it's set, so a
  // slow answer can't be "fixed" by clicking again and stacking requests.
  // 'more' = "Load more results" (loadMoreResults): the results on screen stay as they are (no loadingChange).
  loading: 'query' | 'all' | 'more' | null = null;

  // Advanced search answers one page at a time: the request of the results on screen and whether there are more
  // after them ("Load more results", see loadMoreResults). Emitted so that HomeComponent shows the button.
  hasMoreResults = false;
  @Output() hasMoreResultsChange = new EventEmitter<boolean>();
  // next: the skip of each collection with more results (the answer's `next`); collections: those searched
  private resultsRequest?: { mongoQuery: any; type: string; visibility: string; kind: 'query' | 'all'; page: ResultsPage; collections?: string[]; next?: Record<string, number> };

  // ---- Collections searched: one per Source-Connector connector (api: Sources, orion: Datapoints, minio: Files).
  // The Query-Engine says which ones exist (GET /api/collections), the labels come from config.json
  // ("collectionLabels"), the user chooses with the "Search in" pills - remembered in this browser. Until the user
  // chooses, the selected ones are the Query-Engine's defaults (queryOptions.defaultCollections). An older
  // Query-Engine without collections: no choice shown, nothing sent.
  static readonly COLLECTIONS_STORAGE_KEY = 'qe.selectedCollections';
  static readonly ALL_COLLECTIONS = ['api', 'orion', 'minio'];
  collectionOptions: (CollectionInfo & { label: string })[] = [];
  // the user's choice in this browser, or null if never chosen
  private readonly savedCollections = QueryEngineComponent.readSelectedCollections();
  selectedCollections: string[] = this.savedCollections ?? [...QueryEngineComponent.ALL_COLLECTIONS];
  // Seconds since the query started, shown next to the buttons once the
  // answer is taking a while ("Waiting for the query engine... 4 s").
  loadingSeconds = 0;
  private pendingQuery?: Subscription;
  private loadingTimer?: ReturnType<typeof setInterval>;

  // The action row (Apply Query / Find all / loading status) is
  // position: sticky at the bottom of the window, so with a long form (the
  // intro open, several Advanced search filters) the buttons stay reachable
  // instead of ending up below the fold. `actionsStuck` is true while it is
  // actually floating over the form - i.e. the end of the panel is below the
  // visible area, watched through a 1px sentinel at the panel's very bottom -
  // and only then the row gets its top border and shadow. Emitted so that
  // HomeComponent can lift its "N results below" pill above the row.
  @ViewChild('panelEnd', { static: true }) panelEnd!: ElementRef<HTMLElement>;
  @Output() actionsStuckChange = new EventEmitter<boolean>();
  actionsStuck = false;
  private stuckObserver?: IntersectionObserver;

  // The key / value autocompletes read their suggestions themselves, one page at a time; the first page of keys
  // and of values is read once here, so that a click on an empty field shows it at once (see preloadSuggestions).
  preloadedKeys?: Promise<SuggestionsPage<string>>;
  preloadedValues?: Promise<SuggestionsPage<string>>;
  // Keys whose values are not suggested (not indexed for some sources, e.g. the datapoints' `value`): the row warns.
  keysWithValuesNotSuggested = new Set<string>();
  valueVerified = [];
  keyVerified = [];

  // "Generate curl / fetch / axios" dialog: the request the current form would send, as code.
  readonly snippetLangs = SNIPPET_LANGS;
  snippetOpen = false;
  snippetLang: SnippetLang = 'curl';
  snippetCode = '';
  // Advanced search only: the request of "Find all" (no filters) instead of "Apply Query".
  snippetFindAll = false;
  // Off by default: a snippet with a real token is a credential, easy to paste in a chat or a ticket.
  snippetIncludeToken = false;
  snippetCopied = false;
  private snippetCopiedTimer?: ReturnType<typeof setTimeout>;
  // Same condition TokenInterceptor uses to add the Authorization header.
  authEnabled = false;

  // Simple search: what the configuration leaves out (shown under the search field) - see QueryWarning.
  simpleSearchLimits: QueryWarning[] = [];
  private queryWarningsSub?: Subscription;

  constructor(
    private zone: NgZone,
    private beopenAPI: BeopenAPIService,
    public translation: TranslateService,
    private toastService: ToastService,
    private router: Router,
    private sharedService: SharedService,
    private dataSpaceService: DataSpaceService,
    private nbAuth: NbAuthService,
    private configService: ConfigService,
    private host: ElementRef<HTMLElement>
  ) {
    this.authEnabled = !!this.configService.getSettings('enableAuthentication', false);
    this.form = new UntypedFormGroup({
      mode: new UntypedFormControl(this.modes[1])
    });
    this.graphqlEndpoint = this.beopenAPI.graphqlEndpoint;
  }

  stringify(value) {
    if (typeof value == "string")
      return value;
    return JSON.stringify(value);
  }

  async ngOnInit(): Promise<void> {
    this.getUser();
    const collections = this.beopenAPI.getCollections().then(list => {
      this.collectionOptions = list.map(c => ({ ...c, label: this.beopenAPI.collectionLabel(c.id) }));
      // never chosen in this browser: the Query-Engine's defaults (all of them if it doesn't say)
      if (!this.savedCollections && list.some(c => c.default !== undefined))
        this.selectedCollections = QueryEngineComponent.ALL_COLLECTIONS.filter(id => list.some(c => c.id === id && c.default));
    });
    // the suggestions follow the selection: with a saved choice they can be read at once, else after the defaults
    if (!this.savedCollections)
      await collections;
    this.preloadSuggestions();
    this.loadKeysWithValuesNotSuggested();
    this.beopenAPI.getSimpleSearchLimits().then(warnings => this.simpleSearchLimits = warnings.filter(w => w.kind === "config"));
    // the configuration ones are already listed under the field: only what happened during the query
    this.queryWarningsSub = this.beopenAPI.queryWarnings$.subscribe(warnings => {
      const runtime = warnings.filter(w => w.kind === "runtime");
      if (runtime.length)
        this.createToastr('warning', this.tr("Simple search incomplete"), runtime.map(w => this.warningText(w)).join(" — "));
    });
  }

  // ---- collections

  private static readSelectedCollections(): string[] | null {
    try {
      const saved = JSON.parse(localStorage.getItem(QueryEngineComponent.COLLECTIONS_STORAGE_KEY) ?? 'null');
      if (Array.isArray(saved))
        return QueryEngineComponent.ALL_COLLECTIONS.filter(id => saved.includes(id));
    } catch {
      // not readable: as never chosen
    }
    return null;
  }

  isCollectionSelected(id: string): boolean {
    return this.selectedCollections.includes(id);
  }

  /** The collection is searched by the current mode (e.g. Orion is not in the Simple search unless configured). */
  collectionAvailable(c: CollectionInfo, mode = this.mode): boolean {
    return mode === 'Simple search' ? c.simpleSearch : c.advancedSearch;
  }

  toggleCollection(id: string): void {
    const selected = new Set(this.selectedCollections);
    if (selected.has(id)) selected.delete(id);
    else selected.add(id);
    this.selectedCollections = QueryEngineComponent.ALL_COLLECTIONS.filter(c => selected.has(c));
    try {
      localStorage.setItem(QueryEngineComponent.COLLECTIONS_STORAGE_KEY, JSON.stringify(this.selectedCollections));
    } catch {
      // not remembered - fine
    }
    // the suggestions follow the collections
    this.preloadSuggestions();
    this.loadKeysWithValuesNotSuggested();
  }

  /**
   * The collections a query of this mode searches: the selected ones that the mode can search. undefined (send
   * nothing, i.e. everything) with an older Query-Engine, or before GET /api/collections has answered.
   */
  collectionsFor(mode = this.mode): string[] | undefined {
    if (!this.collectionOptions.length) return undefined;
    return this.collectionOptions.filter(c => this.collectionAvailable(c, mode) && this.isCollectionSelected(c.id)).map(c => c.id);
  }

  /** Simple / Advanced search with no collection to search: Apply Query and Find all are disabled. */
  get noCollectionSelected(): boolean {
    return (this.mode === 'Simple search' || this.mode === 'Advanced search') && this.collectionsFor()?.length === 0;
  }

  /** Collections of the suggestions (keys / values / entries): the selected ones. */
  get suggestionCollections(): string[] {
    return this.selectedCollections;
  }

  private loadKeysWithValuesNotSuggested(): void {
    this.beopenAPI.getKeysWithValuesNotIndexed(this.suggestionCollections).then(keys => this.keysWithValuesNotSuggested = new Set(keys));
  }

  /** First page of keys and of values, read before the user clicks a field (a failed read is read again later). */
  preloadSuggestions(): void {
    const preload = (read: () => Promise<SuggestionsPage<string>>) => {
      const page = read();
      page.catch(err => console.warn("Could not preload the suggestions", err));
      return page;
    };
    const collections = this.suggestionCollections;
    this.preloadedKeys = preload(() => this.beopenAPI.getKeys("", 0, SUGGESTIONS_PAGE_SIZE, collections));
    this.preloadedValues = preload(() => this.beopenAPI.getValues("", 0, SUGGESTIONS_PAGE_SIZE, collections));
  }

  /** The row's key has values that are not suggested (they can still be typed and searched). */
  valuesNotSuggested(key: any): boolean {
    return typeof key === "string" && this.keysWithValuesNotSuggested.has(key);
  }

  getUser() {
    this.beopenAPI.getUser().subscribe(
      (beopenUser) => {
        this.beopenUser = beopenUser;
        this.email = beopenUser.email;
      },
      (err) => {
        console.warn("Could not load the current user", err);
        this.createToastr('warning', "Not logged in", "Could not load the current user.");
      }
    );

    this.sharedService.userRoles$.subscribe((u) => {
      this.userRoles = u || [];
    });

    if (this.userRoles.includes("admin")) {
      this.isAdmin = true;
    }
  }

  get mode() {
    return this.form.get('mode').value;
  }

  // Small helpers for the pill/tab controls in the redesigned template -
  // they write to the exact same properties the rest of this component
  // (and minioQuery()) already reads, just from a (click) instead of an
  // nb-select's [(ngModel)].
  setMode(m: string): void {
    this.form.get('mode').setValue(m);
    if (m === "Query GraphQL") this.loadGraphqlExamples();
  }

  setType(t: string): void {
    this.type = t;
  }

  setItemType(item: any, t: string): void {
    item.type = t;
  }

  // ---- Demo: fills the form with a key/value pair that really exists in the backend

  demoLoading = false;
  // Used only when the backend can't offer a real pair (too many keys to list, nothing indexed, errors).
  private static readonly DEMO_FALLBACK = { key: "a", value: "a1" };
  // Technical fields added by the Source-Connector: valid keys, but poor examples - tried only as a last resort.
  private static readonly DEMO_TECHNICAL_KEYS = new Set(["source", "sourceId", "source_original", "sourceId_original", "datePolled", "record", "name", "_id", "id"]);
  private static readonly DEMO_MAX_KEYS_TRIED = 4;

  async demo(): Promise<void> {
    if (this.demoLoading) return;
    this.demoLoading = true;
    try {
      const pair = await this.pickDemoPair();
      const type = pair ? await this.pickDemoFormat(pair.key, pair.value) : "JSON";
      const { key, value } = pair || QueryEngineComponent.DEMO_FALLBACK;
      // New objects: the @for tracks them by identity, so the rows (and their autocompletes) are re-created
      // and show the new values.
      this.lines = [{ key, value, type: "String" }];
      this.value = value;
      this.type = type;
    } finally {
      this.demoLoading = false;
    }
  }

  /**
   * A page of keys, then getEntries(key) for a few random ones (data keys first) until one has values: returns
   * that key and one of its values. null when the backend has nothing (or can't be read).
   */
  private async pickDemoPair(): Promise<{ key: string; value: string } | null> {
    let keys: string[];
    try {
      const page = await (this.preloadedKeys ?? Promise.reject()).catch(() => this.beopenAPI.getKeys("", 0, SUGGESTIONS_PAGE_SIZE, this.suggestionCollections));
      keys = Array.from(new Set(page.items.filter(k => typeof k === "string" && k)));
    } catch {
      return null;
    }
    if (!keys.length) return null;

    const shuffle = (a: string[]) => a.map(v => [Math.random(), v] as [number, string]).sort((x, y) => x[0] - y[0]).map(([, v]) => v);
    const technical = QueryEngineComponent.DEMO_TECHNICAL_KEYS;
    const candidates = [...shuffle(keys.filter(k => !technical.has(k))), ...shuffle(keys.filter(k => technical.has(k)))]
      .slice(0, QueryEngineComponent.DEMO_MAX_KEYS_TRIED);

    for (const key of candidates) {
      let entries: any[];
      try {
        entries = (await this.beopenAPI.getEntries(key, "", 0, undefined, { key: true }, this.suggestionCollections)).items;
      } catch {
        continue;
      }
      // exact key (case insensitive on the backend): keep only this very key
      const values = (entries || [])
        .filter(e => e?.key === key && e.value !== undefined && e.value !== null && e.value !== "")
        .map(e => typeof e.value === "string" ? e.value : JSON.stringify(e.value));
      // short, plain values make a nicer example than whole JSON objects
      const nice = values.filter(v => v.length <= 80 && !/^[\[{]/.test(v));
      const pool = nice.length ? nice : values;
      if (pool.length) return { key, value: pool[Math.floor(Math.random() * pool.length)] };
    }
    return null;
  }

  /**
   * The format (file type pill) under which the pair actually finds results: entries come from CSV rows, JSON
   * arrays, GeoJSON properties or plain JSON objects (no format), and only the right one matches.
   */
  private async pickDemoFormat(key: string, value: string): Promise<string | undefined> {
    for (const format of ["JSON", "CSV", "GeoJSON", undefined]) {
      try {
        const result = await firstValueFrom(this.beopenAPI.minioQuery("Advanced search", value, { [key]: value }, "", this.visibility, format, { limit: 1, skip: 0 }, this.collectionsFor("Advanced search")));
        const items = Array.isArray(result) ? result : (result?.results || result?.data || result?.items || []);
        if (items.length) return format;
      } catch {
        // try the next format
      }
    }
    return "JSON";
  }

  minioQuery(all: Boolean) {
    if (this.loading || this.noCollectionSelected) {
      return;
    }
    if (this.mode === "Query GraphQL") {
      // GraphQL doesn't go through /api/query at all, and "find all" has no
      // meaning for it: both buttons just run the document in the editor.
      this.runGraphqlQuery(all ? 'all' : 'query');
      return;
    }
    const mongoQuery = this.buildMongoQuery(all);
    const kind = all ? 'all' : 'query';
    const page = this.mode === "Advanced search" ? { limit: ADVANCED_SEARCH_PAGE_SIZE, skip: 0 } : undefined;
    const collections = this.collectionsFor();
    this.pendingQuery = this.track(kind, this.beopenAPI.minioQuery(this.mode, this.searchValue(all), mongoQuery, this.sqlQuery, this.visibility, this.type, page, collections)).subscribe(queryResult => {
      this.resultsRequest = page ? { mongoQuery, type: this.type, visibility: this.visibility, kind, page, collections, next: queryResult?.next } : undefined;
      this.showQueryResult(queryResult, false);
    }, err => {
      console.error("Query error", err);
      this.createToastr('danger', "Error querying objects", err.error);
    });
  }

  /**
   * Advanced search: the next page of the query on screen, appended to its results. Same filters, file type,
   * visibility and collections as the first page, whatever the form says now: only the collections that have
   * more results, from where each one stopped (the answer's `next`).
   */
  loadMoreResults(): void {
    const request = this.resultsRequest;
    if (this.loading || !this.hasMoreResults || !request) {
      return;
    }
    const next = request.next && typeof request.next === 'object' && Object.keys(request.next).length ? request.next : undefined;
    // a Query-Engine without `next`: one skip for everything
    const page: ResultsPage = next
      ? { limit: request.page.limit, skip: next }
      : { limit: request.page.limit, skip: (typeof request.page.skip === 'number' ? request.page.skip : 0) + request.page.limit };
    const collections = next ? Object.keys(next) : request.collections;
    this.pendingQuery = this.track('more', this.beopenAPI.minioQuery("Advanced search", "", request.mongoQuery, "", request.visibility, request.type, page, collections)).subscribe(queryResult => {
      if (this.resultsRequest !== request) return; // a new query was sent meanwhile
      request.page = page;
      request.next = queryResult?.next;
      this.showQueryResult(queryResult, true);
    }, err => {
      console.error("Query error", err);
      this.createToastr('danger', "Error querying objects", err.error);
    });
  }

  /** Shows a REST answer: in place of the results on screen, or after them (append, a further page). */
  private showQueryResult(queryResult: any, append: boolean): void {
    if (!append) {
      this.generalSharedBucketObjects = [];
      this.pilotSharedBucketObjects = [];
      this.userBucketObjects = [];
      this.extractedElements = [];
    }
    // new arrays: the parent gets a new list (and its @for sees the change)
    this.generalSharedBucketObjects = [...this.generalSharedBucketObjects];
    this.pilotSharedBucketObjects = [...this.pilotSharedBucketObjects];
    this.userBucketObjects = [...this.userBucketObjects];
    this.extractedElements = [...this.extractedElements];

    // Simple search (a GET) always came back as a bare array, which is
    // the only shape the loop below ever handled. Advanced search / Query
    // SQL (both POST /api/query) can come back wrapped - e.g. as
    // { results: [...] } - depending on the backend route. If that's the
    // actual cause of "results arrive but nothing renders" for those two
    // modes, this normalizes it instead of silently iterating zero items.
    const items = Array.isArray(queryResult)
      ? queryResult
      : (queryResult?.results || queryResult?.data || queryResult?.items || []);
    // a page ({ results, hasMore }); a bare array is everything (or a Query-Engine without pages)
    this.setHasMoreResults(!!this.resultsRequest && queryResult?.hasMore === true);

    if (!Array.isArray(queryResult) && !Array.isArray(queryResult?.results)) {
      console.warn("minioQuery(): response for mode", this.mode, "was not a bare array - unwrapped to", items.length, "item(s). Raw response:", queryResult);
    }

    let skipped = 0;
    for (let obj of items) {
      try {
        // Some responses may already be the record itself rather than
        // { record, name, element } - fall back to the item itself so a
        // shape difference between modes doesn't throw away the whole
        // batch (see the try/catch below either way).
        const record = obj.record || obj;
        // The item's own data, i.e. everything except `record` - taken
        // BEFORE the lines below add objectPath/pilot/insertedBy to obj.
        // Advanced search returns the stored Source documents as they are,
        // `{ _id, name, json | csv | <the file's own keys>, record }`, where
        // `record` is only the MinIO/S3 metadata of the uploaded file: showing
        // `record` for those (as this used to) showed the metadata and never
        // the data itself.
        // `_collection`: the collection of an Advanced search result (shown as a label, not as data)
        const { record: _meta, _collection, ...ownData } = obj;
        delete obj._collection;
        const collection = typeof _collection === 'string' ? this.beopenAPI.collectionLabel(_collection) : undefined;
        obj.objectPath = record.name;
        obj.pilot = record.bucketName;
        obj.insertedBy = record.insertedBy; //TODO now it is empty

        // TODO(technical debt, acknowledged - not something to silently
        // work around forever): BucketObjectsPush assumes every record is
        // a minio bucket object and, lacking a real `bucketName`, tries to
        // guess one by splitting `.name` on "/" and treating the first
        // segment as the bucket and the last as the file. For a plain
        // MongoDB document that isn't a bucket object at all, `.name` is
        // just ordinary text with no such structure - and this guess
        // doesn't fail safely on that: it can silently drop the record
        // (SMARTERA's "Italian", "English" - name has no "/", so bucket
        // and fileName both end up equal to the whole name and get
        // filtered out) or, worse, silently fabricate a bogus file entry
        // out of it (SMARTERA's "Bosnian/Croatian/Serbian" - that's a
        // language name, not a path, but its "/"s are enough to make
        // BucketObjectsPush invent bucket:"Bosnian" + file:"Serbian" and
        // show it as if it were a real file with no size or date). Both
        // are real, observed backend data, not edge cases. The real fix
        // is decoupling the query-result shape from Minio's file model on
        // both backend and frontend so results are represented
        // generically regardless of source; until then, only attempt this
        // classification when the record actually carries a real
        // `bucketName` - the one field BucketObjectsPush can't fall back
        // to guessing - so a record that isn't a bucket object never gets
        // put through a heuristic built for one. It still isn't lost: the
        // raw-record fallback below always keeps it available.
        if (record.bucketName) {
          this.BucketObjectsPush(record, this.isAdmin, this.generalSharedBucketObjects, this.pilotSharedBucketObjects, this.userBucketObjects, record.pilot);
        }

        if (obj.element !== undefined && obj.element !== null) {
          // Advanced search / Query SQL can extract a nested element out
          // of a matched file (obj.element is the JSON/GeoJSON sub-object
          // the query matched inside it) - more specific than the raw
          // record, so show that instead of it.
          const bucketName = record.bucketName || record.s3?.bucket?.name || "?";
          this.extractedElements.push({ name: bucketName + "/" + (obj.name || record.name || "?"), element: obj.element, collection });
        } else {
          // No nested element to be more specific than - show the item's
          // data. Simple search (isRawQuery: "yes" on the request) carries
          // the true unprocessed file in obj.raw: prefer that. Otherwise
          // show ownData - the item minus its `record` metadata (see where
          // it's taken, above). For items that have no `record` at all
          // (e.g. the SMARTERA language views, plain Mongo documents) that
          // is the whole item, same as before. `record` itself is only
          // shown when the item carries nothing else. It's the only place
          // some results (non-file Mongo documents) show up at all.
          const rawData = obj.raw !== undefined
            ? obj.raw
            : (Object.keys(ownData).length ? ownData.json || ownData.csv || ownData : record);
          this.extractedElements.push({ name: record._id || obj.name || record.name || "?", element: rawData, collection });
        }
      } catch (itemErr) {
        skipped++;
        console.error("minioQuery(): could not process one result item, skipping it - item was:", obj, itemErr);
      }
    }
    if (skipped > 0) {
      this.createToastr('warning', "Some results could not be displayed", `${skipped} of ${items.length} result(s) had an unexpected shape - see the console for details.`);
    }
    this.sendData();
  }

  private setHasMoreResults(more: boolean): void {
    if (more !== this.hasMoreResults) {
      this.hasMoreResults = more;
      this.hasMoreResultsChange.emit(more);
    }
  }

  /** Advanced search filters as sent to the backend ("Find all" = no filters). */
  private buildMongoQuery(all: Boolean): any {
    let mongoQuery = {};
    if (!all)
      for (let l of this.lines) {
        if (l.type == "Date")
          mongoQuery[l.key] = JSON.stringify({
            $gte: new Date(l.from),
            $lte: new Date(l.to),
          });
        else mongoQuery[l.key] = l.value;
      }
    return mongoQuery;
  }

  // ---- "Generate curl / fetch / axios" dialog

  openSnippet(): void {
    this.snippetFindAll = false;
    this.snippetCopied = false;
    this.refreshSnippet();
    this.snippetOpen = true;
    setTimeout(() => this.host.nativeElement.querySelector<HTMLElement>('.ds-modal')?.focus());
  }

  closeSnippet(): void {
    this.snippetOpen = false;
  }

  @HostListener('document:keydown.escape')
  onEscape(): void {
    if (this.snippetOpen) this.closeSnippet();
  }

  setSnippetLang(lang: SnippetLang): void {
    this.snippetLang = lang;
    this.refreshSnippet();
  }

  refreshSnippet(): void {
    this.snippetCopied = false;
    const request = this.currentRequest();
    this.snippetCode = request ? toSnippet(this.snippetLang, request, this.snippetAuthHeader()) : '';
  }

  /** The request "Apply Query" (or "Find all", see snippetFindAll) would send right now. */
  private currentRequest(): QueryRequest | undefined {
    if (this.mode === "Query GraphQL")
      return this.beopenAPI.buildGraphqlRequest(this.graphqlText, this.visibility);
    const all = (this.mode === "Advanced search" || this.mode === "Simple search") && this.snippetFindAll;
    const page = this.mode === "Advanced search" ? { limit: ADVANCED_SEARCH_PAGE_SIZE, skip: 0 } : undefined;
    return this.beopenAPI.buildQueryRequest(this.mode, this.searchValue(all), this.buildMongoQuery(all), this.sqlQuery, this.visibility, this.type, page, this.collectionsFor());
  }

  /** Simple search text: none for "Find all" (the backend then returns everything the user may see). */
  private searchValue(all: Boolean): string {
    return all && this.mode === "Simple search" ? "" : this.value;
  }

  // ---- GraphQL example queries, built from the data the user can see (see graphql-examples.ts)

  // undefined until loaded: the editor keeps its hardcoded fallback examples
  graphqlExamples?: GqlExample[];
  readonly fallbackGqlExamples = FALLBACK_GQL_EXAMPLES;
  private graphqlExamplesByVisibility = new Map<string, Promise<GqlExample[]>>();

  async loadGraphqlExamples(): Promise<void> {
    const key = this.visibility ?? "";
    let examples = this.graphqlExamplesByVisibility.get(key);
    if (!examples) {
      examples = this.fetchGraphqlExamples();
      this.graphqlExamplesByVisibility.set(key, examples);
    }
    const built = await examples;
    if ((this.visibility ?? "") === key) // not changed in the meantime
      this.graphqlExamples = built;
  }

  private async fetchGraphqlExamples(): Promise<GqlExample[]> {
    // null = couldn't be read (unreachable, older backend without the field...): fallback examples for that part
    const read = async (query: string) => {
      try {
        return (await firstValueFrom(this.beopenAPI.graphqlQuery(query, this.visibility)))?.data ?? null;
      } catch {
        return null;
      }
    };
    const [sources, surveys] = await Promise.all([read(SOURCES_DISCOVERY_QUERY), read(SURVEYS_DISCOVERY_QUERY)]);
    return buildGqlExamples({
      sources: sources ? { sample: sources.sample?.[0], api: sources.api?.[0] } : null,
      surveys: Array.isArray(surveys?.surveys) ? surveys.surveys : null,
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    // other visibility, other visible data
    if (changes['visibility'] && !changes['visibility'].firstChange && this.mode === "Query GraphQL")
      this.loadGraphqlExamples();
  }

  private snippetAuthHeader(): string | null {
    if (!this.authEnabled) return null;
    const token = this.snippetIncludeToken ? this.currentAccessToken() : null;
    return `Bearer ${token || '<TOKEN>'}`;
  }

  // Read the same way TokenInterceptor does (NbAuthService emits the stored token synchronously).
  private currentAccessToken(): string | null {
    let token: string | null = null;
    this.nbAuth.getToken().subscribe((t: any) => token = t?.getPayload?.()?.access_token || null).unsubscribe();
    return token;
  }

  // In the component, not the template: "<TOKEN>" inside {{ }} is read as an HTML tag by the template parser.
  get snippetTokenNote(): string {
    return this.snippetIncludeToken
      ? 'This code contains your access token: do not share it.'
      : 'Replace <TOKEN> with a valid access token.';
  }

  get snippetFileName(): string {
    const lang = this.snippetLangs.find(l => l.id === this.snippetLang);
    return `query-engine-request.${lang?.extension || 'txt'}`;
  }

  async copySnippet(): Promise<void> {
    try {
      await navigator.clipboard.writeText(this.snippetCode);
    } catch {
      // No navigator.clipboard outside secure contexts (plain http): old execCommand way.
      const ta = document.createElement('textarea');
      ta.value = this.snippetCode;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
    this.snippetCopied = true;
    clearTimeout(this.snippetCopiedTimer);
    this.snippetCopiedTimer = setTimeout(() => this.snippetCopied = false, 1600);
  }

  downloadSnippet(): void {
    const url = URL.createObjectURL(new Blob([this.snippetCode], { type: 'text/plain' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = this.snippetFileName;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url));
  }

  runGraphqlQuery(kind: 'query' | 'all' = 'query'): void {
    if (this.graphqlBlocked || this.loading) {
      return;
    }
    if (!this.graphqlText.trim()) {
      this.createToastr('warning', "Empty query", "GraphQL empty query");
      return;
    }
    this.pendingQuery = this.track(kind, this.beopenAPI.graphqlQuery(this.graphqlText, this.visibility)).subscribe({
      next: res => this.showGraphqlResult(res),
      error: err => {
        // Apollo answers syntax/validation errors (unknown field, wrong
        // argument type, ...) with HTTP 400 and the usual { errors: [...] }
        // body - the same shape as a 200 with errors, so show them the same way.
        if (Array.isArray(err?.error?.errors)) {
          this.showGraphqlResult(err.error);
        } else {
          console.error("GraphQL query error", err);
          this.createToastr('danger', "Error querying objects", err?.message || "GraphQL request failed");
        }
      },
    });
  }

  /**
   * GraphQL results don't have the { record, element } shape the REST modes
   * return, and aren't bucket files: every top-level field of `data` becomes
   * one or more entries in the "Matched elements" list (one per item when the
   * field is a list), shown as-is. `data` and `errors` can both be present -
   * GraphQL returns partial results - so both are shown.
   */
  private showGraphqlResult(res: any): void {
    this.resultsRequest = undefined;
    this.setHasMoreResults(false);
    this.generalSharedBucketObjects = [];
    this.pilotSharedBucketObjects = [];
    this.userBucketObjects = [];
    this.extractedElements = [];

    const data = res?.data && typeof res.data === "object" ? res.data : {};
    for (const [field, val] of Object.entries<any>(data)) {
      if (Array.isArray(val)) {
        val.forEach((item, i) => this.extractedElements.push({
          name: `${field}[${i}]` + (item?._id ? ` · ${item._id}` : ""),
          element: item,
        }));
      } else if (val !== null && val !== undefined) {
        this.extractedElements.push({ name: field, element: val });
      }
    }

    const errors: any[] = Array.isArray(res?.errors) ? res.errors : [];
    if (errors.length) {
      const shown = errors.slice(0, 3).map(e => e?.message || String(e)).join(" — ");
      const more = errors.length > 3 ? ` (+${errors.length - 3})` : "";
      this.createToastr('danger', "GraphQL query returned errors", shown + more);
    } else if (!this.extractedElements.length) {
      this.createToastr('info', "No results", "GraphQL no results");
    }
    this.sendData();
  }

  /**
   * Wraps a query request so the loading state follows it: set when it's
   * subscribed, cleared by finalize() whatever happens next - answer, error,
   * or cancelQuery() unsubscribing (which also aborts the HTTP request).
   * Callers keep the subscription in pendingQuery so cancelQuery() can reach it.
   */
  private track<T>(kind: 'query' | 'all' | 'more', request: Observable<T>): Observable<T> {
    return defer(() => {
      this.startLoading(kind);
      return request;
    }).pipe(finalize(() => this.stopLoading()));
  }

  cancelQuery(): void {
    if (!this.pendingQuery) {
      return;
    }
    this.pendingQuery.unsubscribe();
    this.createToastr('info', "Query cancelled", "Query cancelled");
  }

  private startLoading(kind: 'query' | 'all' | 'more'): void {
    this.loading = kind;
    this.loadingSeconds = 0;
    clearInterval(this.loadingTimer);
    this.loadingTimer = setInterval(() => this.loadingSeconds++, 1000);
    if (kind !== 'more') this.loadingChange.emit(true);
  }

  private stopLoading(): void {
    clearInterval(this.loadingTimer);
    this.loadingTimer = undefined;
    this.pendingQuery = undefined;
    if (this.loading) {
      const more = this.loading === 'more';
      this.loading = null;
      if (!more) this.loadingChange.emit(false);
    }
  }

  ngAfterViewInit(): void {
    if (typeof IntersectionObserver === 'undefined') {
      return;
    }
    // One element observed, but a batch can hold several entries for it
    // (e.g. the layout settling during load): only the last one is current.
    this.stuckObserver = new IntersectionObserver(entries => {
      const entry = entries[entries.length - 1];
      const stuck = !entry.isIntersecting && entry.boundingClientRect.top > (entry.rootBounds?.bottom ?? window.innerHeight);
      if (stuck !== this.actionsStuck) {
        this.zone.run(() => {
          this.actionsStuck = stuck;
          this.actionsStuckChange.emit(stuck);
        });
      }
      // -14px: the row's `bottom` offset (see .ds-action-row in the scss).
    }, { rootMargin: '0px 0px -14px 0px' });
    this.stuckObserver.observe(this.panelEnd.nativeElement);
  }

  /**
   * Limits to show under the Simple search field. API / Orion records are public data, searched only with the
   * Public visibility (or with authentication disabled): their limits don't apply to Private / Shared searches.
   */
  get shownSimpleSearchLimits(): QueryWarning[] {
    const liveSourcesSearched = !this.authEnabled || this.visibility === "public";
    // only those of the selected collections (a warning without collection: an older Query-Engine, always shown)
    const selected = this.collectionOptions.length ? this.selectedCollections : undefined;
    return this.simpleSearchLimits
      .filter(w => w.code === "MINIO_DISABLED" || liveSourcesSearched)
      .filter(w => !selected || !w.collection || selected.includes(w.collection));
  }

  /** Translated text of a warning ("Simple search warning <CODE>", with {{source}}), else the backend's message. */
  warningText(w: QueryWarning): string {
    const key = `Simple search warning ${w.code}`;
    const text = this.tr(key, { source: w.source ?? "" });
    return text && text !== key ? text : w.message;
  }

  private tr(key: string, params?: object): string {
    try {
      return this.translation?.instant?.(key, params) ?? key;
    } catch {
      return key;
    }
  }

  ngOnDestroy(): void {
    this.queryWarningsSub?.unsubscribe();
    clearTimeout(this.snippetCopiedTimer);
    this.pendingQuery?.unsubscribe();
    clearInterval(this.loadingTimer);
    this.stuckObserver?.disconnect();
  }

  BucketObjectsPush = this.dataSpaceService.BucketObjectsPush.bind(this.dataSpaceService);

  line(add) {
    if (add === 1) this.lines.push({ key: "", value: "" });
    else this.lines.pop();
  }

  createToastr(
    status: ToastStatus,
    message: string,
    description: string
  ) {
    try {
      return this.translation.get(description).subscribe((res: string) => {
        this.toastService.show(status, message, res, 15000);
      });
    } catch (error) {
      console.error(error);
      this.toastService.show(status, message, description, 15000);
    }
  }

  onKeysChange(data: any[], i) {
    this.lines[i].key = data;
  }

  valueVerifiedChange(data: any[], i) {
    this.valueVerified[i] = data;
  }

  keyVerifiedChange(data: any[], i) {
    this.keyVerified[i] = data;
  }

  onValuesChange(data: any[], i) {
    this.lines[i].value = data;
  }

  sendData() {
    this.extractedElementsChange.emit(this.extractedElements);
    this.generalSharedBucketObjectsChange.emit(this.generalSharedBucketObjects);
    this.pilotSharedBucketObjectsChange.emit(this.pilotSharedBucketObjects);
    this.userBucketObjectsChange.emit(this.userBucketObjects);
  }
}
