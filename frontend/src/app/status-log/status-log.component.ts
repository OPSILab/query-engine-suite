import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { BeopenAPIService, StatusLogFilters, StatusLogIgnoredUrl, StatusLogOutcome, StatusLogRecord } from '../services/be-open.service';
import { ConfigService } from '../services/config.service';
import { saveReturnUrl } from '../auth/services/return-url';

/** One part of the detail of a request: a label and its text (JSON pretty printed). */
export interface StatusLogDetail {
  label: string;
  text: string;
  /** The original length, when the Source-Connector cut the text (statusLog.maxDataSize). */
  cutAt?: number;
}

/**
 * /status-log: the HTTP requests of the Source-Connector, failed and successful (its statuslog collection, through
 * GET /api/statuslog of the Query-Engine), newest first, filtered by day, by part of the url and by outcome (all /
 * errors / successes), a page at a time. A click on a request shows its query params, headers, body, response and
 * duration. The header values come hidden ("<masked>") unless the user is
 * an administrator (Query-Engine authConfig.adminRoles): the page says so, and offers the login when there is one.
 * No AuthGuard: readable without logging in (hidden values).
 * Administrators (the answers are not masked) can ignore the url of a request: without a url filter its requests are
 * left out (the count says how many ignored urls), with one they are listed, marked; the ignored urls are listed
 * above, each one with the button to list its failures again.
 */
@Component({
  selector: 'app-status-log',
  templateUrl: './status-log.component.html',
  styleUrls: ['./status-log.component.scss'],
  standalone: false
})
export class StatusLogComponent implements OnInit {
  static readonly PAGE_SIZE = 50;

  static readonly OUTCOMES: { value: StatusLogOutcome | ''; label: string }[] = [
    { value: '', label: 'All requests' },
    { value: 'error', label: 'Errors' },
    { value: 'success', label: 'Successes' },
  ];

  // the form: days (YYYY-MM-DD, <input type="date">), part of the url, outcome ('': all)
  from = '';
  to = '';
  url = '';
  outcome: StatusLogOutcome | '' = '';
  readonly outcomes = StatusLogComponent.OUTCOMES;

  items: StatusLogRecord[] = [];
  total = 0;
  masked = true;
  loading: 'list' | 'more' | null = null;
  error = false;
  ignoredExcluded = 0;
  ignoredUrls: StatusLogIgnoredUrl[] = [];
  ignoredOpen = false;
  /** The url being ignored / restored. */
  busyUrl: string | null = null;
  actionError = false;
  readonly authEnabled: boolean;

  private applied: StatusLogFilters = {};
  private request = 0; // the answers of a request replaced by a newer one are dropped
  private readonly opened = new Set<StatusLogRecord>();
  private readonly details = new WeakMap<StatusLogRecord, StatusLogDetail[]>();

  constructor(private api: BeopenAPIService, configService: ConfigService, private router?: Router) {
    this.authEnabled = !!configService.getSettings('enableAuthentication', false);
  }

  ngOnInit(): void {
    this.search();
    this.loadIgnored();
  }

  get hasMore(): boolean {
    return this.items.length < this.total;
  }

  /** Administrators only: the Query-Engine shows them the values in clear. */
  get canManage(): boolean {
    return !this.masked;
  }

  async loadIgnored(): Promise<void> {
    try {
      this.ignoredUrls = await this.api.getStatusLogIgnored();
    } catch {
      this.ignoredUrls = [];
    }
  }

  /** Ignored in the Query-Engine config: restored only there. */
  isFromConfig(url: string | undefined): boolean {
    return this.ignoredUrls.some(ignored => ignored.fromConfig && ignored.url === url);
  }

  /** Ignores the url of a request, then the first page again (with the same filters). */
  ignore(item: StatusLogRecord, event?: Event): Promise<void> {
    event?.stopPropagation();
    return this.change(item.url, url => this.api.ignoreStatusLogUrl(url));
  }

  /** Lists the requests of an ignored url again. */
  restore(url: string | undefined, event?: Event): Promise<void> {
    event?.stopPropagation();
    return this.change(url, u => this.api.restoreStatusLogUrl(u));
  }

  private async change(url: string | undefined, action: (url: string) => Promise<StatusLogIgnoredUrl[]>): Promise<void> {
    if (!url || this.busyUrl) return;
    this.busyUrl = url;
    this.actionError = false;
    try {
      this.ignoredUrls = await action(url);
      await this.load(false);
    } catch {
      this.actionError = true;
    } finally {
      this.busyUrl = null;
    }
  }

  /** Applies the form (the first page). */
  search(): Promise<void> {
    this.applied = StatusLogComponent.filtersOf(this.from, this.to, this.url, this.outcome);
    return this.load(false);
  }

  /** A pill of the outcome: applied at once (with the rest of the form). */
  setOutcome(outcome: StatusLogOutcome | ''): Promise<void> {
    this.outcome = outcome;
    return this.search();
  }

  reset(): Promise<void> {
    this.from = this.to = this.url = this.outcome = '';
    return this.search();
  }

  loadMore(): Promise<void> {
    return this.load(true);
  }

  private async load(more: boolean): Promise<void> {
    const request = ++this.request;
    this.loading = more ? 'more' : 'list';
    this.error = false;
    try {
      const page = await this.api.getStatusLog(this.applied, { limit: StatusLogComponent.PAGE_SIZE, skip: more ? this.items.length : 0 });
      if (request !== this.request) return;
      if (!more) this.opened.clear();
      this.items = more ? [...this.items, ...page.items] : page.items;
      this.total = page.total;
      this.masked = page.masked;
      this.ignoredExcluded = page.ignoredExcluded;
    } catch {
      if (request === this.request) this.error = true;
    } finally {
      if (request === this.request) this.loading = null;
    }
  }

  toggle(item: StatusLogRecord): void {
    if (this.opened.has(item)) this.opened.delete(item);
    else this.opened.add(item);
  }

  isOpen(item: StatusLogRecord): boolean {
    return this.opened.has(item);
  }

  /** To log in and come back here (the header values are shown to administrators). */
  login(): void {
    saveReturnUrl('/status-log');
    this.router?.navigate(['/keycloak-auth']);
  }

  /** The status code, or for a request without answer its error code. */
  statusLabel(item: StatusLogRecord): string {
    return item.status ? String(item.status) : item.code || 'error';
  }

  /** success: an answer below 400 (outcome success, or none with such a status); client: 4xx; server: 5xx and no answer. */
  statusKind(item: StatusLogRecord): 'success' | 'client' | 'server' {
    if (item.status && item.status < 400 && item.outcome !== 'error') return 'success';
    return item.status && item.status >= 400 && item.status < 500 ? 'client' : 'server';
  }

  /** The parts of the detail of a request that it has (computed once per request). */
  detailOf(item: StatusLogRecord): StatusLogDetail[] {
    let parts = this.details.get(item);
    if (!parts) {
      parts = StatusLogComponent.detailsOf(item);
      this.details.set(item, parts);
    }
    return parts;
  }

  static detailsOf(item: StatusLogRecord): StatusLogDetail[] {
    const parts: StatusLogDetail[] = [];
    const add = (label: string, value: any, cutAt?: number) => {
      if (value === undefined || value === null || value === '') return;
      parts.push({ label, text: StatusLogComponent.pretty(value), ...(cutAt ? { cutAt } : {}) });
    };
    add('URL', item.fullUrl || item.url);
    add('Query params', item.params);
    add('Headers', item.headers);
    add('Body', item.body, item.bodyTruncated ? item.bodyLength : undefined);
    add('Response', item.status ? `${item.status} ${item.statusText ?? ''}`.trim() : undefined);
    add('Response data', item.data, item.dataTruncated ? item.dataLength : undefined);
    add('Error', item.error ? (item.code ? `${item.code}: ${item.error}` : item.error) : undefined);
    add('Duration', typeof item.durationMs === 'number' ? `${item.durationMs} ms` : undefined);
    return parts;
  }

  /** JSON (objects, or text that is JSON) indented; any other text as it is. A cut JSON text stays as it is. */
  static pretty(value: any): string {
    if (typeof value === 'string') {
      try {
        const parsed = JSON.parse(value);
        return parsed !== null && typeof parsed === 'object' ? JSON.stringify(parsed, null, 2) : value;
      } catch {
        return value;
      }
    }
    return JSON.stringify(value, null, 2);
  }

  /** The filters of the form: whole local days as ISO dates (the times shown are local), the url trimmed, the outcome. */
  static filtersOf(from: string, to: string, url: string, outcome: StatusLogOutcome | '' = ''): StatusLogFilters {
    const filters: StatusLogFilters = {};
    const day = (value: string, time: string) => {
      const date = new Date(`${value}T${time}`); // no offset: local time
      return isNaN(date.getTime()) ? undefined : date.toISOString();
    };
    if (from) filters.from = day(from, '00:00:00');
    if (to) filters.to = day(to, '23:59:59.999');
    if (url?.trim()) filters.url = url.trim();
    if (outcome) filters.outcome = outcome;
    for (const key of Object.keys(filters) as (keyof StatusLogFilters)[])
      if (filters[key] === undefined) delete filters[key];
    return filters;
  }
}
