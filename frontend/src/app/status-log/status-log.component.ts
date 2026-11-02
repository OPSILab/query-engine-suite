import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { BeopenAPIService, StatusLogFilters, StatusLogRecord } from '../services/be-open.service';
import { ConfigService } from '../services/config.service';
import { saveReturnUrl } from '../auth/services/return-url';

/** One part of the detail of a failed request: a label and its text (JSON pretty printed). */
export interface StatusLogDetail {
  label: string;
  text: string;
  /** The original length, when the Source-Connector cut the text (statusLog.maxDataSize). */
  cutAt?: number;
}

/**
 * /status-log: the failed HTTP requests of the Source-Connector (its statuslog collection, through GET /api/statuslog
 * of the Query-Engine), newest first, filtered by day and by part of the url, a page at a time. A click on a request
 * shows its query params, headers, body and response. The header values come hidden ("<masked>") unless the user is
 * an administrator (Query-Engine authConfig.adminRoles): the page says so, and offers the login when there is one.
 * No AuthGuard: readable without logging in (hidden values).
 */
@Component({
  selector: 'app-status-log',
  templateUrl: './status-log.component.html',
  styleUrls: ['./status-log.component.scss'],
  standalone: false
})
export class StatusLogComponent implements OnInit {
  static readonly PAGE_SIZE = 50;

  // the form: days (YYYY-MM-DD, <input type="date">) and part of the url
  from = '';
  to = '';
  url = '';

  items: StatusLogRecord[] = [];
  total = 0;
  masked = true;
  loading: 'list' | 'more' | null = null;
  error = false;
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
  }

  get hasMore(): boolean {
    return this.items.length < this.total;
  }

  /** Applies the form (the first page). */
  search(): Promise<void> {
    this.applied = StatusLogComponent.filtersOf(this.from, this.to, this.url);
    return this.load(false);
  }

  reset(): Promise<void> {
    this.from = this.to = this.url = '';
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

  /** client: 4xx; server: 5xx and no answer. */
  statusKind(item: StatusLogRecord): 'client' | 'server' {
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

  /** The filters of the form: whole local days as ISO dates (the times shown are local), the url trimmed. */
  static filtersOf(from: string, to: string, url: string): StatusLogFilters {
    const filters: StatusLogFilters = {};
    const day = (value: string, time: string) => {
      const date = new Date(`${value}T${time}`); // no offset: local time
      return isNaN(date.getTime()) ? undefined : date.toISOString();
    };
    if (from) filters.from = day(from, '00:00:00');
    if (to) filters.to = day(to, '23:59:59.999');
    if (url?.trim()) filters.url = url.trim();
    for (const key of Object.keys(filters) as (keyof StatusLogFilters)[])
      if (filters[key] === undefined) delete filters[key];
    return filters;
  }
}
