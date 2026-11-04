import { Component, OnDestroy, OnInit } from '@angular/core';
import { CacheAdminService, CacheVersion } from '../services/cache-admin.service';

/** The versions of one cached query. */
export interface CachedQuery {
  query: string;
  /** A short name: the survey, else the start of the query. */
  label: string;
  versions: CacheVersion[];
  active?: CacheVersion;
}

/** A backup: its timestamp and the versions it keeps. */
export interface CacheBackup {
  timestamp: number;
  versions: number;
  queries: number;
  /** All its versions are the active ones of their queries. */
  active: boolean;
}

/**
 * /cache (logged users only, LoggedInGuard): the cache of the GraphQL datapoints queries of the Query-Engine.
 *  - The queries with their versions (the active one is answered); any version can be made the active one.
 *  - "Deactivate active cache": the active versions (of the filtered queries, or all) become previous - nothing is
 *    deleted, the next request computes the data again; adminRoles of the Query-Engine needed, after a confirmation.
 *  - Backups: "Back up now" marks the active versions (kept until the backup is deleted); a backup can be made
 *    active again (all its versions), or deleted - with the management password (managementSecret of the
 *    Query-Engine, typed every time, never stored) and a confirmation.
 * An optional filter (part of the query text) narrows the list and what the backup takes.
 * Authentication disabled in the Query-Engine: every call needs its admin token (authConfig.adminToken, X-Admin-Token
 * header). When the Query-Engine asks for it, the page asks the user once and keeps it in memory until leaving the
 * page (never stored); deleting a backup still needs the management password as well.
 */
@Component({
  selector: 'app-cache',
  templateUrl: './cache.component.html',
  styleUrls: ['../shared/admin-page.scss', './cache.component.scss'],
  standalone: false
})
export class CacheComponent implements OnInit, OnDestroy {
  filter = '';
  private applied = '';
  queries: CachedQuery[] = [];
  backups: CacheBackup[] = [];
  loading = false;
  error: string | null = null;
  /** The outcome of the last action: a text key and its parameters. */
  lastMessage: { key: string; params: any } | null = null;
  actionError: string | null = null;
  busy = false;
  readonly opened = new Set<string>();

  /** Asking the confirmation of the deactivation; the filter it applies to ('' = every query). */
  deactivating: string | null = null;

  // deleting a backup
  secret = '';
  deleting: number | null = null;

  /** The Query-Engine asked for its admin token (authentication disabled there): the form is shown. */
  adminTokenNeeded = false;
  /** The token given was refused. */
  adminTokenWrong = false;
  /** The admin token typed in the form. */
  adminTokenInput = '';

  constructor(private api: CacheAdminService) { }

  ngOnInit(): void {
    this.load();
  }

  ngOnDestroy(): void {
    this.api.adminToken = ''; // only while the page is open
  }

  async load(): Promise<void> {
    this.loading = true;
    this.error = null;
    try {
      const rows = await this.api.list(this.applied || undefined);
      this.queries = CacheComponent.queriesOf(rows);
      this.backups = CacheComponent.backupsOf(rows);
      this.adminTokenNeeded = this.adminTokenWrong = false;
    } catch (error) {
      if (!this.askAdminToken(error))
        this.error = CacheComponent.messageOf(error);
    } finally {
      this.loading = false;
    }
  }

  /** The answer of the Query-Engine asks for its admin token: the form shown (true). */
  private askAdminToken(error: any): boolean {
    if (!CacheComponent.adminTokenAsked(error)) return false;
    this.adminTokenWrong = this.adminTokenNeeded && !!this.api.adminToken;
    this.adminTokenNeeded = true;
    this.queries = [];
    this.backups = [];
    return true;
  }

  /** The admin token of the form used from now on (until leaving the page), then the list again. */
  useAdminToken(): Promise<void> {
    const token = this.adminTokenInput.trim();
    if (!token) return Promise.resolve();
    this.api.adminToken = token;
    this.adminTokenInput = '';
    this.adminTokenWrong = false;
    return this.load();
  }

  /** 403 "X-Admin-Token missing or wrong": authentication disabled in the Query-Engine, its admin token needed. (Not
   *  when no admin token is configured there: then the message of the Query-Engine is shown as it is.) */
  static adminTokenAsked(error: any): boolean {
    return error?.status === 403 && /X-Admin-Token missing or wrong/.test(CacheComponent.messageOf(error));
  }

  search(): Promise<void> {
    this.applied = this.filter.trim();
    return this.load();
  }

  toggle(query: string): void {
    if (this.opened.has(query)) this.opened.delete(query);
    else this.opened.add(query);
  }

  backup(): Promise<void> {
    return this.act(async () => {
      const { timestamp, queries } = await this.api.backup(this.applied || undefined);
      return { key: 'Cache backup done', params: { queries, timestamp: new Date(timestamp).toLocaleString() } };
    });
  }

  /** First click: the confirmation (with the filter applied now). */
  askDeactivate(): void {
    this.deactivating = this.applied;
    this.actionError = null;
  }

  cancelDeactivate(): void {
    this.deactivating = null;
  }

  confirmDeactivate(): Promise<void> {
    const filter = this.deactivating;
    if (filter === null) return Promise.resolve();
    return this.act(async () => {
      const { reset } = await this.api.deactivate(filter || undefined);
      this.deactivating = null;
      return { key: 'Cache deactivated', params: { reset } };
    });
  }

  restoreBackup(backup: CacheBackup): Promise<void> {
    return this.act(async () => {
      const { restored, alreadyActive } = await this.api.restoreBackup(backup.timestamp);
      return { key: 'Cache backup restored', params: { restored, alreadyActive } };
    });
  }

  activate(version: CacheVersion): Promise<void> {
    return this.act(async () => {
      await this.api.activate(version._id);
      return { key: 'Cache version activated', params: { version: version.version } };
    });
  }

  /** First click: the confirmation of that backup (password asked there). */
  askDelete(backup: CacheBackup): void {
    this.deleting = backup.timestamp;
    this.actionError = null;
  }

  cancelDelete(): void {
    this.deleting = null;
  }

  confirmDelete(): Promise<void> {
    const timestamp = this.deleting;
    if (timestamp === null || !this.secret) return Promise.resolve();
    return this.act(async () => {
      const { released } = await this.api.deleteBackup(timestamp, this.secret);
      this.secret = '';
      this.deleting = null;
      return { key: 'Cache backup deleted', params: { released } };
    });
  }

  /** An action, then the list again; its outcome (or error) shown. */
  private async act(action: () => Promise<{ key: string; params: any }>): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    this.actionError = null;
    this.lastMessage = null;
    try {
      this.lastMessage = await action();
    } catch (error) {
      if (!this.askAdminToken(error))
        this.actionError = CacheComponent.messageOf(error);
    } finally {
      this.busy = false;
    }
    await this.load();
  }

  /** The versions grouped by query (active first, then newest first), queries by label. */
  static queriesOf(rows: CacheVersion[]): CachedQuery[] {
    const byQuery = new Map<string, CacheVersion[]>();
    for (const row of rows) {
      if (!byQuery.has(row.query)) byQuery.set(row.query, []);
      byQuery.get(row.query)!.push(row);
    }
    return [...byQuery.entries()].map(([query, versions]) => {
      versions.sort((a, b) => (a.state === 'active' ? -1 : 0) - (b.state === 'active' ? -1 : 0) || b.version - a.version);
      const active = versions.find(v => v.state === 'active');
      const survey = versions.find(v => v.survey)?.survey;
      return { query, label: survey || CacheComponent.shortQuery(query), versions, active };
    }).sort((a, b) => a.label.localeCompare(b.label));
  }

  /** The backups (timestamps of the versions), newest first. */
  static backupsOf(rows: CacheVersion[]): CacheBackup[] {
    const byTimestamp = new Map<number, CacheVersion[]>();
    for (const row of rows)
      for (const ts of row.backups || []) {
        if (!byTimestamp.has(ts)) byTimestamp.set(ts, []);
        byTimestamp.get(ts)!.push(row);
      }
    return [...byTimestamp.entries()].map(([timestamp, versions]) => ({
      timestamp,
      versions: versions.length,
      queries: new Set(versions.map(v => v.query)).size,
      active: versions.every(v => v.state === 'active'),
    })).sort((a, b) => b.timestamp - a.timestamp);
  }

  /** The query on one line, cut at 90 characters. */
  static shortQuery(query: string): string {
    const line = String(query ?? '').replace(/\s+/g, ' ').trim();
    return line.length > 90 ? line.slice(0, 89) + '…' : line;
  }

  static messageOf(error: any): string {
    const body = error?.error;
    if (typeof body === 'string' && body.trim()) return body;
    if (body && typeof body.error === 'string') return body.error;
    if (error?.status === 0) return 'Query-Engine not reachable';
    return error?.message || String(error);
  }
}
