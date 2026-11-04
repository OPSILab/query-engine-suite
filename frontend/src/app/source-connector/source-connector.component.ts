import { Component, OnDestroy, OnInit } from '@angular/core';
import {
  IncrementalApi, RebuildStatus, ResetLogEntry, ResetStatus, SourceConnectorService,
} from '../services/source-connector.service';

/**
 * /source-connector (logged users only, LoggedInGuard): the management of the Source-Connector, called directly
 * (config.json "sourceConnectorBaseUrl").
 *  - Rebuild of keys, values and entries (POST /api/rebuild?mode=entries), of every connector or of one; its
 *    progress / result (GET /api/rebuild, read again every few seconds while it runs).
 *  - Reset of the incremental APIs: their records deleted and polled again from the start. The APIs come from a dry
 *    run (with their records); the reset needs the management password (managementSecret of the Source-Connector,
 *    typed every time, never stored) and a confirmation. Every reset stays in the log below (who, when, which APIs,
 *    records before / after, outcome), kept by the Source-Connector.
 */
@Component({
  selector: 'app-source-connector',
  templateUrl: './source-connector.component.html',
  styleUrls: ['../shared/admin-page.scss', './source-connector.component.scss'],
  standalone: false
})
export class SourceConnectorComponent implements OnInit, OnDestroy {
  static readonly POLL_MS = 3000;
  readonly connectors: { value: '' | 'api' | 'orion'; label: string }[] = [
    { value: '', label: 'All connectors' },
    { value: 'api', label: 'API' },
    { value: 'orion', label: 'Orion' },
  ];

  readonly configured: boolean;

  // rebuild
  connector: '' | 'api' | 'orion' = '';
  rebuild: RebuildStatus | null = null;
  rebuildError: string | null = null;
  rebuildStarting = false;

  // incremental reset
  apis: IncrementalApi[] = [];
  apisError: string | null = null;
  readonly selected = new Set<string>();
  secret = '';
  confirming = false;
  resetStarting = false;
  resetError: string | null = null;
  reset: ResetStatus | null = null;

  // log
  log: ResetLogEntry[] = [];
  logError: string | null = null;
  secretConfigured = true;

  private timer: any = null;

  constructor(private api: SourceConnectorService) {
    this.configured = !!api.baseUrl;
  }

  ngOnInit(): void {
    if (!this.configured) return;
    this.refresh();
    this.loadApis();
  }

  ngOnDestroy(): void {
    clearTimeout(this.timer);
  }

  /** The states of rebuild and reset and the log; again in a few seconds while one of them runs. */
  async refresh(): Promise<void> {
    clearTimeout(this.timer);
    await Promise.all([this.loadRebuild(), this.loadReset(), this.loadLog()]);
    if (this.rebuild?.running || this.reset?.running)
      this.timer = setTimeout(() => this.refresh(), SourceConnectorComponent.POLL_MS);
  }

  private async loadRebuild(): Promise<void> {
    try {
      this.rebuild = await this.api.getRebuildStatus();
    } catch (error) {
      this.rebuildError = SourceConnectorComponent.messageOf(error);
    }
  }

  private async loadReset(): Promise<void> {
    try {
      this.reset = await this.api.getResetStatus();
    } catch {
      // shown by the log / the actions
    }
  }

  private async loadLog(): Promise<void> {
    try {
      const { items, secretConfigured } = await this.api.getResetLog();
      this.log = items;
      this.secretConfigured = secretConfigured;
      this.logError = null;
    } catch (error) {
      this.logError = SourceConnectorComponent.messageOf(error);
    }
  }

  async loadApis(): Promise<void> {
    try {
      this.apis = await this.api.incrementalApis();
      this.apisError = null;
      for (const name of [...this.selected])
        if (!this.apis.some(api => api.name === name)) this.selected.delete(name);
    } catch (error) {
      this.apisError = SourceConnectorComponent.messageOf(error);
    }
  }

  async startRebuild(): Promise<void> {
    if (this.rebuildStarting || this.busy) return;
    this.rebuildStarting = true;
    this.rebuildError = null;
    try {
      await this.api.startRebuild(this.connector || undefined);
    } catch (error) {
      this.rebuildError = SourceConnectorComponent.messageOf(error);
    } finally {
      this.rebuildStarting = false;
    }
    await this.refresh();
  }

  toggleApi(name: string): void {
    if (this.selected.has(name)) this.selected.delete(name);
    else this.selected.add(name);
    this.confirming = false;
  }

  get selectedNames(): string {
    return [...this.selected].join(', ');
  }

  /** A rebuild or a reset running: the Source-Connector refuses another one (409). */
  get busy(): boolean {
    return !!(this.rebuild?.running || this.reset?.running);
  }

  get canReset(): boolean {
    return this.secretConfigured && this.selected.size > 0 && !!this.secret && !this.busy && !this.resetStarting;
  }

  /** First click: the confirmation (what will be deleted). */
  askReset(): void {
    if (this.canReset) this.confirming = true;
  }

  async confirmReset(): Promise<void> {
    if (!this.canReset) return;
    this.resetStarting = true;
    this.resetError = null;
    try {
      await this.api.startReset([...this.selected], this.secret);
      this.secret = '';
      this.selected.clear();
      this.confirming = false;
    } catch (error) {
      this.resetError = SourceConnectorComponent.messageOf(error);
    } finally {
      this.resetStarting = false;
    }
    await this.refresh();
  }

  cancelReset(): void {
    this.confirming = false;
  }

  /** The records of an API, as "MongoDB n · PostgreSQL n". */
  static counts(counts?: { mongo?: number; postgres?: number }): string {
    if (!counts) return '-';
    const parts: string[] = [];
    if (counts.mongo !== undefined) parts.push(`MongoDB ${counts.mongo}`);
    if (counts.postgres !== undefined) parts.push(`PostgreSQL ${counts.postgres}`);
    return parts.join(' · ') || '-';
  }

  counts(counts?: { mongo?: number; postgres?: number }): string {
    return SourceConnectorComponent.counts(counts);
  }

  /** The text of an HTTP error (the Source-Connector answers plain text), or its status. */
  static messageOf(error: any): string {
    if (typeof error?.error === 'string' && error.error.trim()) return error.error;
    if (error?.status === 0) return 'Source-Connector not reachable';
    return error?.message || String(error);
  }

  pretty(value: any): string {
    return JSON.stringify(value, null, 2);
  }
}
