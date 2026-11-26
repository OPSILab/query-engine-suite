import { Component, OnDestroy, OnInit, ChangeDetectionStrategy } from '@angular/core';
import { ApiBackup, BackupLogEntry, BackupStatus, SourceConnectorService } from '../../services/source-connector.service';
import { SourceConnectorComponent } from '../source-connector.component';

/**
 * The backups of the apiConnector data, on the Source connector page (Source-Connector /api/backups,
 * utils/apiBackups.js): a backup of the data as they are now, a restore (the data as they are now backed up first,
 * then replaced by the backup chosen), a delete. Every action needs the management password (typed every time,
 * never kept) and, restore and delete, a confirmation. The list says when each backup was made, by whom, what it
 * holds and when it was last restored; the log keeps every action. Read again every few seconds while one runs.
 */
@Component({
  selector: 'app-api-backups',
  templateUrl: './api-backups.component.html',
  styleUrls: ['../../shared/admin-page.scss', '../source-connector.component.scss', './api-backups.component.scss'],
  changeDetection: ChangeDetectionStrategy.Eager,
  standalone: false
})
export class ApiBackupsComponent implements OnInit, OnDestroy {
  readonly configured: boolean;

  backups: ApiBackup[] = [];
  status: BackupStatus = { running: false };
  maxBackups = 0;
  secretConfigured = true;
  listError: string | null = null;
  log: BackupLogEntry[] = [];

  label = '';
  secret = '';
  /** The restore / delete waiting for its confirmation. */
  pending: { action: 'restore' | 'delete'; id: string } | null = null;
  starting = false;
  actionError: string | null = null;

  private timer: any = null;

  constructor(private api: SourceConnectorService) {
    this.configured = !!api.baseUrl;
  }

  ngOnInit(): void {
    if (this.configured) this.refresh();
  }

  ngOnDestroy(): void {
    clearTimeout(this.timer);
  }

  /** The list, the status and the log; again in a few seconds while an action runs. */
  async refresh(): Promise<void> {
    clearTimeout(this.timer);
    try {
      const { items, status, maxBackups, secretConfigured } = await this.api.getBackups();
      Object.assign(this, { backups: items, status, maxBackups, secretConfigured, listError: null });
      if (this.pending && !items.some(item => item.id === this.pending!.id)) this.pending = null;
    } catch (error) {
      this.listError = SourceConnectorComponent.messageOf(error);
    }
    try {
      this.log = await this.api.getBackupLog();
    } catch {
      // the list error says it already
    }
    if (this.status.running)
      this.timer = setTimeout(() => this.refresh(), SourceConnectorComponent.POLL_MS);
  }

  get canAct(): boolean {
    return this.secretConfigured && !!this.secret && !this.status.running && !this.starting;
  }

  async backUp(): Promise<void> {
    if (!this.canAct) return;
    await this.act(() => this.api.createBackup(this.label.trim(), this.secret), () => { this.label = ''; });
  }

  /** First click on restore / delete: the confirmation. */
  ask(action: 'restore' | 'delete', id: string): void {
    if (this.canAct) this.pending = { action, id };
  }

  cancel(): void {
    this.pending = null;
  }

  async confirm(): Promise<void> {
    const pending = this.pending;
    if (!pending || !this.canAct) return;
    await this.act(() => pending.action === 'restore'
      ? this.api.restoreBackup(pending.id, this.secret)
      : this.api.deleteBackup(pending.id, this.secret));
  }

  private async act(request: () => Promise<any>, done?: () => void): Promise<void> {
    this.starting = true;
    this.actionError = null;
    try {
      await request();
      this.secret = '';
      this.pending = null;
      done?.();
    } catch (error) {
      this.actionError = SourceConnectorComponent.messageOf(error);
    } finally {
      this.starting = false;
    }
    await this.refresh();
  }

  /** What a backup holds: "MongoDB n · PostgreSQL n". */
  static records(counts: ApiBackup['counts'] | undefined): string {
    return SourceConnectorComponent.counts({ mongo: counts?.records, postgres: counts?.postgres });
  }

  records(counts: ApiBackup['counts'] | undefined): string {
    return ApiBackupsComponent.records(counts);
  }

  /** The most recent restore among the backups (the one the data came from last). */
  get lastRestored(): string | undefined {
    return [...this.backups].filter(b => b.restoredAt).sort((a, b) => String(b.restoredAt).localeCompare(String(a.restoredAt)))[0]?.id;
  }
}
