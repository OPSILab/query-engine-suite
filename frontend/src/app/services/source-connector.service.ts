import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { firstValueFrom } from 'rxjs';
import { ConfigService } from './config.service';

/** Records of an API: in MongoDB and PostgreSQL (each one only if the Source-Connector writes there). */
export interface RecordCounts { mongo?: number; postgres?: number; }

/** GET /api/rebuild: the last (or running) rebuild. */
export interface RebuildStatus {
  running: boolean;
  mode?: string;
  connector?: string | null;
  startedAt?: string;
  finishedAt?: string;
  result?: any;
  error?: string;
}

/** GET /api/incremental/reset: the last (or running) reset. */
export interface ResetStatus {
  running: boolean;
  phase?: 'deleting' | 'polling' | 'done';
  apis?: string[];
  startedAt?: string;
  finishedAt?: string;
  result?: { name: string; url: string; before?: RecordCounts; after?: RecordCounts }[];
  error?: string;
}

/** An incremental API with its records (dry run of the reset). */
export interface IncrementalApi { name: string; url: string; records: RecordCounts; }

/** A reset done (GET /api/incremental/reset/log), kept by the Source-Connector. */
export interface ResetLogEntry {
  id: string;
  startedAt: string;
  finishedAt?: string;
  by: string;
  apis: string[];
  result?: { name: string; url: string; before?: RecordCounts; after?: RecordCounts }[];
  outcome: 'running' | 'done' | 'error' | 'interrupted';
  error?: string;
}

/**
 * The management API of the Source-Connector, called directly (config.json "sourceConnectorBaseUrl", e.g.
 * http://localhost:3000; the requests get the user's token from TokenInterceptor): rebuild of keys / values /
 * entries, reset of the incremental APIs (with the management password) and its log.
 */
@Injectable({ providedIn: 'root' })
export class SourceConnectorService {
  constructor(private http: HttpClient, private config: ConfigService) { }

  /** The base url of the Source-Connector; '' when not configured (the page says so). */
  get baseUrl(): string {
    const url = this.config.getSettings('sourceConnectorBaseUrl', '');
    return typeof url === 'string' ? url.replace(/\/+$/, '') : '';
  }

  private url(path: string): string {
    return `${this.baseUrl}/api${path}`;
  }

  getRebuildStatus(): Promise<RebuildStatus> {
    return firstValueFrom(this.http.get<RebuildStatus>(this.url('/rebuild')));
  }

  /** Rebuilds keys, values and entries (mode=entries) of one connector, or of api and orion. */
  startRebuild(connector?: 'api' | 'orion'): Promise<any> {
    const params: Record<string, string> = { mode: 'entries', ...(connector ? { connector } : {}) };
    return firstValueFrom(this.http.post(this.url('/rebuild'), {}, { params }));
  }

  /** The incremental APIs with their records (dry run: nothing changed, no password needed). */
  incrementalApis(): Promise<IncrementalApi[]> {
    return firstValueFrom(this.http.post<{ apis: IncrementalApi[] }>(this.url('/incremental/reset'), {}, { params: { dryRun: 'true' } }))
      .then(res => (Array.isArray(res?.apis) ? res.apis : []));
  }

  getResetStatus(): Promise<ResetStatus> {
    return firstValueFrom(this.http.get<ResetStatus>(this.url('/incremental/reset')));
  }

  /** Deletes the records of the APIs named and polls them again from the start; the management password needed. */
  startReset(names: string[], secret: string): Promise<{ started: boolean; apis: string[] }> {
    return firstValueFrom(this.http.post<{ started: boolean; apis: string[] }>(this.url('/incremental/reset'), { names },
      { headers: { 'X-Management-Secret': secret } }));
  }

  getResetLog(): Promise<{ items: ResetLogEntry[]; secretConfigured: boolean }> {
    return firstValueFrom(this.http.get<{ items: ResetLogEntry[]; secretConfigured: boolean }>(this.url('/incremental/reset/log')))
      .then(res => ({ items: Array.isArray(res?.items) ? res.items : [], secretConfigured: res?.secretConfigured === true }));
  }
}
