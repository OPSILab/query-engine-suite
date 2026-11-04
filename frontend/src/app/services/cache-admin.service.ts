import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { firstValueFrom } from 'rxjs';
import { ConfigService } from './config.service';

/** A version of a cached query (GET /api/listCache of the Query-Engine). */
export interface CacheVersion {
  _id: string;
  /** The GraphQL query (as text): the key of the cache. */
  query: string;
  version: number;
  state: 'building' | 'active' | 'previous' | 'deleting';
  count?: number;
  /** The backups (timestamps, ms) that keep this version. */
  backups?: number[];
  createdAt?: string;
  activatedAt?: string;
  replacedAt?: string;
  survey?: string;
}

/**
 * The cache of the GraphQL datapoints queries of the Query-Engine (services/queryCache.js): versions per query, the
 * active one answered. Any logged user lists them, makes a backup (the active versions get its timestamp and are
 * kept), and chooses the active version (one, or all those of a backup); deleting a backup also needs the management
 * password. Deactivating the active versions (POST /minio/resetCache) needs one of authConfig.adminRoles of the
 * Query-Engine (any logged user when it is empty).
 */
@Injectable({ providedIn: 'root' })
export class CacheAdminService {
  constructor(private http: HttpClient, private config: ConfigService) { }

  private get base(): string {
    const settings = this.config.getSettings();
    return `${settings.queryEngineBaseUrl || settings.beopenApiBaseUrl}/api`;
  }

  /** Every version (filter: part of the query, case insensitive). */
  list(filter?: string): Promise<CacheVersion[]> {
    const params: Record<string, string> = filter ? { queriesMapFilter: filter } : {};
    return firstValueFrom(this.http.get<CacheVersion[]>(`${this.base}/listCache`, { params })).then(rows => (Array.isArray(rows) ? rows : []));
  }

  /** A backup of the active versions (of the queries matching `filter`). */
  backup(filter?: string): Promise<{ timestamp: number; queries: number }> {
    const params: Record<string, string> = filter ? { queriesMapFilter: filter } : {};
    return firstValueFrom(this.http.get<{ timestamp: number; queries: number }>(`${this.base}/backupCache`, { params }));
  }

  /** The active versions (of the queries matching `filter`) become previous: kept as history, the next request
   *  computes the data again and writes a new active version. */
  deactivate(filter?: string): Promise<{ reset: number }> {
    const params: Record<string, string> = filter ? { queriesMapFilter: filter } : {};
    return firstValueFrom(this.http.post<{ reset: number }>(`${this.base}/minio/resetCache`, {}, { params }));
  }

  /** The versions of a backup active again. */
  restoreBackup(timestamp: number): Promise<{ restored: number; alreadyActive: number }> {
    return firstValueFrom(this.http.get<{ restored: number; alreadyActive: number }>(`${this.base}/restoreCache`, { params: { timestamp: String(timestamp) } }));
  }

  /** One version active for its query. */
  activate(id: string): Promise<{ restored: number; alreadyActive: number }> {
    return firstValueFrom(this.http.get<{ restored: number; alreadyActive: number }>(`${this.base}/restoreCache`, { params: { id } }));
  }

  /** Deletes a backup (its versions beyond keepVersions are then deleted); the management password needed. */
  deleteBackup(timestamp: number, secret: string): Promise<{ released: number }> {
    return firstValueFrom(this.http.post<{ released: number }>(`${this.base}/resetBackup`, {},
      { params: { timestamp: String(timestamp) }, headers: { 'X-Management-Secret': secret } }));
  }
}
