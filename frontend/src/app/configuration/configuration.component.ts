import { Component, OnInit } from '@angular/core';
import { PolledApi, PublicConfiguration, SourceConnectorService } from '../services/source-connector.service';
import { SourceConnectorComponent } from '../source-connector/source-connector.component';

const MINUTE = 60 * 1000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/**
 * /configuration (everyone, no guard): what can be seen of the configuration of the Source-Connector, read only
 * (GET /api/config, which shows nothing secret): the sources enabled (MinIO, Orion, APIs), how often the APIs are
 * polled, and every API polled - its url, whether it is paginated / batch, incremental or not and, if it is, its
 * start date and the longest window of one request (maxWindow).
 */
@Component({
  selector: 'app-configuration',
  templateUrl: './configuration.component.html',
  styleUrls: ['../shared/admin-page.scss', './configuration.component.scss'],
  standalone: false
})
export class ConfigurationComponent implements OnInit {
  readonly configured: boolean;
  configuration: PublicConfiguration | null = null;
  error: string | null = null;
  loading = false;

  readonly sources: { key: 'minio' | 'orion' | 'api'; label: string }[] = [
    { key: 'minio', label: 'MinIO' },
    { key: 'orion', label: 'Orion' },
    { key: 'api', label: 'External APIs' },
  ];

  constructor(private api: SourceConnectorService) {
    this.configured = !!api.baseUrl;
  }

  ngOnInit(): void {
    if (this.configured)
      void this.load();
  }

  async load(): Promise<void> {
    this.loading = true;
    try {
      this.configuration = await this.api.getConfiguration();
      this.error = null;
    } catch (error) {
      this.error = SourceConnectorComponent.messageOf(error);
    } finally {
      this.loading = false;
    }
  }

  /** A duration as an i18n key and its count: days, hours, minutes when exact, else ms; null when not set. */
  static duration(ms: number | null | undefined): { key: string; params: { count: number } } | null {
    if (typeof ms !== 'number' || !Number.isFinite(ms) || ms <= 0) return null;
    if (ms % DAY === 0) return { key: ms === DAY ? 'One day' : 'Days count', params: { count: ms / DAY } };
    if (ms % HOUR === 0) return { key: 'Hours count', params: { count: ms / HOUR } };
    if (ms % MINUTE === 0) return { key: 'Minutes count', params: { count: ms / MINUTE } };
    return { key: 'Milliseconds count', params: { count: ms } };
  }

  duration(ms: number | null | undefined): { key: string; params: { count: number } } | null {
    return ConfigurationComponent.duration(ms);
  }

  /** How the API is read: batch, paginated, or a single request. */
  static kind(api: PolledApi): string {
    if (api.batch) return 'Batch';
    if (api.paginated) return 'Paginated';
    return 'Single request';
  }

  kind(api: PolledApi): string {
    return ConfigurationComponent.kind(api);
  }

  get incrementalCount(): number {
    return (this.configuration?.apis || []).filter(api => api.incremental).length;
  }
}
