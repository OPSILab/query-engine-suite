import { Component } from '@angular/core';
import { NoticesService, SiteNotice } from '../../services/notices.service';

const DISMISSED_KEY = 'ds-dismissed-notices';

/**
 * The global notices of config.json ("notices.global"), in a bar under the header of every page (app-layout): only
 * when one is shown now. Info in blue, warning in yellow; the visitor can close one (remembered in this browser until
 * its text changes).
 */
@Component({
  selector: 'app-site-notices',
  templateUrl: './site-notices.component.html',
  styleUrls: ['./site-notices.component.scss'],
  standalone: false
})
export class SiteNoticesComponent {
  private dismissed = new Set<string>(SiteNoticesComponent.readDismissed());

  constructor(private notices: NoticesService) { }

  get shown(): SiteNotice[] {
    return this.notices.notices('global').filter(n => !this.dismissed.has(n.key));
  }

  close(notice: SiteNotice): void {
    this.dismissed.add(notice.key);
    try {
      localStorage.setItem(DISMISSED_KEY, JSON.stringify([...this.dismissed].slice(-50)));
    } catch {
      // closed for this visit only
    }
  }

  private static readDismissed(): string[] {
    try {
      const saved = JSON.parse(localStorage.getItem(DISMISSED_KEY) ?? '[]');
      return Array.isArray(saved) ? saved.filter(k => typeof k === 'string') : [];
    } catch {
      return [];
    }
  }
}
