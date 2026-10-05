import { Component, ElementRef, HostListener } from '@angular/core';
import { THEMES, ThemeId, ThemeService } from '../../services/theme.service';

/**
 * Topbar button + menu to pick the UI theme. Each option shows a small
 * preview drawn with that theme's real colours: the preview element carries
 * data-ds-theme itself, so the tokens in styles.scss apply to it locally.
 */
@Component({
  selector: 'app-theme-picker',
  templateUrl: './theme-picker.component.html',
  styleUrls: ['./theme-picker.component.scss'],
  standalone: false
})
export class ThemePickerComponent {
  readonly themes = THEMES;
  open = false;

  constructor(public theme: ThemeService, private host: ElementRef<HTMLElement>) {}

  toggle(): void {
    this.open = !this.open;
    if (this.open) {
      // Focus the current theme, so the arrow keys / Tab start from there.
      setTimeout(() => this.host.nativeElement.querySelector<HTMLElement>('.tp-option--active')?.focus());
    }
  }

  pick(id: ThemeId): void {
    this.theme.set(id);
    this.close(true);
  }

  // Arrow keys move between options; Home/End jump to the ends.
  onMenuKeydown(event: KeyboardEvent): void {
    const options = Array.from(this.host.nativeElement.querySelectorAll<HTMLElement>('.tp-option'));
    const i = options.indexOf(document.activeElement as HTMLElement);
    const to = { ArrowDown: i + 1, ArrowUp: i - 1, Home: 0, End: options.length - 1 }[event.key];
    if (to === undefined) return;
    event.preventDefault();
    options[(to + options.length) % options.length]?.focus();
  }

  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    if (this.open && !this.host.nativeElement.contains(event.target as Node)) {
      this.close(false);
    }
  }

  @HostListener('document:keydown.escape')
  onEscape(): void {
    if (this.open) {
      this.close(true);
    }
  }

  private close(refocus: boolean): void {
    this.open = false;
    if (refocus) {
      this.host.nativeElement.querySelector<HTMLElement>('.tp-trigger')?.focus();
    }
  }
}
