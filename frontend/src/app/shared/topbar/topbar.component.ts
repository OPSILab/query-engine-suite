import { Component, EventEmitter, Input, Output } from '@angular/core';
import { ThemeService } from '../../services/theme.service';

/**
 * The bar at the top of every page: the logos and the title (a link to the query engine), then the page's own
 * controls, given as content (<app-topbar>...controls...</app-topbar>). Its styles are global (styles.scss,
 * .ds-topbar), so that they also apply to the controls of the page.
 * showMenuButton: on phones, a ☰ button before the logos that opens the side menu (app-layout, menuToggle).
 */
@Component({
  selector: 'app-topbar',
  templateUrl: './topbar.component.html',
  standalone: false
})
export class TopbarComponent {
  @Input() showMenuButton = false;
  @Input() menuOpen = false;
  @Output() menuToggle = new EventEmitter<void>();

  constructor(public theme: ThemeService) { }
}
