import { Component } from '@angular/core';
import { ToastService } from '../../services/toast.service';

// Mounted once, at the app root (see app.component.ts) so it survives route
// changes and shows toasts fired from anywhere - the same role NbToastrModule
// used to play, minus the CDK overlay it required <nb-layout> for.
@Component({
  selector: 'ds-toast-container',
  templateUrl: './toast-container.component.html',
  styleUrls: ['./toast-container.component.scss'],
})
export class ToastContainerComponent {
  constructor(public toastService: ToastService) {}
}
