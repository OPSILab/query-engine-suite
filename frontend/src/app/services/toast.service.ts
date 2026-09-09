import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

/**
 * Replaces NbToastrService (part of the @nebular/theme "CDK overlay" family
 * that also caused the nb-autocomplete/toastr NbOverlayContainerAdapter
 * crashes this session - see app.component.ts's history). This is a plain
 * service + <ds-toast-container> pair with no overlay/portal machinery: the
 * container is a normal, always-mounted component (see AppComponent) that
 * just renders whatever is in `toasts$`.
 */
export type ToastStatus = 'success' | 'info' | 'warning' | 'danger';

export interface ToastItem {
  id: number;
  status: ToastStatus;
  title: string;
  message: string;
}

@Injectable({
  providedIn: 'root',
})
export class ToastService {
  private nextId = 1;
  private readonly _toasts = new BehaviorSubject<ToastItem[]>([]);
  readonly toasts$ = this._toasts.asObservable();

  /** Matches the old createToastr()'s default - NbToastrService also defaulted to 15s. */
  show(status: ToastStatus, title: string, message: string, duration = 15000): number {
    const id = this.nextId++;
    this._toasts.next([...this._toasts.value, { id, status, title, message }]);
    if (duration > 0) {
      setTimeout(() => this.dismiss(id), duration);
    }
    return id;
  }

  dismiss(id: number): void {
    this._toasts.next(this._toasts.value.filter(t => t.id !== id));
  }
}
