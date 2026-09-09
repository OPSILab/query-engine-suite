import { Directive, ElementRef, HostListener, Input, OnDestroy, Renderer2 } from '@angular/core';

/**
 * Replaces nbPopoverTrigger="hover" + [nbPopover]="someTemplate" (part of the
 * same @nebular/theme CDK-overlay family as NbToastrService and
 * nb-autocomplete - see app.component.ts's history on NbOverlayContainerAdapter).
 * This appends a plain, absolutely-positioned <div class="ds-popover"> to
 * <body> on hover and removes it on mouseleave - no overlay/portal machinery,
 * so it can't hit the "no <nb-layout> registered as container" crash.
 *
 * Usage: <button [dsTooltip]="'Some text' | translate">...</button>
 * (replaces the old <ng-template #popoverTemplate let-data>{{ data.text }}</ng-template>
 * + [nbPopoverContext]="{ text: '...' }" indirection - just pass the string directly.)
 */
@Directive({
  selector: '[dsTooltip]',
})
export class TooltipDirective implements OnDestroy {
  @Input('dsTooltip') text: string;

  private tooltipEl: HTMLElement | null = null;

  constructor(private el: ElementRef<HTMLElement>, private renderer: Renderer2) {}

  @HostListener('mouseenter')
  show(): void {
    if (!this.text || this.tooltipEl) {
      return;
    }
    const tooltip = this.renderer.createElement('div');
    this.renderer.addClass(tooltip, 'ds-popover');
    this.renderer.addClass(tooltip, 'ds-popover--floating');
    this.renderer.appendChild(tooltip, this.renderer.createText(this.text));
    this.renderer.appendChild(document.body, tooltip);
    this.tooltipEl = tooltip;

    const hostRect = this.el.nativeElement.getBoundingClientRect();
    this.renderer.setStyle(tooltip, 'top', `${hostRect.bottom + 6}px`);
    this.renderer.setStyle(tooltip, 'left', `${hostRect.left}px`);

    // Clamp after layout so a tooltip near the right edge doesn't overflow the viewport.
    requestAnimationFrame(() => {
      if (!this.tooltipEl) {
        return;
      }
      const tRect = this.tooltipEl.getBoundingClientRect();
      const overflowRight = tRect.right - window.innerWidth + 12;
      if (overflowRight > 0) {
        this.renderer.setStyle(this.tooltipEl, 'left', `${hostRect.left - overflowRight}px`);
      }
    });
  }

  @HostListener('mouseleave')
  hide(): void {
    if (this.tooltipEl) {
      this.renderer.removeChild(document.body, this.tooltipEl);
      this.tooltipEl = null;
    }
  }

  ngOnDestroy(): void {
    this.hide();
  }
}
