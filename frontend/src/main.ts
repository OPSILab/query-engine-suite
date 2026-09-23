import { enableProdMode, provideZoneChangeDetection } from '@angular/core';
// Was platformBrowserDynamic() from @angular/platform-browser-dynamic. That
// package is deprecated as of Angular 20: it stopped at 20.0.7 and pins its
// peers to that exact version, so it blocks `ng update` for the whole
// @angular/* family. It only ever mattered for JIT compilation, and the CLI
// has built AOT-only since v12 - platformBrowser() from @angular/platform-browser
// is the AOT equivalent and boots the same NgModule.
import { platformBrowser } from '@angular/platform-browser';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

if (environment.production) {
  enableProdMode();
}

// provideZoneChangeDetection() was added by the Angular 21 migration: from v21
// the default is zoneless, and this keeps the app on Zone.js-based change
// detection, which is what it has always relied on (Nebular included). Dropping
// it means auditing every async update for missing refreshes - a separate job.
platformBrowser()
  .bootstrapModule(AppModule, {
    applicationProviders: [provideZoneChangeDetection()],
  })
  .catch(err => console.error(err));
