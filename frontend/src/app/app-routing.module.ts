import { ExtraOptions, RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';
import { NbAuthComponent } from '@nebular/auth';

import { HomeComponent } from './home/home.component';
import { AuthLoginComponent } from './auth/login/auth-login.component';
import { AuthCallbackComponent } from './auth/callback/auth-callback.component';
import { AuthLogoutComponent } from './auth/logout/auth-logout.component';
import { AuthGuard } from './auth/services/auth.guard';
import { StatusLogComponent } from './status-log/status-log.component';
import { LoggedInGuard } from './auth/services/logged-in.guard';
import { SourceConnectorComponent } from './source-connector/source-connector.component';
import { CacheComponent } from './cache/cache.component';

export const routes: Routes = [
  {
    path: '',
    component: HomeComponent,
    canActivate: [AuthGuard],
  },
  // The failed requests of the Source-Connector: no AuthGuard, readable without logging in (the Query-Engine hides
  // the header values unless the user is an administrator)
  {
    path: 'status-log',
    component: StatusLogComponent,
  },
  // Logged users only (hidden from the menu otherwise): the management of the Source-Connector and of the cache
  {
    path: 'source-connector',
    component: SourceConnectorComponent,
    canActivate: [LoggedInGuard],
  },
  {
    path: 'cache',
    component: CacheComponent,
    canActivate: [LoggedInGuard],
  },
  {
    path: 'keycloak-auth',
    component: NbAuthComponent,
    children: [
      { path: '', component: AuthLoginComponent },
      { path: 'callback', component: AuthCallbackComponent },
      { path: 'logout', component: AuthLogoutComponent },
    ],
  },
  { path: '**', redirectTo: '' },
];

const config: ExtraOptions = {
  useHash: false,
};

@NgModule({
  imports: [RouterModule.forRoot(routes, config)],
  exports: [RouterModule],
})
export class AppRoutingModule {
}
