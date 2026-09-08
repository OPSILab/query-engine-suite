import { ExtraOptions, RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';
import { NbAuthComponent } from '@nebular/auth';

import { HomeComponent } from './home/home.component';
import { AuthLoginComponent } from './auth/login/auth-login.component';
import { AuthCallbackComponent } from './auth/callback/auth-callback.component';
import { AuthLogoutComponent } from './auth/logout/auth-logout.component';
import { AuthGuard } from './auth/services/auth.guard';

export const routes: Routes = [
  {
    path: '',
    component: HomeComponent,
    canActivate: [AuthGuard],
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
