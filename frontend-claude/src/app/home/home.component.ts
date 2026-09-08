import { Component } from '@angular/core';

// This used to be AppComponent's own content, before AppComponent became a
// bare <router-outlet> to make room for the /keycloak-auth routes.
@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
})
export class HomeComponent {
  // Same default the dashboard passes to <ngx-query-engine [visibility]="'private'">
  // on the "private" tab of the Data Space page. In the dashboard this comes
  // from which tab you're on (private/shared/public are 3 separate page
  // instances); here there's a single query engine, so it's a plain select.
  visibilities = ['private', 'shared', 'public'];
  visibility = this.visibilities[0];

  generalSharedBucketObjects: any[] = [];
  pilotSharedBucketObjects: any[] = [];
  userBucketObjects: any[] = [];
  extractedElements: any[] = [];
}
