# data-space-standalone

Il `QueryEngineComponent` del dashboard BeOpen (le 3 modalità Simple search /
Advanced search / Query SQL, dentro `src/app/pages/data-space/query-engine`)
estratto in un progetto Angular 11 indipendente, con lo stesso login Keycloak
del dashboard, così può essere sviluppato, testato e versionato senza
tirarsi dietro tutto il dashboard.

## Cosa è cambiato rispetto all'originale

- **L'editor SQL non è più un `<iframe>`**. Nel dashboard, la modalità "Query
  SQL" caricava `src/app/pages/data-space/autocomplete/index.html` (un
  HTML/JS/CSS scritto a mano, copiato in `assets/autocomplete/` in fase di
  build) dentro un iframe, e comunicava col componente Angular via
  `window.postMessage`. Qui è stato sostituito da `SqlEditorComponent`
  (`src/app/data-space/query-engine/sql-editor/`), un vero componente
  Angular con `[(value)]`. Le query di esempio che avevi hardcodato in
  `script.js` sono state portate come lista di suggerimenti cliccabili
  (`@Input() suggestions`), invece del vecchio "ghost text" che si limitava a
  suggerire in grigio la prima corrispondenza senza un modo per accettarla.
- **Niente più variabile globale `sqlQuery`.** Nel file originale
  `export let sqlQuery` era una variabile a livello di modulo, scritta dal
  listener del `postMessage` e letta (senza `this.`) dentro `minioQuery()` —
  un dettaglio facile da perdere leggendo il codice. Ora è un campo
  d'istanza (`this.sqlQuery`) legato al nuovo editor.
- **Campi morti rimossi** da `QueryEngineComponent` (`pdfSrc`, `isImage`,
  `isText`, `isPdf`, `isGeoJson`, `dialogData`, `map`, ...): sembravano
  residui di copia-incolla da `DataSpaceComponent`, non venivano letti da
  nessuna parte in questo componente.
- **`BeopenAPIService` è ridotto** ai soli metodi usati dal query engine
  (`getUser`, `minioQuery`, `getKeys`, `getValues`, `getEntries`). Il
  servizio completo nel dashboard principale gestisce anche dataset, upload
  file, Superset, ecc.
- **`SharedService.userRoles$` parte da `[]` invece che da `null`**: nel
  dashboard viene valorizzato altrove (dopo il login), cosa che qui è
  invece popolata dal flusso di login stesso (vedi sotto) — ma partire da
  `null` avrebbe comunque fatto esplodere `getUser()` al primo
  `.includes(...)` durante il primo render, prima ancora che il login finisca.
- **Login Keycloak ereditato dal dashboard.** `NbAuthModule` +
  `NbOAuth2AuthStrategy`, `TokenInterceptor` (mette il JWT come header
  `Authorization: Bearer ...` su ogni chiamata API), `AuthGuard` (protegge la
  home), e le pagine di login/callback/logout sono la stessa identica
  configurazione del dashboard (stesso realm Keycloak, stesso client id).
  Vedi la sezione dedicata più sotto.

## Setup

```bash
npm install
npm start   # ng serve sulla porta 4300 (vedi angular.json), proxy /api -> src/proxy.conf.json
```

Prima di avviare, aggiorna `src/assets/config.json` con gli URL reali del
tuo backend (`beopenApiBaseUrl`, `queryEngineBaseUrl`) se sono diversi da
quelli di sviluppo del dashboard.

## Login Keycloak — cosa serve perché funzioni

Il flusso è quello del dashboard (Authorization Code, client pubblico
`beopen-dashboard`, stesso realm su `dx-lab.eng.it`), riconfigurato
a runtime in `app.component.ts` con gli URL letti da `config.json` — stesso
pattern del dashboard (`oauthStrategy.setOptions(...)` dopo che
`ConfigService` ha caricato).

**Prima di poter fare login da questo progetto**, in Keycloak (nel client
`beopen-dashboard`, realm master) devi aggiungere come *Valid Redirect URI*:

```
http://localhost:4300/keycloak-auth/callback
```

Senza questo, Keycloak rifiuterà il redirect dopo il login con un errore
tipo "Invalid parameter: redirect_uri" — è il motivo per cui la porta del
dev server è fissata a `4300` in `angular.json` (`serve.options.port`) e
`dashboardBaseURL` in `config.json` punta a quella stessa porta: se cambi
l'una devi cambiare anche l'altra, e aggiornare la redirect URI in Keycloak
di conseguenza.

Se preferisci isolare questo progetto dal client Keycloak del dashboard
(consigliato se lo usi parallelamente al dashboard vero, per non rischiare
di confondere sessioni/redirect), crea un client Keycloak separato e
aggiorna `client_id`/`client_secret` in `src/environments/environment.ts`.

Cosa succede dopo un login riuscito:
1. `AuthCallbackComponent` decodifica il JWT (via `OidcUserInformationService`)
   e pubblica i ruoli realm su `SharedService.userRoles$` — è quello che
   `QueryEngineComponent` legge per decidere `isAdmin`.
2. Prova anche a chiamare `beopenAPI.getUser()`: se esiste già un utente
   BeOpen associato, lo propaga; se dà 404 (utente Keycloak senza record
   BeOpen), lo logga e prosegue comunque — il dashboard qui reindirizzerebbe
   a `/register-user`, pagina che questo progetto non replica.
3. Redirect a `/` (la home con il query engine), protetta da `AuthGuard`.

Se vuoi disattivare il login in sviluppo (per lavorare solo sulla UI, senza
backend/Keycloak), metti `"enableAuthentication": false` in
`src/assets/config.json`: sia `AuthGuard` che `TokenInterceptor` lo
rispettano, esattamente come nel dashboard.

## Struttura

```
src/app/
  app.module.ts / app.component.ts       bootstrap + NbAuthModule + oauthStrategy.setOptions(...)
  app-routing.module.ts                  '/', '/keycloak-auth(/callback|/logout)'
  home/                                  nb-layout + <ngx-query-engine> (era dentro app.component)
  auth/
    oidc.ts                              UserClaims + OidcJWTToken (identico al dashboard)
    login/ callback/ logout/             pagine di login Keycloak (identiche al dashboard)
    services/
      auth.guard.ts                      protegge '/'
      token.interceptor.ts               header Authorization: Bearer <jwt>
      oidc-user-information.service.ts   decodifica il JWT in UserClaims
  data-space/
    data-space.service.ts                BucketObjectsPush (copiato identico dal dashboard)
    query-engine/
      query-engine.component.*           il componente estratto
      autocomplete/                      autocomplete chiave/valore per "Advanced search" (già Angular, copiato identico)
      sql-editor/                        NUOVO — sostituisce l'iframe di "Query SQL"
  services/
    be-open.service.ts                   ridotto ai metodi usati qui
    shared.service.ts
  model/                                 BeopenUser, BucketObject, DataSpaceFileEntity
```

## Riportare le modifiche nel dashboard principale

Se il piano è sviluppare qui e poi riportare indietro nel dashboard:
`query-engine.component.ts/html/scss`, `autocomplete/`, `sql-editor/` e
`data-space.service.ts` sono pensati per essere copiati così come sono
dentro `src/app/pages/data-space/` del dashboard (stesso Angular 11, stesso
stile a NgModule). Ricorda però di:
- rimuovere dal dashboard il glob in `angular.json` che copia
  `src/app/pages/data-space/autocomplete/` in `assets/autocomplete/`, e i
  file `autocomplete/index.html`, `script.js`, `styles.css`;
- registrare `SqlEditorComponent` nelle `declarations` di `data-space.module.ts`;
- il dashboard ha già `ConfigService`, `SharedService`, `BeopenAPIService`,
  `NbAuthModule`, `TokenInterceptor`, ecc. completi: i file auth qui dentro
  sono stati portati per far funzionare *questo* progetto standalone, non
  vanno ricopiati sopra quelli del dashboard.
