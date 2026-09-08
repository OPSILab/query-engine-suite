// Same Keycloak client the main dashboard uses (src/environments/environment.ts
// there) - same realm, same login. If you'd rather isolate this project,
// register a separate client in Keycloak and swap these.
export const environment = {
  production: false,
  keycloak: {
    client_id: 'dmm',
    client_secret: 'C6RnfcTCKZHyIn2F5zxmgaRCaJn9YHCp',
  },
};
