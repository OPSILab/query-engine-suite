// Unit tests (npm test): Vitest + happy-dom, Angular in JIT mode.
// Components and services are instantiated directly with fake dependencies (no TestBed, no templates):
// the tests cover the logic in the .ts files; templates are checked by the build (ngc).
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'happy-dom',
    include: ['src/**/*.spec.ts'],
    setupFiles: ['src/test-setup.ts'],
    restoreMocks: true,
  },
});
