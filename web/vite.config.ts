import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  base: './',
  plugins: [react()],
  build: {
    outDir: '../src/tally/resources/web',
    emptyOutDir: true,
    // No sourcemap in the production bundle. The build output is committed so
    // that `git clone && uv run tally serve` works without a Node toolchain,
    // and a sourcemap would mean publishing 600 KB of readable frontend source
    // to every visitor who opens the dashboard. `npm run dev` still has full
    // sourcemaps, which is where they are actually useful.
    sourcemap: false,
  },
  server: {
    port: 5173,
    proxy: {
      '/v1': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: false,
      },
    },
  },
});
