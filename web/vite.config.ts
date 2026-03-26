import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import svgr from 'vite-plugin-svgr';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig({
  build: {
    emptyOutDir: true,
    rollupOptions: {
      input: './src/main.tsx',
      output: {
        assetFileNames: `assets/[name].[ext]`,
        dir: '../dist/web',
        entryFileNames: `assets/[name].js`,
        chunkFileNames: `assets/[name].js`,
      },
    },
  },
  plugins: [
    react(),
    svgr({
      // Enable SVG imports as React components with ?react suffix
      svgrOptions: {
        exportType: 'default',
        ref: true,
        svgo: false,
        titleProp: true,
      },
      include: '**/*.svg?react',
    }),
  ],
  resolve: {
    alias: {
      '@shared': path.resolve(__dirname, '../src/shared'),
      '@web': path.resolve(__dirname, './src'),
      '@web/context': path.resolve(__dirname, './src/context'),
      '@web/elements': path.resolve(__dirname, './src/elements'),
      '@web/forms': path.resolve(__dirname, './src/forms'),
      '@web/hooks': path.resolve(__dirname, './src/hooks'),
      '@web/pages': path.resolve(__dirname, './src/pages'),
      '@web/styles': path.resolve(__dirname, './src/styles'),
    },
  },
});
