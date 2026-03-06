import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      '/ws': {
        target: 'ws://localhost:8004',
        ws: true,
      },
      '/api': {
        target: 'http://localhost:8004',
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
  },
})
