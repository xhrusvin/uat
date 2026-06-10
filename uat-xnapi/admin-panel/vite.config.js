import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 8051,
    proxy: {
      '/auth': {
        target: 'http://127.0.0.1:8050',
        changeOrigin: true,
      },
      '/users': {
        target: 'http://127.0.0.1:8050',
        changeOrigin: true,
      },
    }
  },
  build: { outDir: 'dist' }
})
