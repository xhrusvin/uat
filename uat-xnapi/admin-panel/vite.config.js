import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const isProd = mode === 'production'

  return {
    plugins: [react()],
    // In production, app is served under /xnadmin/
    base: isProd ? '/xnadmin/' : '/',
    server: {
      port: 8051,
      proxy: {
        '/auth': { target: 'http://127.0.0.1:8050', changeOrigin: true },
        '/users': { target: 'http://127.0.0.1:8050', changeOrigin: true },
      }
    },
    build: { outDir: 'dist' }
  }
})
