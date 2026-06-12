import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const isProd = mode === 'production'

  const adminPort = parseInt(env.VITE_ADMIN_PORT || '8051')
  const apiPort   = parseInt(env.VITE_API_PORT   || '8050')

  return {
    plugins: [react()],
    base: isProd ? '/xnadmin/' : '/',
    server: {
      port: adminPort,
      proxy: {
        '/auth':  { target: `http://127.0.0.1:${apiPort}`, changeOrigin: true },
        '/users':  { target: `http://127.0.0.1:${apiPort}`, changeOrigin: true },
        '/shifts': { target: `http://127.0.0.1:${apiPort}`, changeOrigin: true },
        '/shifts-db': { target: `http://127.0.0.1:${apiPort}`, changeOrigin: true },
        '/common':   { target: `http://127.0.0.1:${apiPort}`, changeOrigin: true },
        '/clients':  { target: `http://127.0.0.1:${apiPort}`, changeOrigin: true },
      }
    },
    build: { outDir: 'dist' }
  }
})
