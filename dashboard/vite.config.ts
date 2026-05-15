import tailwindcss from '@tailwindcss/vite'
import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vite'

const backendPort = process.env['BACKEND_PORT'] ?? '8001'
const backendUrl = `http://localhost:${backendPort}`

export default defineConfig({
  plugins: [tailwindcss(), sveltekit()],
  server: {
    proxy: {
      '/logs': backendUrl,
      '/health': backendUrl,
      '/orders': backendUrl,
      '/trades': backendUrl,
      '/transfers': backendUrl,
    }
  }
})
