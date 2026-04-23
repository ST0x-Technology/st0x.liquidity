import tailwindcss from '@tailwindcss/vite'
import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [tailwindcss(), sveltekit()],
  server: {
    proxy: {
      '/logs': 'http://localhost:8001',
      '/health': 'http://localhost:8001',
      '/orders': 'http://localhost:8001',
      '/trades': 'http://localhost:8001',
      '/transfers': 'http://localhost:8001',
    }
  }
})
