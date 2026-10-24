import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // In dev, proxy API calls to the FastAPI backend
      '/messages': 'http://localhost:8000',
      '/queue':    'http://localhost:8000',
      '/throttle': 'http://localhost:8000',
    },
  },
  build: {
    outDir: 'dist',
  },
})
