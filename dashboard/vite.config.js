import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Proxy /api sang gateway. Nhờ vậy mã frontend chỉ gọi đường dẫn tương đối
// "/api/..." và không cần biết gateway ở đâu — chạy dev trên máy hay chạy
// trong Docker đều dùng chung một đoạn mã.
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.VITE_GATEWAY || 'http://api-gateway:8090',
        changeOrigin: true,
      },
    },
  },
  preview: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.VITE_GATEWAY || 'http://api-gateway:8090',
        changeOrigin: true,
      },
    },
  },
})
