import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
  },
  build: {
    chunkSizeWarningLimit: 1600,
    rollupOptions: {
      output: {
        manualChunks: {
          vendor: ['react', 'react-dom', 'react-router-dom'],
          antd: ['antd', '@ant-design/icons', '@ant-design/pro-components'],
          charts: ['highcharts', 'highcharts-react-official', 'recharts', '@antv/g6'],
        },
      },
    },
  },
  optimizeDeps: {
    include: ['@ant-design/pro-components', '@antv/g6', 'rc-field-form'],
  },
})
