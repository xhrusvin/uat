export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        brand: {
          50:  '#f0f9f2',
          100: '#dcf0e1',
          500: '#1e7a38',
          600: '#1a6830',
          700: '#155529',
          900: '#0f2d1a',
        },
        accent: '#1565a0',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      }
    }
  },
  plugins: []
}
