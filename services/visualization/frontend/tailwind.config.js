/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        'display': ['JetBrains Mono', 'monospace'],
        'body': ['IBM Plex Sans', 'system-ui', 'sans-serif'],
      },
      colors: {
        'madrid': {
          50: '#fef3f2',
          100: '#fee4e2',
          200: '#ffcdc9',
          300: '#fda9a3',
          400: '#f9756d',
          500: '#f04438',
          600: '#d92d20',
          700: '#b42318',
          800: '#912018',
          900: '#7a1a16',
        },
        'night': {
          50: '#f6f6f9',
          100: '#ececf2',
          200: '#d5d6e2',
          300: '#b1b2c8',
          400: '#8687a9',
          500: '#67688e',
          600: '#525376',
          700: '#444560',
          800: '#3a3b51',
          900: '#1a1b26',
          950: '#12131c',
        },
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'glow': 'glow 2s ease-in-out infinite alternate',
      },
      keyframes: {
        glow: {
          '0%': { boxShadow: '0 0 5px rgba(240, 68, 56, 0.3)' },
          '100%': { boxShadow: '0 0 20px rgba(240, 68, 56, 0.6)' },
        },
      },
    },
  },
  plugins: [],
}
