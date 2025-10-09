import { defaultTheme } from './tailwind.defaults';

const colors = {
  background: 'var(--color-background)',
  'background-contrast': 'var(--color-background-contrast)',
  error: 'var(--color-error)',
  'error-contrast': 'var(--color-error-contrast)',
  info: 'var(--color-info)',
  'info-contrast': 'var(--color-info-contrast)',
  primary: 'var(--color-primary)',
  'primary-contrast': 'var(--color-primary-contrast)',
  secondary: 'var(--color-secondary)',
  'secondary-contrast': 'var(--color-secondary-contrast)',
  success: 'var(--color-success)',
  'success-contrast': 'var(--color-success-contrast)',
  warning: 'var(--color-warning)',
  'warning-contrast': 'var(--color-warning-contrast)',
};

/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  presets: [],
  darkMode: 'class', // or 'class'
  plugins: ['@tailwindcss/forms'],
  theme: {
    ...defaultTheme,
    extend: {
      backgroundColor: {
        ...colors,
      },
      colors,
      textColor: {
        DEFAULT: colors['background-contrast'],
        ...colors,
      },
      ringColor: {
        DEFAULT: colors['background-contrast'],
        ...colors,
      },
    },
  },
};
