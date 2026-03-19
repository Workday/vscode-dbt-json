import js from '@eslint/js';
import globals from 'globals';
import tseslint from '@typescript-eslint/eslint-plugin';
import tsparser from '@typescript-eslint/parser';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import jsxA11y from 'eslint-plugin-jsx-a11y';
import unusedImports from 'eslint-plugin-unused-imports';
import simpleImportSort from 'eslint-plugin-simple-import-sort';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default [
  // Main TypeScript/React config
  {
    files: ['**/*.{ts,tsx}'],
    ignores: [
      '**/*.config.{ts,js,mjs}',
      'vite.config.ts',
      'tailwind.defaults.ts',
    ],
    languageOptions: {
      parser: tsparser,
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
        ecmaFeatures: { jsx: true },
        project: './tsconfig.json',
        tsconfigRootDir: __dirname,
      },
      globals: {
        ...globals.browser,
        ...globals.es2021,
      },
    },
    plugins: {
      '@typescript-eslint': tseslint,
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
      'jsx-a11y': jsxA11y,
      'unused-imports': unusedImports,
      'simple-import-sort': simpleImportSort,
    },
    rules: {
      // ESLint recommended rules
      ...js.configs.recommended.rules,
      // TypeScript ESLint recommended rules (type-aware)
      ...tseslint.configs.recommended.rules,
      ...tseslint.configs['recommended-type-checked'].rules,
      // React Hooks recommended rules
      ...reactHooks.configs.recommended.rules,
      // JSX Accessibility recommended rules
      ...jsxA11y.flatConfigs.recommended.rules,

      // Custom rules
      // Import management
      'unused-imports/no-unused-imports': 'error',
      'unused-imports/no-unused-vars': [
        'warn',
        {
          vars: 'all',
          varsIgnorePattern: '^_',
          args: 'after-used',
          argsIgnorePattern: '^_',
        },
      ],
      'simple-import-sort/imports': 'error',
      'simple-import-sort/exports': 'error',

      'react-refresh/only-export-components': [
        'warn',
        { allowConstantExport: true },
      ],
      // Use unused-imports plugin instead of TypeScript's no-unused-vars
      '@typescript-eslint/no-unused-vars': 'off',
      '@typescript-eslint/consistent-type-imports': [
        'warn',
        { prefer: 'type-imports' },
      ],
      semi: 'warn',

      // Disable rules that TypeScript handles better
      'no-undef': 'off',
      'no-unused-vars': 'off', // Use unused-imports plugin instead
      'no-redeclare': 'off',
      '@typescript-eslint/no-redeclare': ['error'],

      // Relax some strict type-checked rules for practicality
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      // Type safety - keep as warnings to track technical debt
      '@typescript-eslint/no-explicit-any': 'warn',
      '@typescript-eslint/no-base-to-string': 'warn',
      '@typescript-eslint/restrict-template-expressions': 'warn',
      '@typescript-eslint/no-redundant-type-constituents': 'warn',
      '@typescript-eslint/no-unsafe-enum-comparison': 'warn',
      '@typescript-eslint/prefer-promise-reject-errors': 'warn',
      '@typescript-eslint/consistent-type-imports': 'warn',
      'react-refresh/only-export-components': 'warn', // Non-critical pattern
      'no-empty-pattern': 'warn',

      // Accessibility rules - disabled (UI uses custom patterns requiring UX redesign)
      'jsx-a11y/click-events-have-key-events': 'off',
      'jsx-a11y/no-static-element-interactions': 'off',
      'jsx-a11y/label-has-associated-control': 'off',
      'jsx-a11y/mouse-events-have-key-events': 'off',
      'jsx-a11y/alt-text': 'off',
    },
  },

  // Config files (Node.js environment) - disable type-checking for these
  {
    files: [
      '**/*.config.{js,ts,mjs}',
      'vite.config.ts',
      'tailwind.defaults.ts',
    ],
    languageOptions: {
      parser: tsparser,
      parserOptions: {
        ecmaVersion: 'latest',
        sourceType: 'module',
        // Don't use project for config files
      },
      globals: {
        ...globals.node,
      },
    },
    rules: {
      // Disable type-aware rules for config files
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      '@typescript-eslint/no-floating-promises': 'off',
    },
  },

  // Ignores
  {
    ignores: ['dist/', 'node_modules/'],
  },
];
