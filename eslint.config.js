import js from '@eslint/js';
import ts from '@typescript-eslint/eslint-plugin';
import tsParser from '@typescript-eslint/parser';
import nxPlugin from '@nx/eslint-plugin';

export default [
  {
    ignores: ['dist', 'node_modules', 'target', '.nx', 'coverage'],
  },
  js.configs.recommended,
  {
    files: ['**/*.ts', '**/*.tsx'],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: 2022,
        sourceType: 'module',
        project: true,
      },
    },
    plugins: {
      '@typescript-eslint': ts,
      '@nx': nxPlugin,
    },
    rules: {
      '@typescript-eslint/no-explicit-any': ['warn'],
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      '@typescript-eslint/explicit-function-return-types': ['error'],
      '@nx/enforce-module-boundaries': [
        'error',
        {
          enforceBuildableLibPattern: '^[a-zA-Z0-9-]+(/[a-zA-Z0-9-]+)*$',
          allow: ['^.*/eslint\\.config\\.[jt]s$'],
          depConstraints: [
            {
              sourceTag: 'type:app',
              onlyDependOnLibsWithTags: ['type:lib', 'type:service'],
            },
            {
              sourceTag: 'type:service',
              onlyDependOnLibsWithTags: ['type:lib'],
            },
          ],
        },
      ],
    },
  },
];
