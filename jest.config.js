/** @type {import('jest').Config} */
const config = {
  preset: 'ts-jest',
  moduleNameMapper: {
    admin: ['<rootDir>/src/admin'],
    '@services/(.*)': ['<rootDir>/src/services/$1'],
    '@services': ['<rootDir>/src/services'],
    '@shared/(.*)': ['<rootDir>/src/shared/$1'],
    '@shared': ['<rootDir>/src/shared'],
    '@web/(.*)': ['<rootDir>/web/src/shared/$1'],
    '@web': ['<rootDir>/web/src/shared'],
  },
  testPathIgnorePatterns: [
    '<rootDir>/out/',
    '<rootDir>/dist/',
    '<rootDir>/venv/',
    '<rootDir>/.venv/',
  ],
};

module.exports = config;
