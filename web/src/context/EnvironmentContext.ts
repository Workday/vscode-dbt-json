import type { VSCodeApi } from '@shared/types/config';
import { createContext } from 'react';

export type ColorMode = 'dark' | 'light';
export type Environment = 'coder' | 'web';
export type ThemeKey = `${Environment}-${ColorMode}`;

export interface EnvironmentContextValue {
  environment: Environment;
  route: string | null;
  themeKey: ThemeKey;
  vscode: VSCodeApi | null;
}

export const EnvironmentContext = createContext<EnvironmentContextValue | null>(
  null,
);
