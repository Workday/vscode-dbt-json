import type { ApiHandler } from '@shared/api/types';
import type { VSCodeApi } from '@shared/types/config';
import { createContext } from 'react';

export type AppValue = {
  api: {
    post: ApiHandler;
  };
  environment: 'coder' | 'web';
  vscode: VSCodeApi | null;
};

export const AppContext = createContext<AppValue | null>(null);
