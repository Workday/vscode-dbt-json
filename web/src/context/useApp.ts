import { useContext } from 'react';

import { AppContext } from './AppContext';

export function useApp() {
  const app = useContext(AppContext);
  if (!app) {
    throw new Error('useApp must be used within AppProvider');
  }
  return app;
}
