import { useContext } from 'react';

import { EnvironmentContext } from './EnvironmentContext';

export function useEnvironment() {
  const environmentContext = useContext(EnvironmentContext);
  if (!environmentContext) {
    throw new Error('useEnvironment must be used within EnvironmentProvider');
  }
  return environmentContext;
}
