import { useContext } from 'react';

import { TrinoContext } from './TrinoContext';

export const useTrino = () => {
  const context = useContext(TrinoContext);
  if (!context) throw new Error('useTrino must be used within a TrinoProvider');
  return context;
};
