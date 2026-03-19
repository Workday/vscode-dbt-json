import { createContext } from 'react';

export type TrinoValue = {
  setTables: (tables: string[]) => void;
  tables: null | string[];
};

export const TrinoContext = createContext<TrinoValue | null>(null);
