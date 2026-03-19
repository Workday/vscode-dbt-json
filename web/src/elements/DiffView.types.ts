export enum DiffViewMode {
  UNIFIED = 'unified',
  SPLIT = 'split',
}

export interface DiffViewProps {
  original: string;
  modified: string;
  language: 'json' | 'yaml' | 'sql';
  theme: 'light' | 'dark';
  viewMode?: DiffViewMode;
  className?: string;
}
