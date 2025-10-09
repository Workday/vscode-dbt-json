import type { VSCodeApi } from '@shared/coder/types';
import { AppProvider } from '@web/context/app';
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react';
import { useMount, useUnmount } from 'react-use';
import { stateSync } from '../utils/stateSync';

type ColorMode = 'dark' | 'light';
type Environment = 'coder' | 'web';
type ThemeKey = `${Environment}-${ColorMode}`;

type EnvironmentContextValue = {
  environment: Environment;
  route: string | null;
  themeKey: ThemeKey;
  vscode: VSCodeApi | null;
};
const EnvironmentContext = createContext<EnvironmentContextValue | null>(null);

export function EnvironmentProvider() {
  const [vscode, setVscode] = useState<VSCodeApi | null>(null);

  const [colorMode, setColorMode] = useState<ColorMode>(
    window.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark'
      : 'light',
  );

  const route: string | null = useMemo(
    () =>
      document.getElementsByName('route')[0]?.getAttribute('content') || null,
    [],
  );

  const environment = useMemo<Environment>(
    () => (route ? 'coder' : 'web'),
    [route],
  );

  const themeKey = useMemo<ThemeKey>(
    () => `${environment}-${colorMode}`,
    [colorMode, environment],
  );

  const value: EnvironmentContextValue | null = useMemo(() => {
    if (route && !vscode) {
      return null;
    }
    const result = { environment, route, themeKey, vscode };

    return result;
  }, [environment, route, themeKey, vscode]);

  const handleColorModeChange = useCallback((e: MediaQueryListEvent) => {
    setColorMode(e.matches ? 'dark' : 'light');
  }, []);

  // If the desired theme changes, update here
  useEffect(() => {
    console.log('SETTING THEME KEY', themeKey);
    document.querySelector('html')?.setAttribute('data-theme', themeKey);
  }, [themeKey]);

  useMount(() => {
    window
      .matchMedia('(prefers-color-scheme: dark)')
      .addEventListener('change', handleColorModeChange);

    // Get VS Code API from StateSyncManager instead of acquiring it again
    const vsCodeApi = stateSync.getVSCodeApi();
    if (vsCodeApi) {
      setVscode(vsCodeApi);
    }
  });

  useUnmount(() => {
    window
      .matchMedia('(prefers-color-scheme: dark)')
      .removeEventListener('change', handleColorModeChange);
  });

  if (!value) {
    return null;
  }

  return (
    <div className="bg-background text-background-contrast">
      <EnvironmentContext.Provider value={value}>
        <AppProvider />
      </EnvironmentContext.Provider>
    </div>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useEnvironment() {
  const environmentContext = useContext(EnvironmentContext);
  if (!environmentContext) {
    throw new Error('useEnvironment must be used within EnvironmentProvider');
  }
  return environmentContext;
}
