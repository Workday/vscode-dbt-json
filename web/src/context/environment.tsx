import type { VSCodeApi } from '@shared/types/config';
import { AppProvider } from '@web/context';
import { useEffect, useMemo, useState } from 'react';

import { stateSync } from '../utils/stateSync';
import {
  type ColorMode,
  type Environment,
  EnvironmentContext,
  type EnvironmentContextValue,
  type ThemeKey,
} from './EnvironmentContext';

/** Media query for OS dark mode preference */
const darkModeQuery = window.matchMedia('(prefers-color-scheme: dark)');

/** Detect VS Code theme from body class (vscode-dark/vscode-light) */
function getVSCodeTheme(): ColorMode | null {
  const { classList } = document.body;
  if (classList.contains('vscode-dark')) return 'dark';
  if (classList.contains('vscode-light')) return 'light';
  return null;
}

/** Get initial color mode: VS Code theme if available, else OS preference */
function getInitialColorMode(): ColorMode {
  return getVSCodeTheme() ?? (darkModeQuery.matches ? 'dark' : 'light');
}

export function EnvironmentProvider() {
  const [vscode, setVscode] = useState<VSCodeApi | null>(null);
  const [colorMode, setColorMode] = useState<ColorMode>(getInitialColorMode);

  const route = useMemo(
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

  const value = useMemo<EnvironmentContextValue | null>(() => {
    if (route && !vscode) return null;
    return { environment, route, themeKey, vscode };
  }, [environment, route, themeKey, vscode]);

  // Apply theme to document
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', themeKey);
  }, [themeKey]);

  // Set up theme change listeners based on environment
  useEffect(() => {
    const isVSCodeEnv = getVSCodeTheme() !== null;

    if (isVSCodeEnv) {
      // VS Code: watch body class changes
      const observer = new MutationObserver(() => {
        const theme = getVSCodeTheme();
        if (theme) setColorMode(theme);
      });
      observer.observe(document.body, {
        attributes: true,
        attributeFilter: ['class'],
      });
      return () => observer.disconnect();
    } else {
      // Web: watch OS preference changes
      const handler = (e: MediaQueryListEvent) =>
        setColorMode(e.matches ? 'dark' : 'light');
      darkModeQuery.addEventListener('change', handler);
      return () => darkModeQuery.removeEventListener('change', handler);
    }
  }, []);

  // Initialize VS Code API
  useEffect(() => {
    const vsCodeApi = stateSync.getVSCodeApi();
    if (vsCodeApi) setVscode(vsCodeApi);
  }, []);

  if (!value) {
    return null;
  }

  return (
    <div className="bg-background text-background-contrast max-h-full">
      <EnvironmentContext.Provider value={value}>
        <AppProvider />
      </EnvironmentContext.Provider>
    </div>
  );
}
