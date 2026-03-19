import { useTutorialStore } from '@web/stores/useTutorialStore';
import { stateSync } from '@web/utils/stateSync';
import { useCallback, useEffect, useRef } from 'react';

const USER_SETTINGS_FORM_TYPE = 'user-settings';

/**
 * Hook to manage Assist Mode state persistence
 * - VS Code Extension: Saves to .vscode/temp-current-user-settings.json via stateSync
 * - Web Mode: Uses useTutorialStore (in-memory)
 *
 * IMPORTANT: Settings are only loaded AFTER isFormReady becomes true
 * This prevents the tutorial from starting before the form is fully loaded
 */
export function useAssistMode() {
  const {
    assistModeEnabled,
    hasCompletedIntro,
    isFormReady,
    enableAssistMode,
    disableAssistMode,
    toggleAssistMode,
    setHasCompletedIntro,
  } = useTutorialStore();

  /**
   * Load user settings from persistence
   */
  const loadSettings = useCallback(async () => {
    try {
      const data = await stateSync.loadState(USER_SETTINGS_FORM_TYPE);

      if (data && typeof data === 'object') {
        // Apply loaded settings to store
        if (typeof data.hasCompletedIntro === 'boolean') {
          setHasCompletedIntro(data.hasCompletedIntro);
        }

        // Apply assist mode setting
        const assistEnabled = data.assistModeEnabled === true;
        if (assistEnabled) {
          enableAssistMode();
        } else {
          disableAssistMode();
        }
      } else {
        // No saved settings found (completely new user)
        // Enable assist mode by default and ensure hasCompletedIntro is false
        setHasCompletedIntro(false);
        enableAssistMode();
      }
    } catch (error) {
      console.error('[useAssistMode] Error loading settings:', error);
      // On error, enable assist mode by default (fail-safe)
      enableAssistMode();
    }
  }, [setHasCompletedIntro, enableAssistMode, disableAssistMode]);

  /**
   * Save current settings to persistence
   */
  const saveSettings = useCallback(async () => {
    try {
      const data = {
        hasCompletedIntro,
        assistModeEnabled,
        lastUpdated: new Date().toISOString(),
      };

      await stateSync.saveState(USER_SETTINGS_FORM_TYPE, data);
    } catch (error) {
      console.error('[useAssistMode] Error saving settings:', error);
    }
  }, [hasCompletedIntro, assistModeEnabled]);

  /**
   * Load settings only when form is ready
   * This prevents the tutorial from starting before the form is fully loaded
   */
  const hasLoadedRef = useRef(false);

  useEffect(() => {
    // Only load settings once form is ready
    if (!isFormReady) {
      return;
    }

    // Prevent double-loading
    if (hasLoadedRef.current) {
      return;
    }

    void loadSettings().then(() => {
      hasLoadedRef.current = true;
    });
  }, [isFormReady, loadSettings]);

  /**
   * Save settings whenever they change (but only after initial load)
   */
  useEffect(() => {
    if (hasLoadedRef.current) {
      void saveSettings();
    }
  }, [saveSettings, hasCompletedIntro, assistModeEnabled]);

  /**
   * Mark intro as completed and persist
   */
  const markIntroAsCompleted = () => {
    setHasCompletedIntro(true);
    // saveSettings will be triggered by useEffect
  };

  /**
   * Toggle Assist Mode and persist
   * If disabling, marks intro as completed to prevent auto-start on next page load
   * If enabling, tutorial will start via useAssistTutorial (regardless of hasCompletedIntro)
   */
  const toggleAssist = () => {
    if (assistModeEnabled) {
      // Disabling - mark intro as completed to prevent auto-start
      setHasCompletedIntro(true);
    }
    toggleAssistMode();
    // saveSettings will be triggered by useEffect
  };

  /**
   * Enable Assist Mode and persist
   */
  const enableAssist = () => {
    enableAssistMode();
    // saveSettings will be triggered by useEffect
  };

  /**
   * Disable Assist Mode and persist
   * Also marks intro as completed so assist mode won't auto-enable again
   */
  const disableAssist = () => {
    setHasCompletedIntro(true);
    disableAssistMode();
    // saveSettings will be triggered by useEffect
  };

  return {
    assistModeEnabled,
    hasCompletedIntro,
    isFormReady,
    enableAssist,
    disableAssist,
    toggleAssist,
    markIntroAsCompleted,
    loadSettings,
  };
}
