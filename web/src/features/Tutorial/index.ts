// Hooks
export { useAssistMode } from './hooks/useAssistMode';
export { useAssistTutorial } from './hooks/useAssistTutorial';
export { useOnDemandGuide } from './hooks/useOnDemandGuide';
export { usePlayTutorial } from './hooks/usePlayTutorial';

// Components
export { TutorialSelector } from './components/TutorialSelector';

// Types
export type { DemoData, TutorialMode, TutorialState } from './types';

// Constants
export { TUTORIAL_STORAGE_KEY } from './config/constants';

// Tutorial Store (re-export for convenience)
export type { TutorialStore } from '@web/stores/useTutorialStore';
export {
  // Enums
  ActionType,
  TutorialComponentState,
  useActiveColumnConfigTab,
  // Assist Mode selectors
  useAssistModeEnabled,
  useCurrentAssistStepIndex,
  // Enum-based selectors
  useCurrentComponentState,
  useCurrentStepIndex,
  useHasCompletedIntro,
  useIsAssistTutorialActive,
  // Form Ready selectors
  useIsFormReady,
  useIsPlayTutorialActive,
  useTutorialColumnData,
  useTutorialMode,
  useTutorialStore,
} from '@web/stores/useTutorialStore';
