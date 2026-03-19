import type {
  AdditionalFieldsSchema,
  ModelingStateType,
} from '@web/stores/useModelStore';
import type { TutorialStore } from '@web/stores/useTutorialStore';
import type { DriveStep } from 'driver.js';

export type ModelType = 'select' | 'join' | 'union' | 'rollup' | 'lookback';
export type ModelSource = 'staging' | 'intermediate' | 'marts';

export interface TutorialConfig {
  source: ModelSource;
  type: ModelType;
}

export interface TutorialStep extends DriveStep {
  // Extended properties for our use case
  id?: string; // Unique identifier for the step
  requiresWizardAdvance?: boolean; // Should we move to next wizard step?
  waitForUserAction?: boolean; // Wait for user to interact before proceeding
  wizardStep?: number; // Which wizard step this belongs to (0-3)
  canSkip?: boolean; // Whether this step can be skipped
  isInformational?: boolean; // Is this just info (no user action needed)?
  isCompleted?: boolean; // Has the user already completed this field?
  nodeType?: string; // React Flow node type to navigate to in Data Modeling
  zoomLevel?: number; // Custom zoom level for React Flow navigation (default: 1, lower = zoomed out)
  defaultTabIndex?: number; // Default tab index for components with tabs (0=first, 1=second, etc.)

  // NEW: Step lifecycle hooks for UI simulation
  onStepEnter?: (tutorialStore: TutorialStore) => void; // Execute when step starts
  onStepExit?: (tutorialStore: TutorialStore) => void; // Execute when step ends
}

export interface TutorialState {
  isActive: boolean;
  hasSeenTutorial: boolean;
  currentStepIndex: number;
  mode: TutorialMode | null;
}

export type TutorialMode = 'select' | 'join' | 'union' | 'rollup' | 'lookback';

// Demo data structure for Play Tutorial
// Extends ModelingStateType to include all modeling fields and additional fields
export type DemoData = Partial<ModelingStateType> &
  Partial<AdditionalFieldsSchema>;
