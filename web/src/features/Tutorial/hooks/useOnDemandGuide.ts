import 'driver.js/dist/driver.css';

import { useTutorialStore } from '@web/stores/useTutorialStore';
import {
  type AllowedButtons,
  type Driver,
  driver,
  type DriveStep,
} from 'driver.js';
import { useEffect, useRef } from 'react';

import {
  DataModelingGuideStep,
  getGuideForStep,
  type ModelState,
} from '../config/assistMe/assistSteps';

// Driver.js config for on-demand guides
const ON_DEMAND_GUIDE_CONFIG = {
  showProgress: false,
  animate: true,
  overlayColor: 'rgba(0, 0, 0, 0.5)',
  smoothScroll: true,
  allowClose: true,
  overlayClickNext: false,
  disableActiveInteraction: false,
  showButtons: ['close'] as AllowedButtons[],
  popoverClass: 'driverjs-theme',
  progressText: '',
  nextBtnText: 'Next',
  prevBtnText: 'Previous',
  doneBtnText: 'Done',
};

interface UseOnDemandGuideProps {
  modelState: ModelState;
  onWizardStepChange?: (step: number) => void;
}

/**
 * Hook for showing on-demand guides based on user interactions
 * Watches for changes in currentDataModelingGuideStep and displays relevant guide
 */
export function useOnDemandGuide({ modelState }: UseOnDemandGuideProps) {
  const driverInstance = useRef<Driver | null>(null);

  const {
    currentDataModelingGuideStep,
    assistModeEnabled,
    currentAssistStepIndex,
    setTutorialNavigationNodeType,
  } = useTutorialStore((state) => ({
    currentDataModelingGuideStep: state.currentDataModelingGuideStep,
    assistModeEnabled: state.assistModeEnabled,
    currentAssistStepIndex: state.currentAssistStepIndex,
    setTutorialNavigationNodeType: state.setTutorialNavigationNodeType,
  }));

  // Watch for guide step changes
  useEffect(() => {
    // If guide step is NONE, clear any existing guide
    if (currentDataModelingGuideStep === DataModelingGuideStep.NONE) {
      if (driverInstance.current) {
        driverInstance.current.destroy();
        driverInstance.current = null;
      }
      return;
    }

    // Check if this is an action-specific guide (always show these)
    const isActionGuide =
      currentDataModelingGuideStep === DataModelingGuideStep.ACTION_LIGHTDASH ||
      currentDataModelingGuideStep === DataModelingGuideStep.ACTION_GROUPBY ||
      currentDataModelingGuideStep === DataModelingGuideStep.ACTION_WHERE;

    // Only show guides when:
    // 1. Assist mode is enabled, OR
    // 2. Sequential assist tutorial is running (currentAssistStepIndex > 0), OR
    // 3. This is an action-specific guide (user explicitly clicked an action)
    const shouldShowGuide =
      assistModeEnabled || currentAssistStepIndex > 0 || isActionGuide;

    if (!shouldShowGuide) {
      // Clean up any existing guide
      if (driverInstance.current) {
        driverInstance.current.destroy();
        driverInstance.current = null;
      }
      return;
    }

    // Get the guide definition for current step
    const guideRule = getGuideForStep(currentDataModelingGuideStep, modelState);

    if (!guideRule) {
      console.warn(
        `[useOnDemandGuide] No guide rule found for step: ${currentDataModelingGuideStep}`,
      );
      return;
    }

    // Destroy previous guide before showing new one
    if (driverInstance.current) {
      driverInstance.current.destroy();
      driverInstance.current = null;
    }

    // Navigate to the node if nodeType and zoomLevel are specified
    if (guideRule.nodeType && guideRule.zoomLevel !== undefined) {
      setTutorialNavigationNodeType(guideRule.nodeType, guideRule.zoomLevel);
    } else if (guideRule.nodeType) {
      setTutorialNavigationNodeType(guideRule.nodeType, 1); // default zoom
    }

    // Call onStepEnter if defined
    if (guideRule.onStepEnter) {
      guideRule.onStepEnter(useTutorialStore.getState());
    }

    // Wait for navigation/zoom to complete before showing guide
    setTimeout(() => {
      // Check if element exists
      const element = document.querySelector(guideRule.element);
      if (!element) {
        console.warn(
          `[useOnDemandGuide] Element not found: ${guideRule.element}`,
        );
        return;
      }

      // Create the drive step
      const driveStep: DriveStep = {
        element: guideRule.element,
        popover: {
          title: guideRule.popover.title,
          description: guideRule.popover.description,
          side: guideRule.popover.side || 'right',
          align: guideRule.popover.align || 'start',
        },
      };

      // Create new driver instance with single step
      driverInstance.current = driver({
        ...ON_DEMAND_GUIDE_CONFIG,
        steps: [driveStep],
        onDestroyed: () => {
          // Call onStepExit if defined
          if (guideRule.onStepExit) {
            guideRule.onStepExit(useTutorialStore.getState());
          }
          // Clear the guide step so it doesn't re-trigger
          useTutorialStore.getState().clearDataModelingGuideStep();
        },
      });

      // Show the guide
      driverInstance.current.drive();
    }, 800); // Wait for zoom/navigation animation

    // Cleanup on unmount or when guide changes
    return () => {
      if (driverInstance.current) {
        driverInstance.current.destroy();
        driverInstance.current = null;
      }
    };
  }, [
    currentDataModelingGuideStep,
    assistModeEnabled,
    currentAssistStepIndex,
    modelState,
    modelState.isLightdashActive,
    modelState.isGroupByActive,
    modelState.isWhereActive,
    modelState.fromModel,
    modelState.fromSource,
    setTutorialNavigationNodeType,
  ]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (driverInstance.current) {
        driverInstance.current.destroy();
        driverInstance.current = null;
      }
    };
  }, []);

  return {
    // Return any methods if needed in the future
  };
}
