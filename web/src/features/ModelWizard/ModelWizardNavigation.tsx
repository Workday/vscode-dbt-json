import { ChevronLeftIcon } from '@heroicons/react/24/outline';
import { Button, Stepper } from '@web/elements';

export interface ModelWizardNavigationProps {
  steps: Array<{ id: string; label: string; tooltip?: string }>;
  currentStep: number;
  completedSteps?: number[];
  onPrevious: () => void;
  onNext: () => void;
  onSubmit: () => void;
  isFirstStep: boolean;
  isLastStep: boolean;
  canProceed: boolean;
  submitLabel?: string;
  onStepClick?: (stepIndex: number) => void;
}

export function ModelWizardNavigation({
  steps,
  currentStep,
  completedSteps = [],
  onPrevious,
  onNext,
  onSubmit,
  isFirstStep,
  isLastStep,
  canProceed,
  submitLabel = 'Create Model',
  onStepClick,
}: ModelWizardNavigationProps) {
  return (
    <div className="sticky top-0 z-10 bg-background border-b border-surface px-4 py-2">
      {/* Desktop Layout - Only for large screens (1024px+) */}
      <div className="hidden lg:flex items-center justify-between">
        <Button
          label={isFirstStep ? 'Close' : 'Back'}
          variant="iconButton"
          icon={<ChevronLeftIcon className="h-5 w-5" />}
          type="button"
          onClick={onPrevious}
          disabled={false}
          className="flex-shrink-0"
        />

        <div className="mx-4 flex justify-center">
          <div className="w-full">
            <Stepper
              steps={steps}
              currentStep={currentStep}
              completedSteps={completedSteps}
              onStepClick={onStepClick}
            />
          </div>
        </div>

        {isLastStep ? (
          <div data-tutorial-id="submit-button">
            <Button
              label={submitLabel}
              variant="primary"
              type="button"
              onClick={onSubmit}
              disabled={!canProceed}
              className="flex-shrink-0"
            />
          </div>
        ) : (
          <Button
            label="Next"
            variant="primary"
            type="button"
            onClick={onNext}
            disabled={!canProceed}
            className="flex-shrink-0"
          />
        )}
      </div>

      {/* Mobile/Tablet Layout - Up to large screens (< 1024px) */}
      <div className="flex flex-col gap-3 lg:hidden">
        <div className="space-y-2">
          <div className="flex justify-between items-center">
            <div className="text-sm text-primary font-medium">
              Step {currentStep + 1} of {steps.length}
            </div>
            <div className="text-base font-semibold text-background-contrast">
              {steps[currentStep]?.label}
            </div>
          </div>

          <div className="w-full bg-surface rounded-full h-2">
            <div
              className="bg-primary h-2 rounded-full transition-all duration-300 ease-in-out"
              style={{ width: `${((currentStep + 1) / steps.length) * 100}%` }}
            />
          </div>
        </div>

        {/* Mobile Navigation Buttons */}
        <div className="flex justify-between items-center gap-3">
          <Button
            label={isFirstStep ? 'Close' : 'Back'}
            variant="iconButton"
            icon={<ChevronLeftIcon className="h-5 w-5" />}
            type="button"
            onClick={onPrevious}
            disabled={false}
            className="flex-shrink-0"
          />

          {isLastStep ? (
            <Button
              label={submitLabel}
              variant="primary"
              type="button"
              onClick={onSubmit}
              disabled={!canProceed}
              className="flex-1 max-w-xs"
            />
          ) : (
            <Button
              label="Next"
              variant="primary"
              type="button"
              onClick={onNext}
              disabled={!canProceed}
              className="flex-1 max-w-xs"
            />
          )}
        </div>
      </div>
    </div>
  );
}
