import { CheckIcon } from '@heroicons/react/24/solid';
import { makeClassName } from '@web';

import { Tooltip } from './Tooltip';

export type StepperStep = {
  id: string;
  label: string;
  tooltip?: string;
};

export type StepperProps = {
  steps: StepperStep[];
  currentStep: number;
  completedSteps?: number[];
  className?: string;
  onStepClick?: (stepIndex: number) => void;
};

export function Stepper({
  steps,
  currentStep,
  completedSteps = [],
  className,
  onStepClick,
}: StepperProps) {
  return (
    <div className={makeClassName('flex items-center w-full', className)}>
      {steps.map((step, index) => {
        const isCompleted = completedSteps.includes(index);
        const isCurrent = index === currentStep;
        const isClickable = onStepClick !== undefined;

        return (
          <div key={step.id} className="flex items-center flex-1">
            <Tooltip content={step.tooltip} variant="solid">
              <button
                type="button"
                onClick={() => isClickable && onStepClick(index)}
                disabled={!isClickable}
                className={makeClassName(
                  'flex gap-2 items-center transition-all',
                  isClickable && 'cursor-pointer hover:opacity-80',
                  !isClickable && 'cursor-default',
                )}
              >
                <div
                  className={makeClassName(
                    'flex items-center justify-center w-8 h-8 rounded-full text-sm font-medium transition-all',
                    isCompleted
                      ? 'bg-green-500 text-white'
                      : isCurrent
                        ? 'bg-primary text-white'
                        : 'bg-gray-300 text-gray-600',
                  )}
                >
                  {isCompleted ? (
                    <CheckIcon className="w-5 h-5" />
                  ) : (
                    <span>{index + 1}</span>
                  )}
                </div>

                {step.tooltip ? (
                  <span
                    className={makeClassName(
                      'w-max text-sm font-medium text-center',
                      isCompleted || isCurrent
                        ? 'text-background-contrast'
                        : 'text-gray-500',
                    )}
                  >
                    {step.label}
                  </span>
                ) : (
                  <span
                    className={makeClassName(
                      'w-max text-sm font-medium text-center',
                      isCompleted || isCurrent
                        ? 'text-background-contrast'
                        : 'text-gray-500',
                    )}
                  >
                    {step.label}
                  </span>
                )}
              </button>
            </Tooltip>
            {index < steps.length - 1 && (
              <div className="flex-1 mx-4">
                <div
                  className={makeClassName(
                    'h-0.5 w-[50px]',
                    isCompleted
                      ? 'bg-green-500'
                      : index < currentStep
                        ? 'bg-primary'
                        : 'bg-gray-500',
                  )}
                />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
