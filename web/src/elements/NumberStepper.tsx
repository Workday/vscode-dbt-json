import { makeClassName } from '@web';

interface NumberStepperProps {
  /** Label displayed next to the stepper */
  label: string;
  /** Current value */
  value: number;
  /** Called when value changes */
  onChange: (value: number) => void;
  /** Minimum allowed value */
  min?: number;
  /** Maximum allowed value */
  max?: number;
  /** Whether the stepper is disabled */
  disabled?: boolean;
  /** Additional CSS classes */
  className?: string;
}

/**
 * Number stepper with +/- buttons for incrementing/decrementing a value.
 */
export function NumberStepper({
  label,
  value,
  onChange,
  min = 1,
  max,
  disabled,
  className,
}: NumberStepperProps) {
  const decrement = () => onChange(Math.max(min, value - 1));
  const increment = () =>
    onChange(max !== undefined ? Math.min(max, value + 1) : value + 1);

  return (
    <div
      className={makeClassName(
        'flex items-center justify-between gap-3',
        className,
      )}
    >
      <span className="text-sm text-surface-contrast">{label}</span>
      <div className="flex items-center">
        <button
          type="button"
          onClick={decrement}
          disabled={disabled || value <= min}
          className="w-8 h-8 flex items-center justify-center bg-surface border border-neutral rounded-l text-surface-contrast hover:bg-neutral disabled:opacity-50 disabled:cursor-not-allowed"
        >
          −
        </button>
        <div className="w-10 h-8 flex items-center justify-center bg-surface border-t border-b border-neutral text-sm text-surface-contrast">
          {value}
        </div>
        <button
          type="button"
          onClick={increment}
          disabled={disabled || (max !== undefined && value >= max)}
          className="w-8 h-8 flex items-center justify-center bg-surface border border-neutral rounded-r text-surface-contrast hover:bg-neutral disabled:opacity-50 disabled:cursor-not-allowed"
        >
          +
        </button>
      </div>
    </div>
  );
}
