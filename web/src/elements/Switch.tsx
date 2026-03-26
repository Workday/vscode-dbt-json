import { Field, Label, Switch as HeadlessSwitch } from '@headlessui/react';
import { makeClassName } from '@web';
import { Fragment } from 'react/jsx-runtime';

import { Tooltip } from './Tooltip';

interface SwitchProps {
  /** Current checked state */
  checked: boolean;
  /** Called when the switch is toggled */
  onChange: (checked: boolean | React.ChangeEvent<HTMLInputElement>) => void;
  /** Label text displayed next to the switch */
  label?: string;
  /** Additional CSS classes for the container */
  className?: string;
  /** Whether the switch is disabled */
  disabled?: boolean;
  /** Tooltip text shown on hover */
  tooltipText?: string;
  /** Position of the label relative to the switch */
  position?: 'left' | 'right';
  /** Size variant of the switch */
  size?: 'base' | 'sm';
}

const SIZE_CLASSES = {
  sm: {
    label: 'text-xs',
    track: 'h-4 w-7',
    thumb: 'size-3',
    thumbOn: 'translate-x-3',
  },
  base: {
    label: 'text-base',
    track: 'h-6 w-11',
    thumb: 'size-4',
    thumbOn: 'translate-x-6',
  },
} as const;

export const Switch: React.FC<SwitchProps> = ({
  checked,
  onChange,
  label,
  className,
  disabled,
  tooltipText,
  position = 'left',
  size = 'base',
}) => {
  const sizeClasses = SIZE_CLASSES[size];

  const hasLabelContent = label || tooltipText;

  return (
    <Field className={makeClassName('flex items-center gap-2', className)}>
      {hasLabelContent && (
        <div
          className={makeClassName(
            'flex items-center gap-2',
            position === 'left' ? 'order-first' : 'order-last',
          )}
        >
          {label && (
            <Label
              className={makeClassName(
                'font-medium select-none text-surface-contrast flex-shrink-0',
                disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer',
                sizeClasses.label,
              )}
            >
              {label}
            </Label>
          )}
          {tooltipText && <Tooltip content={tooltipText} />}
        </div>
      )}
      <HeadlessSwitch
        checked={checked}
        onChange={onChange}
        disabled={disabled}
        as={Fragment}
      >
        {({ checked, disabled }) => (
          <button
            className={makeClassName(
              'group inline-flex items-center rounded-full flex-shrink-0',
              checked ? 'bg-switch-on' : 'bg-switch-off',
              disabled && 'cursor-not-allowed opacity-50',
              position === 'left' ? 'ml-auto' : 'mr-auto',
              sizeClasses.track,
            )}
          >
            <span
              className={makeClassName(
                'rounded-full bg-background transition',
                sizeClasses.thumb,
                checked ? sizeClasses.thumbOn : 'translate-x-1',
              )}
            />
          </button>
        )}
      </HeadlessSwitch>
    </Field>
  );
};
