import { makeClassName } from '@web';
import React from 'react';

export interface RadioOption {
  value: string;
  label: string;
  disabled?: boolean;
}

interface RadioGroupProps {
  options: RadioOption[];
  value: string;
  onChange: (value: string) => void;
  name: string;
  className?: string;
  variant?: 'standard' | 'button-group';
}

export const RadioGroup: React.FC<RadioGroupProps> = ({
  options,
  value,
  onChange,
  name,
  className = '',
  variant = 'standard',
}) => {
  return (
    <div
      className={makeClassName(
        'flex items-center relative',
        className,
        variant === 'button-group' &&
          'bg-surface text-background-contrast rounded-md',
      )}
    >
      {options.map((option) => (
        <div
          key={option.value}
          className={makeClassName('flex items-center flex-grow')}
        >
          <input
            type="radio"
            id={`${name}-${option.value}`}
            name={name}
            disabled={option.disabled}
            value={option.value}
            checked={value === option.value}
            onChange={(e) => onChange(e.target.value)}
            className={makeClassName(
              'w-4 h-4 text-blue-600 bg-background border-0 transition-all duration-200 ease-in-out focus:ring-blue-500',
              variant === 'button-group' && 'sr-only',
            )}
          />
          <label
            htmlFor={`${name}-${option.value}`}
            className={makeClassName(
              'text-sm font-regular cursor-pointer transition-colors duration-200 ease-in-out',
              variant === 'standard' && 'ml-2',
              variant === 'standard' && value === option.value
                ? 'text-blue-600'
                : 'text-foreground',
              variant === 'button-group' && 'px-4 py-2 w-full text-center',
              variant === 'button-group' &&
                value === option.value &&
                'bg-primary text-primary-contrast rounded-md',
            )}
          >
            {option.label}
          </label>
        </div>
      ))}
    </div>
  );
};
