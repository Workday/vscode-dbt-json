import { makeClassName } from '@web';
import React, { useState } from 'react';
import type { FieldError } from 'react-hook-form';

import { Button } from './Button';
import Tooltip from './Tooltip';

export interface ButtonGroupProps {
  label?: string;
  tooltipText?: string;
  options: string[];
  initialValue: string;
  onSelect?: (value: string) => void;
  error?: FieldError;
  disabled?: boolean;
  className?: string;
}

const stopPropagation = (
  e: React.MouseEvent | React.MouseEvent<HTMLDivElement>,
) => {
  e.stopPropagation();
};

export function ButtonGroup({
  label,
  tooltipText,
  initialValue = '',
  options = [],
  onSelect,
  error,
  disabled = false,
  className = '',
}: ButtonGroupProps) {
  const [selectedOption, setSelectedOption] = useState<string>(initialValue);

  // Update selectedOption when initialValue changes
  React.useEffect(() => {
    setSelectedOption(initialValue);
  }, [initialValue]);

  const handleOptionClick = (option: string) => {
    if (disabled) return;
    setSelectedOption(option);
    if (onSelect) {
      onSelect(option);
    }
  };

  const hasError = !!error;

  // Computed class names
  const errorClasses = hasError ? 'ring-2 ring-error border-error' : '';
  const selectedButtonClasses = 'bg-primary text-primary-contrast shadow';
  const unselectedButtonClasses = 'bg-surface text-background-contrast';

  // Helper function to render a button with responsive styling
  const renderButton = (option: string, index: number) => {
    const isSelected = selectedOption === option;

    const baseClasses =
      'py-3 px-4 w-full text-left font-medium transition-all duration-200';
    const borderRadiusClasses =
      index === 0
        ? 'rounded-t-lg'
        : index === options.length - 1
          ? 'rounded-b-lg'
          : 'rounded-none';

    // Responsive overrides for horizontal layout on lg screens and up
    const responsiveClasses =
      'lg:py-2 lg:rounded-full lg:flex-shrink-0 lg:w-auto lg:text-center';

    const buttonClassName = `${baseClasses} ${borderRadiusClasses} ${responsiveClasses} ${disabled ? 'opacity-80 cursor-not-allowed' : ''} ${
      isSelected ? selectedButtonClasses : unselectedButtonClasses
    }`;

    return (
      <Button
        key={option}
        variant="link"
        disabled={disabled}
        onClick={() => handleOptionClick(option)}
        className={buttonClassName}
        label={option}
      />
    );
  };

  return (
    <div className={makeClassName('flex flex-col gap-2', className)}>
      {label && (
        <div className="text-sm/6 font-semibold leading-6 text-background-contrast flex gap-1 items-center">
          {label}
          {tooltipText && <Tooltip content={tooltipText} />}
        </div>
      )}
      <div
        className={`flex flex-col bg-surface shadow-sm rounded-lg divide-y divide-background-contrast/20 w-full max-w-xs lg:flex-row lg:flex-wrap lg:gap-2 lg:rounded-full lg:w-max lg:max-w-full lg:divide-y-0 ${errorClasses} ${disabled ? 'opacity-95 cursor-not-allowed' : ''}`}
        onMouseDown={stopPropagation}
        onClick={stopPropagation}
      >
        {options.map((option, index) => renderButton(option, index))}
      </div>

      {/* Error message */}
      {error && (
        <p className="inline-block text-error text-xs italic">
          {error.message}
        </p>
      )}
    </div>
  );
}
