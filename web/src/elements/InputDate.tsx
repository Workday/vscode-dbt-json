import { Field, Input, Label } from '@headlessui/react';
import { makeClassName } from '@web';

import { Tooltip } from './Tooltip';

export type InputDateProps = React.ComponentProps<'input'> & {
  label?: string;
  error?: string;
  tooltipText?: string;
  ref?: React.Ref<HTMLInputElement>;
};

export const InputDate: React.FC<InputDateProps> = ({
  name,
  id,
  className,
  error,
  label,
  tooltipText,
  ref,
  ...props
}) => {
  return (
    <Field className="w-full">
      {label && (
        <Label
          className="text-sm/6 font-semibold leading-6 mt-2 text-background-contrast flex gap-1 items-center"
          htmlFor={id}
        >
          {label}
          {tooltipText && <Tooltip content={tooltipText} />}
        </Label>
      )}
      <Input
        type="date"
        name={name}
        id={id}
        {...props}
        className={makeClassName(
          'block bg-background ring-1 rounded-lg px-3 h-10 text-sm text-background-contrast w-full',
          'focus:ring-2 focus:ring-primary focus:outline-none',
          error ? 'ring-2 ring-error' : 'ring-[#D9D9D9] dark:ring-[#4A4A4A]',
          className,
        )}
        ref={ref}
      />
      {error && typeof error === 'string' && (
        <p className="inline-block text-error text-xs italic">{error}</p>
      )}
    </Field>
  );
};
