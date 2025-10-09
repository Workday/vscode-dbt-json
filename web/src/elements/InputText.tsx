import { Description, Field, Input, Label } from '@headlessui/react';
import { makeClassName } from '@web';
import { Tooltip } from '@web/elements';

export type InputTextProps = React.ComponentProps<'input'> & {
  description?: string;
  error?: boolean | string;
  innerRef?: React.Ref<HTMLInputElement>;
  label?: string;
  tooltipText?: string;
};

export function InputText({
  description,
  error,
  innerRef,
  label,
  value = '',
  tooltipText,
  ...props
}: InputTextProps) {
  return (
    <Field className="w-full">
      {label && (
        <Label className="block text-sm/6 leading-6 mt-2 text-background-contrast flex gap-1 items-center">
          {label}
          {tooltipText && <Tooltip content={tooltipText} />}
        </Label>
      )}
      {!tooltipText && description && (
        <Description className="text-sm/6">{description}</Description>
      )}
      <Input
        {...props}
        className={makeClassName(
          'block bg-background ring-1 ring-background-contrast rounded-lg px-3 py-1 mt-3 text-sm text-background-contrast w-full',
          error ? 'ring-2 ring-error' : 'focus:outline-2',
        )}
        ref={innerRef}
        value={value}
      />
      {error && typeof error === 'string' && (
        <p className="inline-block text-error text-xs italic">{error}</p>
      )}
    </Field>
  );
}
