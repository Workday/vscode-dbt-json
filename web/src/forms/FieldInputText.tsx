import { InputText } from '@web/elements';
import { forwardRef } from 'react';
import type { ControllerRenderProps, FieldError } from 'react-hook-form';

export type FieldInputTextProps = ControllerRenderProps & {
  error?: FieldError;
  label: string;
  tooltipText?: string;
  inputClassName?: string;
  labelClassName?: string;
  helpIcon?: React.ReactNode; // Help icon for Assist Me
};

export const FieldInputText = forwardRef<HTMLInputElement, FieldInputTextProps>(
  ({ error, ...props }, ref) => {
    return (
      <InputText
        {...props}
        error={error ? error?.message || true : undefined}
        innerRef={ref}
        tooltipText={props.tooltipText}
      />
    );
  },
);
