import { SelectSingle } from '@web/elements';
import { forwardRef, useMemo } from 'react';
import type { ControllerRenderProps, FieldError } from 'react-hook-form';

export type FieldSelectSingleProps = ControllerRenderProps & {
  error?: FieldError;
  label?: string;
  options: { label: string; value: string }[];
  tooltipText?: string;
  className?: string;
  wrapperClass?: string;
  inputClassName?: string;
  labelClass?: string;
  helpIcon?: React.ReactNode; // Help icon for Assist Me
};

export const FieldSelectSingle = forwardRef<
  HTMLSelectElement,
  FieldSelectSingleProps
>(({ error, onChange, options, value, ...props }, ref) => {
  const selected = useMemo(
    () => options.find((o) => o.value === value) || null,
    [options, value],
  );
  return (
    <>
      <SelectSingle
        {...props}
        error={error ? error?.message || true : undefined}
        innerRef={ref}
        tooltipText={props.tooltipText}
        onChange={(o) => onChange(o?.value || null)}
        options={options}
        value={selected}
      />
    </>
  );
});
