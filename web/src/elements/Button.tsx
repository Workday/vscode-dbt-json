import * as Headless from '@headlessui/react';
import { makeClassName } from '@web';
import { Spinner } from '@web/elements';

export type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  fullWidth?: boolean;
  label: string;
  loading?: boolean;
  type?: 'button' | 'submit' | 'reset';
  variant?: 'primary' | 'secondary' | 'error' | 'neutral' | 'iconButton';
  icon?: React.ReactNode;
};

export function Button({
  className,
  fullWidth,
  label,
  loading,
  type = 'button',
  variant,
  icon,
  ...props
}: ButtonProps) {
  const _props = {
    ...props,
    disabled: loading,
    children: loading ? <Spinner size={10} /> : label,
    type,
  };
  switch (variant) {
    case 'secondary': {
      return (
        <Headless.Button
          {..._props}
          className={makeClassName(
            'rounded bg-background px-2 py-1 text-xs font-semibold text-background-contrast shadow-sm ring-1 ring-inset ring-secondary',
            fullWidth && 'w-full',
            className,
          )}
        />
      );
    }

    case 'error': {
      return (
        <Headless.Button
          {..._props}
          className={makeClassName(
            'rounded bg-red-600 py-2 px-4 text-sm text-white hover:bg-red-700 disabled:opacity-50',
            fullWidth && 'w-full',
            className,
          )}
        />
      );
    }

    case 'neutral': {
      return (
        <Headless.Button
          {..._props}
          className={makeClassName(
            'rounded bg-gray-600 py-2 px-4 text-sm text-white hover:bg-gray-700 disabled:opacity-50',
            fullWidth && 'w-full',
            className,
          )}
        />
      );
    }

    case 'iconButton':
      return (
        <Headless.Button
          {..._props}
          className={makeClassName(
            'p-2 rounded flex gap-2 items-center justify-center hover:text-primary',
            fullWidth && 'w-full',
            className,
          )}
        >
          {icon}
          {label}
        </Headless.Button>
      );

    case 'primary':
    default:
      return (
        <Headless.Button
          {..._props}
          className={makeClassName(
            'm-3 rounded bg-primary py-2 px-4 text-sm text-primary-contrast',
            fullWidth && 'w-full',
            className,
          )}
        />
      );
  }
}
