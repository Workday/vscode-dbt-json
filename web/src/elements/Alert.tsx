import { CheckCircleIcon, XCircleIcon } from '@heroicons/react/20/solid';
import { makeClassName } from '@web';
import { useMemo } from 'react';

export type AlertProps = {
  description?: string;
  label?: string;
  variant?: 'success' | 'error' | 'warning' | 'info';
};

export function Alert({ description, label, variant = 'success' }: AlertProps) {
  const icon = useMemo(() => {
    switch (variant) {
      case 'error': {
        return (
          <XCircleIcon
            aria-hidden="true"
            className={makeClassName('h-5 w-5', `text-${variant}`)}
          />
        );
      }
      default: {
        return (
          <CheckCircleIcon
            aria-hidden="true"
            className={makeClassName('h-5 w-5', `text-${variant}`)}
          />
        );
      }
    }
  }, [variant]);

  return (
    <div className={makeClassName('p-4 rounded-md', `bg-${variant}`)}>
      <div className="flex">
        <div className="flex-shrink-0 mr-2 mt-2">{icon}</div>
        <div
          className={makeClassName('mt-2 text-sm', `text-${variant}-contrast`)}
        >
          <h3
            className={makeClassName(
              'text-sm font-medium',
              `text-${variant}-contrast`,
            )}
          >
            {label}
          </h3>
          {description?.split('\n').map((l, i) => <p key={i}>{l}</p>)}
        </div>
      </div>
    </div>
  );
}
