import { clsx } from 'clsx';
import { useEffect, useMemo } from 'react';
import { usePrevious } from 'react-use';
import { twMerge } from 'tailwind-merge';

export function makeClassName(
  ...classNames: (
    | React.ComponentProps<'div'>['className']
    | false
    | undefined
  )[]
) {
  return twMerge(clsx(...classNames));
}

export function useChange(value: unknown, callback: () => void) {
  const previousValue = usePrevious(value);
  const shouldTrigger = useMemo(
    () => previousValue !== value,
    [previousValue, value],
  );
  useEffect(() => {
    if (shouldTrigger) {
      callback();
    }
  }, [shouldTrigger]);
}

export function useTrigger(value: unknown, callback: () => void) {
  const previousValue = usePrevious(value);
  const shouldTrigger = useMemo(
    () => value && !previousValue,
    [previousValue, value],
  );
  useEffect(() => {
    if (shouldTrigger) {
      callback();
    }
  }, [shouldTrigger]);
}
