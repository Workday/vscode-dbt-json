import type { AppError } from '@shared';
import type { ApiError } from '@shared/api/types';
import { useCallback, useState } from 'react';

export function useError(): {
  clearError: () => void;
  error: AppError | null;
  handleError: (_err: unknown, fallbackMessage?: string) => void;
} {
  const clearError = useCallback(() => setError(null), []);

  const [error, setError] = useState<AppError | null>(null);

  const handleError = useCallback((_err: unknown, fallbackMessage?: string) => {
    const err = _err as
      | ApiError
      | (Error & { details?: string[] })
      | { response: { data: Record<string, string>; message?: string } };
    setError({
      details:
        ('details' in err && Array.isArray(err.details) && err.details) ||
        ('response' in err && err?.response?.data) ||
        undefined,
      message:
        err instanceof Error || 'message' in err
          ? err.message
          : ('response' in err && err?.response?.message) ||
            fallbackMessage ||
            'Unknown Error',
    });
  }, []);

  return { error, clearError, handleError };
}
