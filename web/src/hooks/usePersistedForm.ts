// web/src/hooks/usePersistedForm.ts
import { useEffect, useState } from 'react';
import type { UseFormProps, UseFormReturn } from 'react-hook-form';
import { useForm } from 'react-hook-form';
import { stateSync } from '../utils/stateSync';
import { useDebounce } from './useDebounce';

interface UsePersistedFormOptions<T extends Record<string, unknown>>
  extends UseFormProps<T> {
  formType: string;
  autoSave?: boolean;
  debounceMs?: number;
}

export function usePersistedForm<T extends Record<string, unknown>>({
  formType,
  autoSave = true,
  debounceMs = 500,
  ...formOptions
}: UsePersistedFormOptions<T>): UseFormReturn<T> & { isLoading: boolean } {
  const form = useForm<T>(formOptions);
  const [isLoading] = useState(false); // Start with false to show form immediately

  // Watch all form values for auto-save
  const watchedValues = form.watch();
  const debouncedValues = useDebounce(watchedValues, debounceMs);

  // Load initial state on mount
  useEffect(() => {
    const loadInitialState = async () => {
      try {
        const savedState = await stateSync.loadState(formType);

        if (savedState && Object.keys(savedState).length > 0) {
          // Reset form with saved values
          form.reset(savedState as T);
        }
      } catch (error) {
        console.error('[usePersistedForm] Failed to load form state:', error);
      }
    };

    // Load state in background
    void loadInitialState();
  }, [formType]);

  // Auto-save when form values change
  useEffect(() => {
    if (!autoSave) {
      return;
    }

    if (!form.formState.isDirty) {
      return;
    }

    // Don't save empty forms
    if (!debouncedValues || Object.keys(debouncedValues).length === 0) {
      return;
    }

    try {
      void stateSync.saveState(formType, debouncedValues);
    } catch (error) {
      console.error('[usePersistedForm] Failed to save form state:', error);
    }
  }, [debouncedValues, autoSave, form.formState.isDirty, formType]);

  return {
    ...form,
    isLoading,
  };
}
