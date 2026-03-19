import { useEffect, useRef, useState } from 'react';
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
  const [isLoading] = useState(false);
  const [initialStateLoaded, setInitialStateLoaded] = useState(false);
  const lastSavedValues = useRef<string>('{}');

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
          // Update the reference to prevent immediate save
          lastSavedValues.current = JSON.stringify(savedState);
        }
      } catch (error) {
        console.error('[usePersistedForm] Failed to load form state:', error);
      } finally {
        setInitialStateLoaded(true);
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

    if (!initialStateLoaded) {
      return;
    }

    // Only save if form is dirty and has changes.
    if (!form.formState.isDirty) {
      return;
    }

    if (!debouncedValues || Object.keys(debouncedValues).length === 0) {
      return;
    }

    // Prevent saving if values haven't actually changed
    const hasRealChanges = Object.values(debouncedValues).some(
      (value) => value !== undefined && value !== null && value !== '',
    );

    if (!hasRealChanges) {
      return;
    }

    // Check if values have actually changed from last save
    const currentValuesString = JSON.stringify(debouncedValues);
    if (currentValuesString === lastSavedValues.current) {
      return;
    }

    try {
      void stateSync.saveState(formType, debouncedValues);
      lastSavedValues.current = currentValuesString;
    } catch (error) {
      console.error('[usePersistedForm] Failed to save form state:', error);
    }
  }, [debouncedValues, autoSave, formType, initialStateLoaded]);

  return {
    ...form,
    isLoading,
  };
}
