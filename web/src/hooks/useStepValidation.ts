import { useCallback } from 'react';
import type {
  FieldValues,
  UseFormGetFieldState,
  UseFormTrigger,
} from 'react-hook-form';

interface FieldConfig {
  name: string;
  label: string;
}

interface StepValidationConfig {
  fields: FieldConfig[];
}

export function useStepValidation<T extends FieldValues = FieldValues>(
  trigger: UseFormTrigger<T>,
  getFieldState: UseFormGetFieldState<T>,
) {
  const validateStep = useCallback(
    async (config: StepValidationConfig): Promise<string[]> => {
      const fieldNames = config.fields.map((f) => f.name);

      const isValid = await trigger(fieldNames as any);

      if (!isValid) {
        const errorMessages: string[] = [];

        config.fields.forEach((field) => {
          const fieldState = getFieldState(field.name as any);
          if (fieldState.error) {
            errorMessages.push(`${fieldState.error.message}`);
          }
        });

        return errorMessages;
      }

      return [];
    },
    [trigger, getFieldState],
  );

  return { validateStep };
}
