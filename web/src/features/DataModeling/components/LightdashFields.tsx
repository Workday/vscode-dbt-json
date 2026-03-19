import { Checkbox, InputText, SelectMulti, SelectSingle } from '@web/elements';

import type {
  DimensionField,
  MetricMergeField,
} from '../../../utils/lightdash';

interface LightdashFieldsProps<T> {
  field: DimensionField | MetricMergeField;
  value: unknown;
  onChange: (key: keyof T, value: unknown) => void;
}

/**
 * Generic component for rendering Lightdash field inputs based on field type
 */
export function LightdashFields<T>({
  field,
  value,
  onChange,
}: LightdashFieldsProps<T>) {
  switch (field.type) {
    case 'string':
      return (
        <InputText
          value={(value as string) || ''}
          onChange={(e) => onChange(field.key as keyof T, e.target.value)}
          placeholder={`Enter ${field.label.toLowerCase()}`}
          className="w-full"
        />
      );

    case 'textarea': {
      // Display value: convert array to string if needed
      const displayValue = Array.isArray(value)
        ? value.join(', ')
        : (value as string) || '';

      return (
        <textarea
          value={displayValue}
          onChange={(e) => onChange(field.key as keyof T, e.target.value)}
          onBlur={(e) => {
            const inputValue = e.target.value.trim();
            // For ai_hint field, convert comma-separated values to array
            if (field.key === 'ai_hint' && inputValue.includes(',')) {
              const arrayValue = inputValue
                .split(',')
                .map((v) => v.trim())
                .filter((v) => v);
              onChange(field.key as keyof T, arrayValue);
            } else {
              // Keep as string if no comma or not ai_hint
              onChange(field.key as keyof T, inputValue);
            }
          }}
          placeholder={`Enter ${field.label.toLowerCase()}`}
          rows={3}
          className="w-full px-3 py-2 border border-input rounded-md bg-background text-foreground focus:outline-none focus:ring-2 focus:ring-primary resize-none"
        />
      );
    }

    case 'select':
      return (
        <SelectSingle
          value={
            value
              ? {
                  label:
                    field.options?.find((opt) => opt.value === value)?.label ||
                    '',
                  value: value as string,
                }
              : null
          }
          onChange={(option) =>
            onChange(field.key as keyof T, option?.value || undefined)
          }
          onBlur={() => {}}
          options={field.options || []}
          placeholder={`Select ${field.label.toLowerCase()}`}
        />
      );

    case 'multiselect':
      return (
        <SelectMulti
          options={field.options || []}
          value={Array.isArray(value) ? (value as string[]) : []}
          onChange={(selectedValues) =>
            onChange(field.key as keyof T, selectedValues)
          }
          placeholder={`Select ${field.label.toLowerCase()}`}
        />
      );

    case 'number':
      return (
        <InputText
          type="number"
          value={(value as number)?.toString() || ''}
          onChange={(e) => {
            const numValue = e.target.value;
            onChange(
              field.key as keyof T,
              numValue ? parseInt(numValue) : undefined,
            );
          }}
          placeholder="0"
          min="0"
          className="w-full"
          inputClassName="[&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
        />
      );

    case 'boolean':
      return (
        <Checkbox
          checked={(value as boolean) || false}
          onChange={(checked) => onChange(field.key as keyof T, checked)}
        />
      );

    case 'array':
      return (
        <InputText
          value={
            Array.isArray(value) ? value.join(', ') : (value as string) || ''
          }
          onChange={(e) => {
            const inputValue = e.target.value;
            const arrayValue = inputValue
              .split(',')
              .map((v) => v.trim())
              .filter((v) => v);
            onChange(field.key as keyof T, arrayValue);
          }}
          placeholder={`Enter ${field.label.toLowerCase()} (comma-separated)`}
          className="w-full"
        />
      );

    default:
      return null;
  }
}
