import {
  Dialog,
  DialogBackdrop,
  DialogPanel,
  DialogTitle,
} from '@headlessui/react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { InformationCircleIcon } from '@heroicons/react/24/outline';
import { Button, Checkbox, InputText, SelectSingle } from '@web/elements';
import type { LightdashMetricWithId } from '@web/stores/useModelStore';
import React, { useCallback, useEffect, useMemo, useState } from 'react';

interface AddLightdashMetricProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metric: LightdashMetricWithId) => void;
  editingMetric?: LightdashMetricWithId | null;
}

const METRIC_TYPE_OPTIONS = [
  { label: 'Average', value: 'average' },
  { label: 'Boolean', value: 'boolean' },
  { label: 'Count', value: 'count' },
  { label: 'Count Distinct', value: 'count_distinct' },
  { label: 'Date', value: 'date' },
  { label: 'Max', value: 'max' },
  { label: 'Median', value: 'median' },
  { label: 'Min', value: 'min' },
  { label: 'Number', value: 'number' },
  { label: 'Percentile', value: 'percentile' },
  { label: 'String', value: 'string' },
  { label: 'Sum', value: 'sum' },
];

const COMPACT_OPTIONS = [
  { label: 'None', value: '' },
  { label: 'Thousands', value: 'thousands' },
  { label: 'Millions', value: 'millions' },
  { label: 'Billions', value: 'billions' },
  { label: 'Trillions', value: 'trillions' },
];

const FORMAT_OPTIONS = [
  { label: 'None', value: '' },
  { label: 'EUR (€)', value: 'eur' },
  { label: 'GBP (£)', value: 'gbp' },
  { label: 'ID', value: 'id' },
  { label: 'Percent (%)', value: 'percent' },
  { label: 'USD ($)', value: 'usd' },
];

// Field configuration based on lightdash.metric.schema.json
interface FieldConfig {
  key: keyof LightdashMetricWithId;
  label: string;
  type: 'string' | 'select' | 'number' | 'boolean' | 'textarea' | 'array';
  required?: boolean;
  options?: { label: string; value: string }[];
  description?: string;
}

const FIELD_CONFIGS: FieldConfig[] = [
  {
    key: 'name',
    label: 'Name',
    type: 'string',
    required: true,
    description: 'The name of the metric',
  },
  {
    key: 'type',
    label: 'Type',
    type: 'select',
    required: true,
    options: METRIC_TYPE_OPTIONS,
    description: 'The type of metric',
  },
  {
    key: 'label',
    label: 'Label',
    type: 'string',
    description: 'The label that will be applied to the column in lightdash',
  },
  {
    key: 'group_label',
    label: 'Group Label',
    type: 'string',
    description:
      'The group label that will be applied to the column in lightdash',
  },
  {
    key: 'description',
    label: 'Description',
    type: 'textarea',
    description:
      'The description that will be applied to the column in lightdash',
  },
  {
    key: 'sql',
    label: 'SQL',
    type: 'textarea',
    description: 'The custom sql expression to generate the metric',
  },

  {
    key: 'groups',
    label: 'Groups',
    type: 'array',
    description: 'Array of group names',
  },
  {
    key: 'hidden',
    label: 'Hidden',
    type: 'boolean',
    description:
      'The hidden status that will be applied to the column in lightdash',
  },
  {
    key: 'compact',
    label: 'Compact',
    type: 'select',
    options: COMPACT_OPTIONS,
    description:
      'The compact status that will be applied to the column in lightdash',
  },
  {
    key: 'round',
    label: 'Round',
    type: 'number',
    description: 'Number of decimal places to round to (minimum 0)',
  },
  {
    key: 'format',
    label: 'Format',
    type: 'select',
    options: FORMAT_OPTIONS,
    description: 'The format to apply to the metric',
  },
  {
    key: 'ai_hint',
    label: 'AI Hint',
    type: 'textarea',
    description: 'AI hint for the metric (can be string or array of strings)',
  },
];

// Form data type that allows arrays to be strings during editing
type MetricFormData = Omit<
  Partial<LightdashMetricWithId>,
  'groups' | 'ai_hint'
> & {
  groups?: string | string[];
  ai_hint?: string | string[];
};

export const AddLightdashMetric: React.FC<AddLightdashMetricProps> = ({
  isOpen,
  onClose,
  onSave,
  editingMetric,
}) => {
  const [formData, setFormData] = useState<MetricFormData>({
    name: '',
    type: 'count',
    label: '',
    description: '',
    sql: '',
    group_label: '',
    groups: '',
    hidden: false,
    compact: undefined,
    round: undefined,
    format: undefined,
    ai_hint: '',
  });

  const [visibleFields, setVisibleFields] = useState<
    Set<keyof LightdashMetricWithId>
  >(
    new Set(['name', 'type', 'description', 'sql', 'label', 'group_label']), // Default visible fields
  );

  const nonRemovableFields = useMemo<Set<keyof LightdashMetricWithId>>(
    () =>
      new Set(['name', 'type', 'description', 'sql', 'label', 'group_label']),
    [],
  );
  const [errors, setErrors] = useState<Record<string, string>>({});

  useEffect(() => {
    if (isOpen) {
      if (editingMetric) {
        // Convert arrays to comma-separated strings for display in input fields
        const processedFormData = {
          ...editingMetric,
          compact: editingMetric.compact || undefined,
          format: editingMetric.format || undefined,
          groups: Array.isArray(editingMetric.groups)
            ? editingMetric.groups.join(', ')
            : editingMetric.groups || '',
          ai_hint: Array.isArray(editingMetric.ai_hint)
            ? editingMetric.ai_hint.join(', ')
            : editingMetric.ai_hint || '',
        };
        setFormData(processedFormData);
        // Show fields that have values in the editing metric
        const fieldsWithValues = new Set<keyof LightdashMetricWithId>([
          'name',
          'type',
        ]);
        Object.entries(editingMetric).forEach(([key, value]) => {
          if (
            value !== undefined &&
            value !== null &&
            value !== '' &&
            (!Array.isArray(value) || value.length > 0)
          ) {
            fieldsWithValues.add(key as keyof LightdashMetricWithId);
          }
        });
        setVisibleFields(fieldsWithValues);
      } else {
        setFormData({
          name: '',
          type: 'count',
          label: '',
          description: '',
          sql: '',
          group_label: '',
          groups: '',
          hidden: false,
          compact: undefined,
          round: undefined,
          format: undefined,
          ai_hint: '',
        });
        setVisibleFields(
          new Set([
            'name',
            'type',
            'description',
            'sql',
            'label',
            'group_label',
          ]),
        );
      }
      setErrors({});
    }
  }, [isOpen, editingMetric]);

  const handleFieldVisibilityChange = useCallback(
    (fieldKey: keyof LightdashMetricWithId, visible: boolean) => {
      setVisibleFields((prev) => {
        const newSet = new Set(prev);
        if (visible) {
          newSet.add(fieldKey);
        } else {
          // Don't allow hiding non-removable fields
          if (!nonRemovableFields.has(fieldKey)) {
            newSet.delete(fieldKey);
            // Clear the field value when hiding
            setFormData((prevData) => ({
              ...prevData,
              [fieldKey]:
                FIELD_CONFIGS.find((f) => f.key === fieldKey)?.type ===
                'boolean'
                  ? false
                  : FIELD_CONFIGS.find((f) => f.key === fieldKey)?.type ===
                      'array'
                    ? ''
                    : undefined,
            }));
          }
        }
        return newSet;
      });
    },
    [nonRemovableFields],
  );

  const validateForm = useCallback((): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name?.trim()) {
      newErrors.name = 'Name is required';
    }

    if (!formData.type) {
      newErrors.type = 'Type is required';
    }

    if (
      formData.round !== undefined &&
      (formData.round < 0 || !Number.isInteger(formData.round))
    ) {
      newErrors.round = 'Round must be a non-negative integer';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  }, [formData]);

  const handleSave = useCallback(() => {
    if (!validateForm()) {
      return;
    }

    // Helper to get trimmed string value or undefined
    const getStringValue = (value: unknown): string | undefined => {
      if (typeof value === 'string') {
        const trimmed = value.trim();
        return trimmed.length > 0 ? trimmed : undefined;
      }
      return undefined;
    };

    // Process comma-separated strings into arrays
    const processGroups = (value: unknown): string[] => {
      if (Array.isArray(value)) {
        return value;
      }
      if (typeof value === 'string' && value.trim()) {
        return value
          .split(',')
          .map((v) => v.trim())
          .filter((v) => v);
      }
      return [];
    };

    const processAiHint = (value: unknown): string | string[] | undefined => {
      if (Array.isArray(value)) {
        return value;
      }
      if (typeof value === 'string') {
        if (value.includes(',')) {
          // Convert to array if it contains commas
          return value
            .split(',')
            .map((v) => v.trim())
            .filter((v) => v);
        }
        // Keep as string if no commas
        return value.trim() || undefined;
      }
      return undefined;
    };

    // Build the metric object with explicit value handling
    const metric: LightdashMetricWithId = {
      id: editingMetric?.id || `metric-${Date.now()}`,
      name: formData.name!.trim(),
      type: formData.type as LightdashMetricWithId['type'],
      label: getStringValue(formData.label),
      description: getStringValue(formData.description),
      sql: getStringValue(formData.sql),
      group_label: getStringValue(formData.group_label),
      hidden: formData.hidden || false,
      compact: formData.compact || undefined,
      round: formData.round ?? undefined,
      format: formData.format || undefined,
      groups: processGroups(formData.groups),
      ai_hint: processAiHint(formData.ai_hint),
    };

    onSave(metric);
    onClose();
  }, [formData, editingMetric, validateForm, onSave, onClose]);

  const handleInputChange = useCallback(
    (field: keyof LightdashMetricWithId, value: unknown) => {
      // Don't process comma-separated strings immediately - keep as string during typing
      // They will be converted to arrays when saving
      setFormData((prev) => ({
        ...prev,
        [field]: value,
      }));

      // Clear error when user starts typing
      if (errors[field]) {
        setErrors((prev) => ({
          ...prev,
          [field]: '',
        }));
      }
    },
    [errors],
  );

  // Helper function to render field input based on type
  const renderFieldInput = useCallback(
    (fieldConfig: FieldConfig) => {
      const { key, type, options } = fieldConfig;
      const value = formData[key];
      const error = errors[key];

      switch (type) {
        case 'string':
          return (
            <InputText
              value={(value as string) || ''}
              onChange={(e) => handleInputChange(key, e.target.value)}
              placeholder={`Enter ${fieldConfig.label.toLowerCase()}`}
              className={`w-full ${error ? 'border-red-500' : ''}`}
            />
          );

        case 'textarea':
          return (
            <textarea
              value={(value as string) || ''}
              onChange={(e) => handleInputChange(key, e.target.value)}
              placeholder={`Enter ${fieldConfig.label.toLowerCase()}`}
              rows={key === 'sql' ? 4 : 3}
              className={`w-full px-3 py-2 border border-gray-300 text-background-contrast bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none ${key === 'sql' ? 'font-mono text-sm' : ''} ${error ? 'border-red-500' : ''}`}
            />
          );

        case 'select':
          return (
            <SelectSingle
              value={
                value
                  ? {
                      label:
                        options?.find((opt) => opt.value === value)?.label ||
                        '',
                      value: value as string,
                    }
                  : null
              }
              onChange={(option) =>
                handleInputChange(key, option?.value || undefined)
              }
              onBlur={() => {}}
              options={options || []}
              placeholder={`Select ${fieldConfig.label.toLowerCase()}`}
              className={error ? 'border-red-500' : ''}
            />
          );

        case 'number':
          return (
            <InputText
              type="number"
              value={(value as number)?.toString() || ''}
              onChange={(e) => {
                const numValue = e.target.value;
                handleInputChange(
                  key,
                  numValue ? parseInt(numValue) : undefined,
                );
              }}
              placeholder="0"
              min="0"
              className={`w-full ${error ? 'border-red-500' : ''}`}
            />
          );

        case 'boolean':
          return (
            <Checkbox
              checked={(value as boolean) || false}
              onChange={(checked) => handleInputChange(key, checked)}
            />
          );

        case 'array':
          return (
            <InputText
              value={(value as string) || ''}
              onChange={(e) => handleInputChange(key, e.target.value)}
              placeholder={`Enter ${fieldConfig.label.toLowerCase()} (comma-separated)`}
              className={`w-full ${error ? 'border-red-500' : ''}`}
            />
          );

        default:
          return null;
      }
    },
    [formData, errors, handleInputChange],
  );

  if (!isOpen) {
    return null;
  }

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      <DialogBackdrop className="fixed inset-0 bg-black/30" />

      <div className="fixed inset-0 flex w-screen items-center justify-center p-4 z-[100000]">
        <DialogPanel className="max-w-2xl w-full bg-background rounded-lg shadow-xl max-h-[90vh] overflow-y-auto react-flow__node-scrollable ">
          {/* Header */}
          <div className="flex items-center justify-between p-2 border-b border-gray-200">
            <DialogTitle className="text-xl font-semibold text-background-contrast">
              {editingMetric ? 'Edit Metric' : 'Add Metric'}
            </DialogTitle>
            <Button
              className="p-1 hover:bg-surface rounded-full transition-colors"
              icon={
                <XMarkIcon className="w-6 h-6 text-background-contrast opacity-70" />
              }
              onClick={(e) => {
                e.stopPropagation();
                onClose();
              }}
              variant="iconButton"
              label=""
            ></Button>
          </div>

          <div className="grid grid-cols-4 items-center gap-2 px-6 pt-4 text-sm text-gray-600">
            {FIELD_CONFIGS.filter(
              (fieldConfig) => !nonRemovableFields.has(fieldConfig.key),
            ).map((fieldConfig) => (
              <label key={fieldConfig.key} className="flex items-start gap-3">
                <Checkbox
                  checked={visibleFields.has(fieldConfig.key)}
                  onChange={(checked) => {
                    const isChecked =
                      typeof checked === 'boolean'
                        ? checked
                        : checked.target.checked;
                    handleFieldVisibilityChange(fieldConfig.key, isChecked);
                  }}
                />
                <div className="flex items-center min-w-0">
                  <div className="text-sm font-medium text-foreground">
                    {fieldConfig.label}
                  </div>
                  <div title={fieldConfig.description || ''}>
                    <InformationCircleIcon className="w-4 h-4 text-gray-400 ml-1 cursor-pointer" />
                  </div>
                </div>
              </label>
            ))}
          </div>

          {/* Form Content */}
          <div className="p-4">
            <div className="space-y-4 max-h-96 overflow-y-auto p-1 react-flow__node-scrollable">
              {FIELD_CONFIGS.filter((config) =>
                visibleFields.has(config.key),
              ).map((fieldConfig) => (
                <div key={fieldConfig.key}>
                  <label className="block text-sm font-medium text-background-contrast mb-1">
                    {fieldConfig.label}
                    {fieldConfig.required && (
                      <span className="text-red-500 ml-1">*</span>
                    )}
                  </label>

                  {fieldConfig.type === 'boolean' ? (
                    <div className="flex items-center gap-2">
                      {renderFieldInput(fieldConfig)}
                      <span className="text-sm text-foreground">
                        {fieldConfig.label}
                      </span>
                    </div>
                  ) : (
                    renderFieldInput(fieldConfig)
                  )}

                  {errors[fieldConfig.key] && (
                    <p className="text-red-500 text-sm mt-1">
                      {errors[fieldConfig.key]}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 p-2 border-t border-gray-200">
            <Button
              className="px-8 py-2 text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-md transition-colors"
              onClick={(e) => {
                e.stopPropagation();
                onClose();
              }}
              variant="link"
              label="Cancel"
            ></Button>
            <Button
              onClick={(e) => {
                e.stopPropagation();
                handleSave();
              }}
              variant="link"
              label={editingMetric ? 'Update' : 'Add'}
              className="px-8 py-2 text-white bg-primary hover:bg-primary-700 rounded-md transition-colors"
            ></Button>
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  );
};
