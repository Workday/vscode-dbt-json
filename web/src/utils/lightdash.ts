import type {
  SchemaLightdashDimension,
  SchemaLightdashMetricMerge,
} from '@shared/schema/types/model.schema';
import {
  LIGHTDASH_COMPACT_OPTIONS,
  LIGHTDASH_DIMENSION_TYPES,
  LIGHTDASH_FORMAT_OPTIONS,
  LIGHTDASH_TIME_INTERVALS,
} from '@web/features/DataModeling/types';

export interface DimensionField {
  key: keyof SchemaLightdashDimension;
  label: string;
  type:
    | 'string'
    | 'select'
    | 'number'
    | 'boolean'
    | 'textarea'
    | 'array'
    | 'multiselect';
  options?: { label: string; value: string }[];
  defaultVisible: boolean;
}

export interface MetricMergeField {
  key: keyof SchemaLightdashMetricMerge;
  label: string;
  type: 'string' | 'select' | 'number' | 'boolean' | 'textarea' | 'array';
  options?: { label: string; value: string }[];
  defaultVisible: boolean;
}

/**
 * Configuration for Lightdash Dimension fields
 */
export const dimensionFields: DimensionField[] = [
  {
    key: 'group_label',
    label: 'Group Label',
    type: 'string',
    defaultVisible: true,
  },
  {
    key: 'label',
    label: 'Label',
    type: 'string',
    defaultVisible: true,
  },
  {
    key: 'type',
    label: 'Type',
    type: 'select',
    options: LIGHTDASH_DIMENSION_TYPES as unknown as {
      label: string;
      value: string;
    }[],
    defaultVisible: false,
  },
  {
    key: 'ai_hint',
    label: 'AI Hint',
    type: 'textarea',
    defaultVisible: true,
  },
  {
    key: 'groups',
    label: 'Groups',
    type: 'array',
    defaultVisible: false,
  },
  {
    key: 'hidden',
    label: 'Hidden',
    type: 'boolean',
    defaultVisible: false,
  },
  {
    key: 'round',
    label: 'Round',
    type: 'number',
    defaultVisible: false,
  },
  {
    key: 'format',
    label: 'Format',
    type: 'select',
    options: LIGHTDASH_FORMAT_OPTIONS as unknown as {
      label: string;
      value: string;
    }[],
    defaultVisible: false,
  },
  {
    key: 'sql',
    label: 'SQL',
    type: 'textarea',
    defaultVisible: true,
  },
  {
    key: 'time_intervals',
    label: 'Time Intervals',
    type: 'multiselect',
    options: LIGHTDASH_TIME_INTERVALS as unknown as {
      label: string;
      value: string;
    }[],
    defaultVisible: false,
  },
];

/**
 * Configuration for Lightdash Metrics Merge fields
 */
export const metricsMergeFields: MetricMergeField[] = [
  {
    key: 'description',
    label: 'Description',
    type: 'textarea',
    defaultVisible: true,
  },
  {
    key: 'compact',
    label: 'Compact',
    type: 'select',
    options: LIGHTDASH_COMPACT_OPTIONS as unknown as {
      label: string;
      value: string;
    }[],
    defaultVisible: true,
  },
  {
    key: 'format',
    label: 'Format',
    type: 'select',
    options: LIGHTDASH_FORMAT_OPTIONS as unknown as {
      label: string;
      value: string;
    }[],
    defaultVisible: true,
  },
  {
    key: 'round',
    label: 'Round',
    type: 'number',
    defaultVisible: true,
  },
  {
    key: 'group_label',
    label: 'Group Label',
    type: 'string',
    defaultVisible: false,
  },
  {
    key: 'groups',
    label: 'Groups',
    type: 'array',
    defaultVisible: false,
  },
  {
    key: 'hidden',
    label: 'Hidden',
    type: 'boolean',
    defaultVisible: false,
  },
  {
    key: 'sql',
    label: 'SQL',
    type: 'textarea',
    defaultVisible: false,
  },
];
