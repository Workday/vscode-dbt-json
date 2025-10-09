import {
  FrameworkColumnAgg,
  FrameworkModel,
  FrameworkPartitionName,
} from './types';

export const FRAMEWORK_AGGS: FrameworkColumnAgg[] = [
  'count',
  'hll',
  'max',
  'min',
  'sum',
  'tdigest',
];

export const FRAMEWORK_MODEL_TYPE_OPTIONS: {
  label: string;
  value: FrameworkModel['type'];
}[] = [
  { label: 'Staging Select Source', value: 'stg_select_source' },
  { label: 'Staging Union Sources', value: 'stg_union_sources' },
  { label: 'Staging Select Model', value: 'stg_select_model' },
  { label: 'Intermediate Select Model', value: 'int_select_model' },
  { label: 'Intermediate Rollup Model', value: 'int_rollup_model' },
  { label: 'Intermediate Union Models', value: 'int_union_models' },
  { label: 'Intermediate Join Models', value: 'int_join_models' },
  { label: 'Intermediate Join Column', value: 'int_join_column' },
  { label: 'Intermediate Lookback Model', value: 'int_lookback_model' },
  { label: 'Mart Select Model', value: 'mart_select_model' },
  { label: 'Mart Join Models', value: 'mart_join_models' },
];

export const FRAMEWORK_PARTITIONS: FrameworkPartitionName[] = [
  'portal_partition_monthly',
  'portal_partition_daily',
  'portal_partition_hourly',
];

/**
 * Common exclude patterns for dbt_project.yml file
 */
export const FRAMEWORK_JSON_SYNC_EXCLUDE_PATHS = [
  '**/dist/**',
  '**/node_modules/**',
  '**/dbt_packages/**',
  '**/.venv/**',
  '**/.env/**',
  '**/venv/**',
  '**/env/**',
];
