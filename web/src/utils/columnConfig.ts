/**
 * Column Fields Configuration
 *
 * This configuration defines which fields are supported in the UI
 * based on the current model type.
 */

export type ModelType =
  | 'stg_select_source'
  | 'stg_select_model'
  | 'stg_union_sources'
  | 'int_select_model'
  | 'int_join_column'
  | 'int_join_models'
  | 'int_lookback_model'
  | 'int_rollup_model'
  | 'int_union_models'
  | 'mart_select_model'
  | 'mart_join_models';

export interface ColumnFieldsConfig {
  // Basic fields (AddColumnBasic & ColumnConfigGeneral)
  name: ModelType[];
  data_type: ModelType[];
  description: ModelType[];
  type: ModelType[];
  expr: ModelType[];

  // Aggregation fields (AddColumnBasic & ColumnConfigGeneral)
  agg: ModelType[];
  aggs: ModelType[];

  // Others tab fields (ColumnConfigOthers)
  exclude_from_group_by: ModelType[];
  interval: ModelType[];
  override_prefix: ModelType[];
  override_suffix_agg: ModelType[];
  data_tests: ModelType[];

  // Lightdash tab (ColumnConfigLightdash)
  lightdash: ModelType[];

  // Advanced fields
  model: ModelType[];
  source: ModelType[];
  include: ModelType[];
  exclude: ModelType[];
}

/**
 * Column fields configuration based on model types
 * Maps each field to the model types where it is supported
 */
export const COLUMN_FIELDS: ColumnFieldsConfig = {
  // Basic fields - supported in most models except int_rollup_model
  name: [
    'stg_select_source',
    'stg_select_model',
    'stg_union_sources',
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  data_type: [
    'stg_select_source',
    'stg_select_model',
    'stg_union_sources',
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  description: [
    'stg_select_source',
    'stg_select_model',
    'stg_union_sources',
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  type: [
    'stg_select_source',
    'stg_select_model',
    'stg_union_sources',
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  expr: [
    'stg_select_source',
    'stg_select_model',
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'int_rollup_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  // Aggregation fields - only for specific model types
  agg: [
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'mart_join_models',
  ],

  aggs: [
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'mart_join_models',
  ],

  // Others tab fields
  exclude_from_group_by: [
    'stg_select_source',
    'stg_select_model',
    'int_select_model',
    'int_join_models',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  interval: [
    'stg_select_source',
    'stg_select_model',
    'int_select_model',
    'int_join_models',
    'int_rollup_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  override_prefix: [
    'stg_select_model',
    'int_select_model',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
  ],

  override_suffix_agg: [
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'mart_join_models',
  ],

  data_tests: [
    'stg_select_source',
    'stg_select_model',
    'stg_union_sources',
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  // Lightdash - supported in most models
  lightdash: [
    'stg_select_source',
    'stg_select_model',
    'stg_union_sources',
    'int_select_model',
    'int_join_column',
    'int_join_models',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  // Model/Source references
  model: [
    'stg_select_model',
    'int_select_model',
    'int_join_models',
    'int_lookback_model',
    'int_rollup_model',
    'int_union_models',
    'mart_select_model',
    'mart_join_models',
  ],

  source: ['stg_select_source', 'stg_union_sources'],

  include: [
    'stg_select_source',
    'stg_select_model',
    'int_select_model',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
  ],

  exclude: [
    'stg_select_source',
    'stg_select_model',
    'int_select_model',
    'int_lookback_model',
    'int_union_models',
    'mart_select_model',
  ],
};

/**
 * Helper function to check if a field is supported for a given model type
 */
export const isFieldSupported = (
  field: keyof ColumnFieldsConfig,
  modelType: ModelType | undefined,
): boolean => {
  if (!modelType) return true; // Show all fields if model type is unknown
  return COLUMN_FIELDS[field].includes(modelType);
};

/**
 * Helper function to check if a field is supported based on both model type and column name
 * Special handling for SchemaModelSelectInterval (when name is "datetime")
 */
export const isFieldSupportedForColumn = (
  field: keyof ColumnFieldsConfig,
  modelType: ModelType | undefined,
  columnName: string | undefined,
): boolean => {
  // Special case: SchemaModelSelectInterval (datetime column)
  if (columnName === 'datetime') {
    switch (field) {
      case 'name':
        return true; // name is required and must be "datetime"
      case 'interval':
        return isFieldSupported(field, modelType); // interval is required
      case 'type':
        return true; // type is supported but can only be "dim"
      case 'description':
      case 'lightdash':
      case 'data_tests':
      case 'model':
        return isFieldSupported(field, modelType); // these are optional
      case 'expr': // expr is NOT supported for datetime columns
      case 'agg': // aggregations are NOT supported for datetime columns
      case 'aggs':
      case 'data_type': // data_type is NOT supported for datetime columns
      case 'exclude_from_group_by':
      case 'override_prefix':
      case 'override_suffix_agg':
      case 'source':
      case 'include':
      case 'exclude':
        return false;
      default:
        return isFieldSupported(field, modelType);
    }
  }

  // Default: use model-type-based validation
  return isFieldSupported(field, modelType);
};

/**
 * Helper function to get all supported fields for a given model type
 */
export const getSupportedFields = (
  modelType: ModelType | undefined,
): Partial<Record<keyof ColumnFieldsConfig, boolean>> => {
  if (!modelType) return {};

  const supportedFields: Partial<Record<keyof ColumnFieldsConfig, boolean>> =
    {};

  (Object.keys(COLUMN_FIELDS) as Array<keyof ColumnFieldsConfig>).forEach(
    (field) => {
      supportedFields[field] = COLUMN_FIELDS[field].includes(modelType);
    },
  );

  return supportedFields;
};
