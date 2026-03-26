import type {
  SchemaColumnAgg,
  SchemaColumnDataTests,
  SchemaColumnDataType,
  SchemaColumnLightdash,
  SchemaModelSelectExpr,
  SchemaModelSelectExprWithAgg,
  SchemaModelSelectModelWithAgg,
} from '@shared/schema/types/model.schema';

/**
 * Column name validation pattern (from column.name.schema.json)
 * Only allows lowercase letters, numbers, and underscores
 */
export const COLUMN_NAME_PATTERN = /^([a-z]|[0-9]|_)+$/;

/**
 * Error message constants for column validation
 */
export const COLUMN_ERROR_MESSAGES = {
  NAME_REQUIRED: 'Name is required',
  NAME_INVALID_PATTERN:
    'Name must only contain lowercase letters, numbers, and underscores',
  DESCRIPTION_REQUIRED: 'Description is required',
  DATA_TYPE_REQUIRED: 'Data type is required',
  EXPRESSION_REQUIRED: 'Expression is required',
} as const;

/**
 * Validate column fields and return error messages
 */
export interface ColumnValidationErrors {
  name?: string;
  description?: string;
  dataType?: string;
  expression?: string;
  interval?: string;
}

export function validateColumnFields(data: {
  name?: string;
  description?: string;
  dataType?: string;
  expression?: string;
}): ColumnValidationErrors {
  const errors: ColumnValidationErrors = {};

  if (!data.name || !data.name.trim()) {
    errors.name = COLUMN_ERROR_MESSAGES.NAME_REQUIRED;
  } else if (!COLUMN_NAME_PATTERN.test(data.name.trim())) {
    // Validate column name pattern: only lowercase letters, numbers, and underscores
    errors.name = COLUMN_ERROR_MESSAGES.NAME_INVALID_PATTERN;
  }

  if (!data.description || !data.description.trim()) {
    errors.description = COLUMN_ERROR_MESSAGES.DESCRIPTION_REQUIRED;
  }

  // Skip dataType validation for datetime columns (SchemaModelSelectInterval)
  // datetime columns don't have a data_type field
  if (data.name !== 'datetime') {
    if (!data.dataType || !data.dataType.trim()) {
      errors.dataType = COLUMN_ERROR_MESSAGES.DATA_TYPE_REQUIRED;
    }
  }

  return errors;
}

/**
 * Check if there are any validation errors
 */
export function hasValidationErrors(errors: ColumnValidationErrors): boolean {
  return Object.keys(errors).length > 0;
}

/**
 * Build a SchemaModelSelectExpr object for dimension columns
 */
export function buildDimensionColumn(data: {
  name: string;
  expr: string;
  data_type?: string;
  description?: string;
  exclude_from_group_by?: boolean;
  lightdash?: SchemaColumnLightdash;
  data_tests?: SchemaColumnDataTests;
}): SchemaModelSelectExpr {
  return {
    name: data.name,
    expr: data.expr,
    type: 'dim',
    ...(data.data_type && {
      data_type: data.data_type as SchemaColumnDataType,
    }),
    ...(data.description && { description: data.description }),
    ...(data.exclude_from_group_by !== undefined && {
      exclude_from_group_by: data.exclude_from_group_by,
    }),
    ...(data.lightdash && { lightdash: data.lightdash }),
    ...(data.data_tests && { data_tests: data.data_tests }),
  };
}

/**
 * Build a SchemaModelSelectExpr object for fact columns without aggregation
 */
export function buildFactColumn(data: {
  name: string;
  expr: string;
  data_type?: string;
  description?: string;
  lightdash?: SchemaColumnLightdash;
  data_tests?: SchemaColumnDataTests;
}): SchemaModelSelectExpr {
  return {
    name: data.name,
    expr: data.expr,
    type: 'fct',
    ...(data.data_type && {
      data_type: data.data_type as SchemaColumnDataType,
    }),
    ...(data.description && { description: data.description }),
    ...(data.lightdash && { lightdash: data.lightdash }),
    ...(data.data_tests && { data_tests: data.data_tests }),
  };
}

/**
 * Build a SchemaModelSelectExprWithAgg object for fact columns with aggregation
 */
export function buildFactColumnWithAgg(data: {
  name: string;
  expr: string;
  agg?: SchemaColumnAgg;
  aggs?: SchemaColumnAgg[];
  data_type?: string;
  description?: string;
  override_suffix_agg?: boolean;
  lightdash?: SchemaColumnLightdash;
  data_tests?: SchemaColumnDataTests;
}): SchemaModelSelectExprWithAgg {
  const result: SchemaModelSelectExprWithAgg = {
    name: data.name,
    expr: data.expr,
    type: 'fct',
    ...(data.data_type && {
      data_type: data.data_type as SchemaColumnDataType,
    }),
    ...(data.description && { description: data.description }),
    ...(data.lightdash && { lightdash: data.lightdash }),
    ...(data.data_tests && { data_tests: data.data_tests }),
  };

  if (data.agg) {
    result.agg = data.agg;
  }

  if (data.aggs && data.aggs.length > 0) {
    result.aggs = [data.aggs[0], ...data.aggs.slice(1)] as [
      SchemaColumnAgg,
      ...SchemaColumnAgg[],
    ];
  }

  if (data.override_suffix_agg !== undefined) {
    result.override_suffix_agg = data.override_suffix_agg;
  }

  return result;
}

/**
 * Build a SchemaModelSelectModelWithAgg object for fact columns that reference
 * an existing column from another model with aggregation.
 * This is used for int_select_model and int_lookback_model types.
 */
export function buildFactColumnWithModelRef(data: {
  name: string;
  model: string;
  agg?: SchemaColumnAgg;
  aggs?: SchemaColumnAgg[];
  data_type?: string;
  description?: string;
  override_prefix?: string;
  override_suffix_agg?: boolean;
  lightdash?: SchemaColumnLightdash;
  data_tests?: SchemaColumnDataTests;
}): SchemaModelSelectModelWithAgg {
  const result: SchemaModelSelectModelWithAgg = {
    name: data.name,
    model: data.model,
    type: 'fct',
    ...(data.data_type && {
      data_type: data.data_type as SchemaColumnDataType,
    }),
    ...(data.description && { description: data.description }),
    ...(data.override_prefix && { override_prefix: data.override_prefix }),
    ...(data.lightdash && { lightdash: data.lightdash }),
    ...(data.data_tests && { data_tests: data.data_tests }),
  };

  if (data.agg) {
    result.agg = data.agg;
  }

  if (data.aggs && data.aggs.length > 0) {
    result.aggs = [data.aggs[0], ...data.aggs.slice(1)] as [
      SchemaColumnAgg,
      ...SchemaColumnAgg[],
    ];
  }

  if (data.override_suffix_agg !== undefined) {
    result.override_suffix_agg = data.override_suffix_agg;
  }

  return result;
}

/**
 * Determine if a column should use aggregation based on its configuration
 */
export function shouldUseAggregation(
  columnType: 'dim' | 'fct',
  agg?: SchemaColumnAgg,
  aggs?: SchemaColumnAgg[],
): boolean {
  if (columnType !== 'fct') return false;
  return !!(agg || (aggs && aggs.length > 0));
}

/**
 * Build the appropriate select column based on type, aggregation, and model reference
 */
export function buildSelectColumn(data: {
  name: string;
  type: 'dim' | 'fct';
  expr: string;
  model?: string;
  data_type?: string;
  description?: string;
  exclude_from_group_by?: boolean;
  agg?: SchemaColumnAgg;
  aggs?: SchemaColumnAgg[];
  override_prefix?: string;
  override_suffix_agg?: boolean;
  lightdash?: SchemaColumnLightdash;
  data_tests?: SchemaColumnDataTests;
}):
  | SchemaModelSelectExpr
  | SchemaModelSelectExprWithAgg
  | SchemaModelSelectModelWithAgg {
  const { type, agg, aggs, model, ...rest } = data;

  if (type === 'dim') {
    return buildDimensionColumn({
      ...rest,
      exclude_from_group_by: data.exclude_from_group_by,
    });
  }

  // Fact column with model reference (SchemaModelSelectModelWithAgg)
  // Used for int_select_model and int_lookback_model when referencing existing columns
  if (model && shouldUseAggregation(type, agg, aggs)) {
    return buildFactColumnWithModelRef({
      name: data.name,
      model,
      agg,
      aggs,
      data_type: data.data_type,
      description: data.description,
      override_prefix: data.override_prefix,
      override_suffix_agg: data.override_suffix_agg,
      lightdash: data.lightdash,
      data_tests: data.data_tests,
    });
  }

  // Fact column with expression and aggregation (SchemaModelSelectExprWithAgg)
  if (shouldUseAggregation(type, agg, aggs)) {
    return buildFactColumnWithAgg({
      ...rest,
      agg,
      aggs,
      override_suffix_agg: data.override_suffix_agg,
    });
  }

  return buildFactColumn(rest);
}
