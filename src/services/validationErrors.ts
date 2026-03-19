import type { ValidateFunction } from 'ajv';
import type { ErrorObject } from 'ajv';

/**
 * Maps model types to their specific schema file names
 */
const MODEL_TYPE_SCHEMA_MAP: Record<string, string> = {
  stg_select_source: 'model.type.stg_select_source.schema.json',
  stg_select_model: 'model.type.stg_select_model.schema.json',
  stg_union_sources: 'model.type.stg_union_sources.schema.json',
  int_select_model: 'model.type.int_select_model.schema.json',
  int_join_column: 'model.type.int_join_column.schema.json',
  int_join_models: 'model.type.int_join_models.schema.json',
  int_lookback_model: 'model.type.int_lookback_model.schema.json',
  int_rollup_model: 'model.type.int_rollup_model.schema.json',
  int_union_models: 'model.type.int_union_models.schema.json',
  mart_select_model: 'model.type.mart_select_model.schema.json',
  mart_join_models: 'model.type.mart_join_models.schema.json',
};

/**
 * Gets the specific schema validator for a given model type
 */
export function getValidatorForType(
  ajv: any,
  type: string,
): ValidateFunction | null {
  const schemaId = MODEL_TYPE_SCHEMA_MAP[type];
  if (!schemaId) {
    return null;
  }

  try {
    return ajv.getSchema(schemaId);
  } catch {
    return null;
  }
}

/**
 * Formats validation errors into human-readable messages
 * @param errors - Array of AJV validation errors
 * @param context - Either 'model' or 'source'
 * @param type - For models: the model type (required). For sources: undefined
 */
export function formatValidationErrors(
  errors: ErrorObject[] | null | undefined,
  context: 'model' | 'source',
  type?: string,
): string[] {
  if (!errors || errors.length === 0) {
    return [];
  }

  const messages: string[] = [];

  // Group errors by type for better presentation
  const requiredErrors: ErrorObject[] = [];
  const additionalPropErrors: ErrorObject[] = [];
  const typeErrors: ErrorObject[] = [];
  const otherErrors: ErrorObject[] = [];

  for (const error of errors) {
    if (error.keyword === 'required') {
      requiredErrors.push(error);
    } else if (error.keyword === 'additionalProperties') {
      additionalPropErrors.push(error);
    } else if (error.keyword === 'type' || error.keyword === 'const') {
      typeErrors.push(error);
    } else {
      otherErrors.push(error);
    }
  }

  // Add header based on context
  if (context === 'model') {
    if (type) {
      messages.push(`Validation errors for model type "${type}":\n`);
    } else {
      messages.push(`Validation errors for model:\n`);
    }
  } else if (context === 'source') {
    messages.push(`Validation errors for source:\n`);
  }

  // Format required field errors
  if (requiredErrors.length > 0) {
    const missingFields = requiredErrors
      .map((e) => e.params?.missingProperty)
      .filter(Boolean);
    if (missingFields.length > 0) {
      messages.push(`Missing required fields: ${missingFields.join(', ')}`);
    }
  }

  // Format additional properties errors
  if (additionalPropErrors.length > 0) {
    const extraFields = additionalPropErrors
      .map((e) => e.params?.additionalProperty)
      .filter(Boolean);
    if (extraFields.length > 0) {
      messages.push(`Invalid fields: ${extraFields.join(', ')}`);
      if (context === 'model' && type) {
        messages.push(
          `   These fields are not allowed for "${type}" models. Please remove them or change the model type.`,
        );
      } else {
        messages.push(
          `   These fields are not allowed. Please remove them or check the schema.`,
        );
      }
    }
  }

  // Format type errors
  for (const error of typeErrors) {
    messages.push(`${formatSingleError(error)}`);
  }

  // Format other errors
  for (const error of otherErrors) {
    messages.push(`${formatSingleError(error)}`);
  }

  return messages;
}

/**
 * Validates CTE-specific constraints that cannot be expressed in JSON Schema alone:
 * - Unique CTE names within the model
 * - No forward references (a CTE can only reference CTEs defined earlier)
 * - No circular dependencies
 * - CTE from.cte must reference a valid CTE name
 * - Main model from.cte must reference a valid CTE name
 */
export function validateCtes(modelJson: any): string[] {
  const errors: string[] = [];
  if (!modelJson?.ctes || !Array.isArray(modelJson.ctes)) {
    return errors;
  }

  const cteNames = new Set<string>();
  const cteOrder: string[] = [];

  for (let i = 0; i < modelJson.ctes.length; i++) {
    const cte = modelJson.ctes[i];
    const cteName = cte.name;

    // Unique name check
    if (cteNames.has(cteName)) {
      errors.push(
        `ctes[${i}]: duplicate CTE name "${cteName}". CTE names must be unique within the model.`,
      );
    }

    // Forward reference check
    if (cte.from) {
      if ('cte' in cte.from && cte.from.cte) {
        const refName = cte.from.cte;
        if (!cteNames.has(refName)) {
          errors.push(
            `ctes[${i}] ("${cteName}"): references CTE "${refName}" which is not defined earlier in the array. CTEs can only reference previously defined CTEs.`,
          );
        }
      }
      if ('union' in cte.from && cte.from.union?.ctes) {
        for (const unionCte of cte.from.union.ctes) {
          if (!cteNames.has(unionCte)) {
            errors.push(
              `ctes[${i}] ("${cteName}"): union references CTE "${unionCte}" which is not defined earlier in the array.`,
            );
          }
        }
      }
      if ('join' in cte.from && Array.isArray(cte.from.join)) {
        for (const j of cte.from.join) {
          if ('cte' in j && j.cte && !cteNames.has(j.cte)) {
            errors.push(
              `ctes[${i}] ("${cteName}"): join references CTE "${j.cte}" which is not defined earlier in the array.`,
            );
          }
        }
      }
    }

    // Check select references
    if (cte.select) {
      for (const sel of cte.select) {
        if (typeof sel === 'object' && 'cte' in sel && sel.cte) {
          if (!cteNames.has(sel.cte)) {
            errors.push(
              `ctes[${i}] ("${cteName}"): select references CTE "${sel.cte}" which is not defined earlier in the array.`,
            );
          }
        }
      }
    }

    // WHERE on a union CTE is silently ignored in SQL generation because
    // SQL requires each UNION branch to have its own WHERE clause.
    if (cte.where && cte.from && 'union' in cte.from) {
      errors.push(
        `ctes[${i}] ("${cteName}"): "where" is not supported on union CTEs because it cannot be applied across UNION ALL branches. Apply filters to the individual CTEs before the union instead.`,
      );
    }

    cteNames.add(cteName);
    cteOrder.push(cteName);
  }

  // Validate main model from.cte reference
  if (modelJson.from && 'cte' in modelJson.from && modelJson.from.cte) {
    if (!cteNames.has(modelJson.from.cte)) {
      errors.push(
        `from.cte: references CTE "${modelJson.from.cte}" which is not defined in the ctes array.`,
      );
    }
  }

  // Validate main model from.join CTE references
  if (
    modelJson.from &&
    'join' in modelJson.from &&
    Array.isArray(modelJson.from.join)
  ) {
    for (const j of modelJson.from.join) {
      if ('cte' in j && j.cte && !cteNames.has(j.cte)) {
        errors.push(
          `from.join: references CTE "${j.cte}" which is not defined in the ctes array.`,
        );
      }
    }
  }

  // Validate main model select CTE references
  if (modelJson.select) {
    for (const sel of modelJson.select) {
      if (typeof sel === 'object' && 'cte' in sel && sel.cte) {
        if (!cteNames.has(sel.cte)) {
          errors.push(
            `select: references CTE "${sel.cte}" which is not defined in the ctes array.`,
          );
        }
      }
    }
  }

  return errors;
}

/**
 * Formats a single error object into a human-readable message
 */
function formatSingleError(error: ErrorObject): string {
  const path = error.instancePath ?? 'root';
  const field = path.replace(/^\//, '').replace(/\//g, '.') || 'model';

  switch (error.keyword) {
    case 'required':
      return `${field}: missing required property "${error.params?.missingProperty}"`;
    case 'additionalProperties':
      return `${field}: unexpected property "${error.params?.additionalProperty}"`;
    case 'type':
      return `${field}: should be ${error.params?.type}`;
    case 'const':
      return `${field}: must be "${error.params?.allowedValue}"`;
    case 'enum':
      return `${field}: must be one of: ${error.params?.allowedValues?.join(', ')}`;
    case 'minItems':
      return `${field}: must have at least ${error.params?.limit} items`;
    case 'maxItems':
      return `${field}: must have at most ${error.params?.limit} items`;
    case 'minimum':
      return `${field}: must be >= ${error.params?.limit}`;
    case 'maximum':
      return `${field}: must be <= ${error.params?.limit}`;
    case 'pattern':
      return `${field}: must match pattern ${error.params?.pattern}`;
    default:
      return `${field}: ${error.message || 'validation failed'}`;
  }
}
