/**
 * Lineage types and utilities
 *
 * Provides shared types and functions for both column and model lineage.
 *
 * @example
 * // Import column lineage types
 * import { ColumnLineageDAG, ColumnLineageNode } from '@shared/lineage';
 *
 * // Import common types
 * import { ModelLayerType, getModelLayer } from '@shared/lineage';
 *
 * // Import serialization helpers
 * import { serializeColumnLineageDAG, deserializeColumnLineageDAG } from '@shared/lineage';
 *
 * // Import parsing constants
 * import { SQL_KEYWORDS, JINJA_KEYWORDS, MACRO_PATTERN } from '@shared/lineage';
 */

// Common types
export type { ModelLayerType } from './types';
export { getModelLayer, MODEL_LAYER_PREFIXES } from './types';

// Parsing constants
export {
  AGG_TYPES,
  type AggType,
  FUNCTION_CALL_PATTERN,
  IDENTIFIER_PATTERN,
  JINJA_KEYWORDS,
  LITERAL_PATTERN,
  MACRO_PATTERN,
  QUALIFIED_COLUMN_PATTERN,
  SIMPLE_COLUMN_PATTERN,
  SQL_KEYWORDS,
  STRING_LITERAL_PATTERN,
} from './constants';

// Column lineage types
export type {
  ColumnLineageDAG,
  ColumnLineageDAGSerialized,
  ColumnLineageEdge,
  ColumnLineageNode,
  ColumnLineageResult,
  ColumnNodeId,
  ColumnTransformationType,
} from './columnTypes';
export {
  deserializeColumnLineageDAG,
  serializeColumnLineageDAG,
} from './columnTypes';

// CSV export utilities
export type { ColumnLineageCsvRow } from './columnCsvExport';
export { generateAllColumnsCSV, generateLineageCSV } from './columnCsvExport';
