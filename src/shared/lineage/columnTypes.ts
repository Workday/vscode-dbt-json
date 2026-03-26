/**
 * Column lineage specific types
 */

import type { ModelLayerType } from './types';

/** Transformation type for a column */
export type ColumnTransformationType =
  | 'raw'
  | 'passthrough'
  | 'renamed'
  | 'derived';

/** Unique identifier for a column in a model: "model_name.column_name" */
export type ColumnNodeId = string;

/** A node in the column lineage DAG */
export interface ColumnLineageNode {
  /** Unique identifier: "model_name.column_name" */
  id: ColumnNodeId;
  /** Column name */
  columnName: string;
  /** Model or source name */
  modelName: string;
  /** Layer in the data pipeline */
  modelLayer: ModelLayerType;
  /** Model type (e.g., 'stg_select_source', 'int_join_models', etc.) */
  modelType: string;
  /** Column data type */
  dataType?: string;
  /** Column description */
  description?: string;
  /** How the column was transformed */
  transformation: ColumnTransformationType;
  /** Expression used for derived/renamed columns */
  expression?: string;
  /** Path to the .model.json file (not available for sources) */
  filePath?: string;
}

/** An edge in the column lineage DAG */
export interface ColumnLineageEdge {
  /** Upstream column (parent) */
  source: ColumnNodeId;
  /** Downstream column (child) */
  target: ColumnNodeId;
  /** Transformation type */
  transformation: ColumnTransformationType;
  /** Transformation expression if any */
  expression?: string;
}

/** The complete column lineage DAG */
export interface ColumnLineageDAG {
  /** The target column this lineage was computed for */
  targetColumn: ColumnNodeId;
  /** All nodes in the lineage (keyed by id for O(1) lookup) */
  nodes: Map<ColumnNodeId, ColumnLineageNode>;
  /** All edges representing transformations */
  edges: ColumnLineageEdge[];
  /** Root nodes (source columns with no upstream) */
  roots: ColumnNodeId[];
  /** Leaf nodes (columns with no downstream in this lineage) */
  leaves: ColumnNodeId[];
  /** Whether there are more upstream nodes beyond the current roots (truncated by depth limit) */
  hasMoreUpstream: boolean;
  /** Whether there are more downstream nodes beyond the current leaves (truncated by depth limit) */
  hasMoreDownstream: boolean;
}

/** Result wrapper with metadata for column lineage computation */
export interface ColumnLineageResult {
  /** Whether the lineage computation succeeded */
  success: boolean;
  /** Error message if failed */
  error?: string;
  /** The computed lineage DAG */
  dag?: ColumnLineageDAG;
}

/**
 * Serializable version of ColumnLineageDAG for API responses and UI consumption.
 * Uses array instead of Map for JSON compatibility.
 */
export interface ColumnLineageDAGSerialized {
  /** The target column this lineage was computed for */
  targetColumn: ColumnNodeId;
  /** All nodes in the lineage as array */
  nodes: ColumnLineageNode[];
  /** All edges representing transformations */
  edges: ColumnLineageEdge[];
  /** Root nodes (source columns with no upstream) */
  roots: ColumnNodeId[];
  /** Leaf nodes (columns with no downstream in this lineage) */
  leaves: ColumnNodeId[];
  /** Whether there are more upstream nodes beyond the current roots */
  hasMoreUpstream: boolean;
  /** Whether there are more downstream nodes beyond the current leaves */
  hasMoreDownstream: boolean;
}

/**
 * Convert a ColumnLineageDAG (with Map) to serializable format (with array)
 */
export function serializeColumnLineageDAG(
  dag: ColumnLineageDAG,
): ColumnLineageDAGSerialized {
  return {
    targetColumn: dag.targetColumn,
    nodes: Array.from(dag.nodes.values()),
    edges: dag.edges,
    roots: dag.roots,
    leaves: dag.leaves,
    hasMoreUpstream: dag.hasMoreUpstream,
    hasMoreDownstream: dag.hasMoreDownstream,
  };
}

/**
 * Convert a serialized DAG back to ColumnLineageDAG (with Map)
 */
export function deserializeColumnLineageDAG(
  serialized: ColumnLineageDAGSerialized,
): ColumnLineageDAG {
  return {
    targetColumn: serialized.targetColumn,
    nodes: new Map(serialized.nodes.map((n) => [n.id, n])),
    edges: serialized.edges,
    roots: serialized.roots,
    leaves: serialized.leaves,
    hasMoreUpstream: serialized.hasMoreUpstream,
    hasMoreDownstream: serialized.hasMoreDownstream,
  };
}
