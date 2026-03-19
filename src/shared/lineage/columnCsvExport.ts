/**
 * CSV export utilities for column lineage
 */

import type {
  ColumnLineageDAG,
  ColumnLineageNode,
  ColumnNodeId,
} from './columnTypes';

/** CSV row structure for column lineage export */
export interface ColumnLineageCsvRow {
  model: string;
  column: string;
  data_type: string;
  source_model: string;
  source_column: string;
  ancestors: string;
  descendants: string;
}

/** Arrow separator for lineage paths (ASCII-safe) */
const PATH_SEPARATOR = ' -> ';

/**
 * Escape a value for CSV (handles commas, quotes, newlines)
 */
function escapeCsvValue(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/**
 * Build the ancestor path for a node by traversing edges backward
 */
function buildAncestorPath(
  nodeId: ColumnNodeId,
  dag: ColumnLineageDAG,
): string {
  const path: string[] = [];
  const visited = new Set<ColumnNodeId>();

  function traverse(currentId: ColumnNodeId): void {
    if (visited.has(currentId)) {
      return;
    }
    visited.add(currentId);

    // Find parent edges (where current node is the target)
    const parentEdges = dag.edges.filter((e) => e.target === currentId);

    for (const edge of parentEdges) {
      const parentNode = dag.nodes.get(edge.source);
      if (parentNode) {
        traverse(edge.source);
      }
    }

    // Add current node to path after processing parents
    const node = dag.nodes.get(currentId);
    if (node) {
      path.push(`${node.modelName}.${node.columnName}`);
    }
  }

  traverse(nodeId);

  // Remove the target node itself from the path (we only want ancestors)
  if (path.length > 0) {
    path.pop();
  }

  return path.join(PATH_SEPARATOR);
}

/**
 * Build the descendant path for a node by traversing edges forward
 */
function buildDescendantPath(
  nodeId: ColumnNodeId,
  dag: ColumnLineageDAG,
): string {
  const paths: string[][] = [];

  function traverse(currentId: ColumnNodeId, currentPath: string[]): void {
    // Find child edges (where current node is the source)
    const childEdges = dag.edges.filter((e) => e.source === currentId);

    if (childEdges.length === 0) {
      // Leaf node - save the path if it has any nodes
      if (currentPath.length > 0) {
        paths.push([...currentPath]);
      }
      return;
    }

    for (const edge of childEdges) {
      const childNode = dag.nodes.get(edge.target);
      if (childNode) {
        const newPath = [
          ...currentPath,
          `${childNode.modelName}.${childNode.columnName}`,
        ];
        traverse(edge.target, newPath);
      }
    }
  }

  traverse(nodeId, []);

  // Join multiple paths with semicolon if there are multiple descendants
  return paths.map((p) => p.join(PATH_SEPARATOR)).join('; ');
}

/**
 * Find the ultimate source(s) for a node
 */
function findSources(
  nodeId: ColumnNodeId,
  dag: ColumnLineageDAG,
): ColumnLineageNode[] {
  const sources: ColumnLineageNode[] = [];
  const visited = new Set<ColumnNodeId>();

  function traverse(currentId: ColumnNodeId): void {
    if (visited.has(currentId)) {
      return;
    }
    visited.add(currentId);

    const parentEdges = dag.edges.filter((e) => e.target === currentId);

    if (parentEdges.length === 0) {
      // This is a root/source node
      const node = dag.nodes.get(currentId);
      if (node) {
        sources.push(node);
      }
      return;
    }

    for (const edge of parentEdges) {
      traverse(edge.source);
    }
  }

  traverse(nodeId);
  return sources;
}

/**
 * Generate CSV content for a single column's lineage
 */
export function generateLineageCSV(
  dag: ColumnLineageDAG,
  targetColumn: string,
  modelName: string,
): string {
  const rows: ColumnLineageCsvRow[] = [];

  // Find the target node
  const targetNodeId = `${modelName}.${targetColumn}`;
  const targetNode = dag.nodes.get(targetNodeId);

  if (!targetNode) {
    // If target node not found, return empty CSV with headers
    return 'model,column,data_type,source_model,source_column,ancestors,descendants';
  }

  // Find sources
  const sources = findSources(targetNodeId, dag);
  const sourceInfo =
    sources.length > 0
      ? {
          // Deduplicate model names while preserving column list
          model: [...new Set(sources.map((s) => s.modelName))].join('; '),
          column: sources.map((s) => s.columnName).join('; '),
        }
      : { model: '', column: '' };

  // Build paths
  const ancestors = buildAncestorPath(targetNodeId, dag);
  const descendants = buildDescendantPath(targetNodeId, dag);

  rows.push({
    model: modelName,
    column: targetColumn,
    data_type: targetNode.dataType ?? '',
    source_model: sourceInfo.model,
    source_column: sourceInfo.column,
    ancestors,
    descendants,
  });

  return formatCsv(rows);
}

/**
 * Generate CSV content for all columns in a model
 */
export function generateAllColumnsCSV(
  results: Array<{
    column: string;
    dag: ColumnLineageDAG;
    modelName: string;
    dataType?: string;
  }>,
): string {
  const rows: ColumnLineageCsvRow[] = [];

  for (const { column, dag, modelName, dataType } of results) {
    const targetNodeId = `${modelName}.${column}`;
    const targetNode = dag.nodes.get(targetNodeId);

    // Find sources
    const sources = findSources(targetNodeId, dag);
    const sourceInfo =
      sources.length > 0
        ? {
            // Deduplicate model names while preserving column list
            model: [...new Set(sources.map((s) => s.modelName))].join('; '),
            column: sources.map((s) => s.columnName).join('; '),
          }
        : { model: '', column: '' };

    // Build paths
    const ancestors = buildAncestorPath(targetNodeId, dag);
    const descendants = buildDescendantPath(targetNodeId, dag);

    rows.push({
      model: modelName,
      column,
      data_type: targetNode?.dataType ?? dataType ?? '',
      source_model: sourceInfo.model,
      source_column: sourceInfo.column,
      ancestors,
      descendants,
    });
  }

  return formatCsv(rows);
}

/**
 * Format rows as CSV string
 */
function formatCsv(rows: ColumnLineageCsvRow[]): string {
  const headers = [
    'model',
    'column',
    'data_type',
    'source_model',
    'source_column',
    'ancestors',
    'descendants',
  ];

  const lines = [headers.join(',')];

  for (const row of rows) {
    const values = [
      escapeCsvValue(row.model),
      escapeCsvValue(row.column),
      escapeCsvValue(row.data_type),
      escapeCsvValue(row.source_model),
      escapeCsvValue(row.source_column),
      escapeCsvValue(row.ancestors),
      escapeCsvValue(row.descendants),
    ];
    lines.push(values.join(','));
  }

  return lines.join('\n');
}
