import type {
  DbtProjectManifest,
  DbtProjectManifestNode,
  DbtProjectManifestSource,
} from '@shared/dbt/types';

import type { Column } from '../types';

/**
 * Converts a manifest node or source object's columns map into a Column array.
 * Shared core extracted from SelectNode.extractColumns,
 * JoinNode.extractColumnsFromModel, and JoinColumnNode.fetchBaseColumns.
 */
export function extractColumnsFromNode(
  node:
    | Partial<DbtProjectManifestNode>
    | Partial<DbtProjectManifestSource>
    | undefined,
): Column[] {
  if (!node?.columns) return [];
  return Object.entries(node.columns).map(([columnName, columnData]) => {
    const columnType: 'dimension' | 'fact' =
      columnData?.meta?.type === 'fct' ? 'fact' : 'dimension';
    return {
      name: columnName,
      dataType: columnData?.data_type || 'string',
      type: columnType,
      description: columnData?.description,
    };
  });
}

/**
 * Finds a model or seed node in the manifest by name.
 * Checks both `model.*` and `seed.*` prefixed keys.
 */
export function findModelNode(
  manifest: DbtProjectManifest | null | undefined,
  modelName: string,
): Partial<DbtProjectManifestNode> | undefined {
  if (!manifest?.nodes || !modelName) return undefined;
  const key = Object.keys(manifest.nodes).find((k) => {
    if (!k.startsWith('model.') && !k.startsWith('seed.')) return false;
    return manifest.nodes[k]?.name === modelName;
  });
  return key ? manifest.nodes[key] : undefined;
}

/**
 * Finds a source node in the manifest by "source_name.table_name" reference string.
 */
export function findSourceNode(
  manifest: DbtProjectManifest | null | undefined,
  sourceRef: string,
): Partial<DbtProjectManifestSource> | undefined {
  if (!manifest?.sources || !sourceRef) return undefined;
  const [sourceName, tableName] = sourceRef.split('.');
  if (!sourceName || !tableName) return undefined;
  const key = Object.keys(manifest.sources).find((k) => {
    const src = manifest.sources[k];
    return src?.source_name === sourceName && src?.name === tableName;
  });
  return key ? manifest.sources[key] : undefined;
}
