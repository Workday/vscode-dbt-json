import type { DbtProjectManifest } from '@shared/dbt/types';
import type { CteState } from '@web/stores/useModelStore';
import { useMemo } from 'react';

import { findModelNode, findSourceNode } from '../utils/manifestColumns';

/**
 * Resolves available column names for a subquery's FROM entity
 * by looking up the dbt manifest (for models/sources) or CTE state.
 */
export function useSubqueryFromColumns(
  fromType: 'model' | 'source' | 'cte',
  fromValue: string,
  manifest: DbtProjectManifest | Record<string, unknown> | null | undefined,
  ctes: CteState[],
): string[] {
  return useMemo(() => {
    if (!fromValue) {
      return [];
    }

    if (fromType === 'model') {
      const node = findModelNode(
        manifest as DbtProjectManifest | null | undefined,
        fromValue,
      );
      return node?.columns ? Object.keys(node.columns) : [];
    }

    if (fromType === 'source') {
      const src = findSourceNode(
        manifest as DbtProjectManifest | null | undefined,
        fromValue,
      );
      return src?.columns ? Object.keys(src.columns) : [];
    }

    if (fromType === 'cte') {
      const cte = ctes.find((c) => c.name === fromValue);
      if (!cte?.select) {
        return [];
      }

      const columns: string[] = [];
      for (const item of cte.select) {
        if (typeof item === 'string') {
          columns.push(item);
        } else if (item && typeof item === 'object' && 'name' in item) {
          columns.push((item as { name: string }).name);
        }
        // Skip bulk selectors (all_from_model, etc.)
      }
      return columns;
    }

    return [];
  }, [fromType, fromValue, manifest, ctes]);
}
