// Utility function to calculate optimal include/exclude based on selection efficiency
export const modelColumnSelectionList = (
  allColumns: string[],
  selectedColumns: string[],
): { include?: string[]; exclude?: string[] } => {
  const totalColumns = allColumns.length;
  const selectedCount = selectedColumns.length;
  const excludedCount = totalColumns - selectedCount;

  // Rule 1: If nothing is selected, exclude all columns
  if (selectedCount === 0) {
    return { exclude: allColumns };
  }

  // Rule 2: If all columns are selected, return empty object
  if (selectedCount === totalColumns) {
    return {};
  }

  // Rule 3: If selecting only a few items (less than half), use include
  if (selectedCount <= totalColumns / 2) {
    return { include: selectedColumns };
  }

  // Rule 4: If excluding fewer items, use exclude
  if (excludedCount < selectedCount) {
    const excludedColumns = allColumns.filter(
      (col) => !selectedColumns.includes(col),
    );
    return { exclude: excludedColumns };
  }

  // Rule 5: If equal or close, prefer include
  return { include: selectedColumns };
};

// Utility function to get selected columns from include/exclude
export const getSelectedColumns = (
  allColumns: string[],
  include?: string[],
  exclude?: string[],
): string[] => {
  if (include) {
    return include.filter((col) => allColumns.includes(col));
  }
  if (exclude) {
    return allColumns.filter((col) => !exclude.includes(col));
  }
  return allColumns; // If neither include nor exclude, all are selected
};

// Import types from ModelStore
import type { ModelingStateAdapter } from '../stores/useModelStore';

interface JoinWithUUID {
  model?: string;
  _uuid?: string;
}

/**
 * Get all used models and sources from modelingState
 * This is the comprehensive version used by UnionNode that tracks all usage across:
 * - from.model/source (base model/source)
 * - union.models/sources (union selections)
 * - join[].model (all join models)
 * - select[].model (all select models)
 *
 * @param modelingState - The current modeling state from ModelStore
 * @returns Set of all used model/source names
 */
export const getAllUsedModelsAndSources = (
  modelingState: ModelingStateAdapter,
): Set<string> => {
  const used = new Set<string>();

  // Add from.model and from.source with proper type checking
  if (
    modelingState.from?.model &&
    typeof modelingState.from.model === 'string'
  ) {
    used.add(modelingState.from.model);
  }
  if (
    modelingState.from?.source &&
    typeof modelingState.from.source === 'string'
  ) {
    used.add(modelingState.from.source);
  }
  if (modelingState.from?.cte && typeof modelingState.from.cte === 'string') {
    used.add(modelingState.from.cte);
  }

  // Add union models and sources
  if (
    modelingState.union?.models &&
    Array.isArray(modelingState.union.models)
  ) {
    modelingState.union.models.forEach((model) => {
      if (typeof model === 'string') used.add(model);
    });
  }
  if (
    modelingState.union?.sources &&
    Array.isArray(modelingState.union.sources)
  ) {
    modelingState.union.sources.forEach((source) => {
      if (typeof source === 'string') used.add(source);
    });
  }

  // Add join models
  if (modelingState.join && Array.isArray(modelingState.join)) {
    modelingState.join.forEach((joinItem) => {
      if (
        joinItem &&
        typeof joinItem === 'object' &&
        'model' in joinItem &&
        typeof joinItem.model === 'string'
      ) {
        used.add(joinItem.model);
      }
    });
  }

  // Add select models and CTE refs
  if (modelingState.select && Array.isArray(modelingState.select)) {
    modelingState.select.forEach((selectItem) => {
      if (!selectItem || typeof selectItem !== 'object') return;
      if ('model' in selectItem && typeof selectItem.model === 'string') {
        used.add(selectItem.model);
      }
      const item = selectItem as Record<string, unknown>;
      if ('cte' in item && typeof item.cte === 'string') {
        used.add(item.cte);
      }
    });
  }

  return used;
};

/**
 * Get used models excluding current context (for SelectNode)
 * Tracks models from from.model, join operations, and select operations
 * that specify different models than the base model
 *
 * @param modelingState - The current modeling state from ModelStore
 * @returns Set of used model names that should be excluded from SelectNode dropdown
 */
export const getUsedModelsForSelect = (
  modelingState: ModelingStateAdapter,
): Set<string> => {
  const used = new Set<string>();

  // Add the base model/cte from 'from' if it exists
  if (
    modelingState.from?.model &&
    typeof modelingState.from.model === 'string'
  ) {
    used.add(modelingState.from.model);
  }
  if (modelingState.from?.cte && typeof modelingState.from.cte === 'string') {
    used.add(modelingState.from.cte);
  }

  // Add models from join operations
  if (modelingState.join && Array.isArray(modelingState.join)) {
    modelingState.join.forEach((join) => {
      if (
        join &&
        typeof join === 'object' &&
        'model' in join &&
        typeof join.model === 'string'
      ) {
        used.add(join.model);
      }
    });
  }

  // Add models/CTEs from select operations
  if (modelingState.select && Array.isArray(modelingState.select)) {
    modelingState.select.forEach((select) => {
      if (typeof select !== 'object' || !select) return;
      if (
        'model' in select &&
        typeof select.model === 'string' &&
        select.model !== modelingState.from?.model
      ) {
        used.add(select.model);
      }
      if (
        'cte' in select &&
        typeof (select as Record<string, unknown>).cte === 'string'
      ) {
        used.add((select as Record<string, unknown>).cte as string);
      }
    });
  }

  return used;
};

/**
 * Get used models excluding current join (for JoinNode)
 * Tracks models from from.model and other join operations, but excludes the
 * current join being edited to allow modification
 *
 * @param modelingState - The current modeling state from ModelStore
 * @param currentJoin - The current join being edited (will be excluded from used models)
 * @returns Set of used model names that should be excluded from JoinNode dropdown
 */
export const getUsedModelsForJoin = (
  modelingState: ModelingStateAdapter,
  currentJoin?: JoinWithUUID,
): Set<string> => {
  const used = new Set<string>();

  // Add the base model from 'from' if it exists
  if (
    modelingState.from?.model &&
    typeof modelingState.from.model === 'string'
  ) {
    used.add(modelingState.from.model);
  }

  // Add models from other join operations (excluding current join)
  if (modelingState.join && Array.isArray(modelingState.join)) {
    modelingState.join.forEach((join) => {
      if (
        join &&
        typeof join === 'object' &&
        'model' in join &&
        typeof join.model === 'string'
      ) {
        // Exclude current join from the used models (allow editing current join)
        const joinWithUUID = join as JoinWithUUID;
        const currentJoinUUID = currentJoin?._uuid;
        if (currentJoinUUID && joinWithUUID._uuid !== currentJoinUUID) {
          used.add(join.model);
        } else if (!currentJoinUUID && join.model !== currentJoin?.model) {
          // Fallback to model name comparison if no UUID
          used.add(join.model);
        }
      }
    });
  }

  return used;
};

/**
 * Filter available models while preserving currently selected models
 * Creates dropdown options by excluding already used models but keeping
 * currently selected models to allow editing
 *
 * @param allModels - Complete list of available models/sources
 * @param usedModels - Set of models/sources already used elsewhere
 * @param currentSelections - Models/sources currently selected (preserved for editing)
 * @returns Array of dropdown options with label/value structure
 */
export const filterAvailableModels = (
  allModels: string[],
  usedModels: Set<string>,
  currentSelections: string[] = [],
): Array<{ label: string; value: string }> => {
  const currentSelectionsSet = new Set(currentSelections);

  return allModels
    .filter((modelName) => {
      // Allow if it's currently selected, or if it's not used elsewhere
      return currentSelectionsSet.has(modelName) || !usedModels.has(modelName);
    })
    .map((modelName) => ({
      label: modelName,
      value: modelName,
    }));
};
