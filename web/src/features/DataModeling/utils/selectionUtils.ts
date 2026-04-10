import type { SchemaSelect } from '@web/stores/useModelStore';

import type { Column, SelectionTypeValues } from '../types';
import { SelectionType, supportsColumnName, supportsExprOnly } from '../types';

export interface UpdateSelectionsConfig {
  /** Qualifier prefix for entries owned by this node (model name or alias) */
  qualifier: string;
  /** Column names available from the current model/node */
  modelColumnNames: Set<string>;
  /** Current model type (e.g., 'int_join_models') */
  modelType: string;
  selectionType: SelectionType | '';
  selection: { include?: string[]; exclude?: string[] };
  shouldClear: boolean;
  /** The selected model/source identifier */
  selectedModelValue: string;
  /** Whether this is a source-type model (only used by SelectNode) */
  isTypeSource?: boolean;
  /** Column metadata for type determination fallback */
  columns?: Column[];
}

/**
 * Returns true when the given `{name, expr}` entry was created by the node
 * identified by `qualifier`.  Unqualified legacy entries (`expr === name`) are
 * treated as owned by whoever has the column in their column list.
 */
function isOwnedByQualifier(
  expr: string,
  name: string,
  qualifier: string,
): boolean {
  if (expr === name) return true;
  if (expr.endsWith(`.${name}`)) {
    const entryQualifier = expr.substring(0, expr.lastIndexOf('.'));
    return entryQualifier === qualifier;
  }
  return false;
}

function isSimpleExprEntry(
  sel: Exclude<SchemaSelect, string>,
): sel is { name: string; expr?: string } {
  return (
    'name' in sel && 'expr' in sel && !('model' in sel) && !('source' in sel)
  );
}

function isSimpleColumnRef(sel: { name: string; expr?: string }): boolean {
  const { name, expr } = sel;
  if (!expr) return false;
  return expr === name || expr.endsWith(`.${name}`);
}

/**
 * Pure function that builds the next `select` array after a column selection
 * change from either SelectNode or JoinNode.
 *
 * Qualifier-aware: only removes entries whose `expr` prefix matches the
 * calling node's qualifier, preventing cross-model removal of shared column
 * names.
 */
export function buildUpdatedSelections(
  currentSelections: SchemaSelect[],
  config: UpdateSelectionsConfig,
): SchemaSelect[] {
  const {
    qualifier,
    modelColumnNames,
    modelType,
    selectionType,
    selection,
    shouldClear,
    selectedModelValue,
    isTypeSource = false,
    columns = [],
  } = config;

  const isFilterMode = selectionType !== '';

  // --- Step 1: Filter out entries owned by the current node ---

  const filtered: SchemaSelect[] = currentSelections.filter(
    (existingSelection) => {
      if (typeof existingSelection === 'string') {
        return !modelColumnNames.has(existingSelection);
      }

      if (
        isSimpleExprEntry(existingSelection) &&
        isSimpleColumnRef(existingSelection)
      ) {
        const expr = existingSelection.expr!;
        const { name } = existingSelection;

        if (isFilterMode) {
          return !isOwnedByQualifier(expr, name, qualifier);
        }
        if (!isOwnedByQualifier(expr, name, qualifier)) return true;
        return !modelColumnNames.has(name);
      }

      if ('model' in existingSelection || 'source' in existingSelection) {
        const existingModel =
          'model' in existingSelection
            ? existingSelection.model
            : 'source' in existingSelection
              ? existingSelection.source
              : undefined;
        return existingModel !== selectedModelValue;
      }

      return true;
    },
  );

  // --- Step 2: Add new entries (unless shouldClear) ---

  if (shouldClear) return filtered;

  if (selectionType === '') {
    const columnNames = selection.include || [];

    const columnsToAdd = columnNames.filter((colName) => {
      return !filtered.some(
        (item) =>
          typeof item !== 'string' && 'name' in item && item.name === colName,
      );
    });

    if (columnsToAdd.length > 0) {
      if (supportsColumnName(modelType)) {
        columnsToAdd.forEach((colName) => {
          filtered.push(colName as never);
        });
      } else if (supportsExprOnly(modelType)) {
        columnsToAdd.forEach((colName) => {
          filtered.push({
            name: colName,
            expr: qualifier ? `${qualifier}.${colName}` : colName,
          } as never);
        });
      } else {
        addTypeDeterminedSelection(
          filtered,
          columnsToAdd,
          columns,
          selectedModelValue,
          isTypeSource,
        );
      }
    }
  } else {
    const baseSelection = isTypeSource
      ? {
          source: selectedModelValue,
          type: selectionType as SelectionTypeValues,
        }
      : {
          model: selectedModelValue,
          type: selectionType as SelectionTypeValues,
        };

    const newSelection = {
      ...baseSelection,
      ...(selection.include && selection.include.length > 0
        ? { include: selection.include }
        : {}),
      ...(selection.exclude && selection.exclude.length > 0
        ? { exclude: selection.exclude }
        : {}),
    };

    filtered.push(newSelection as never);
  }

  return filtered;
}

/**
 * Fallback path: determines the selection type from column metadata and pushes
 * a typed `{ model/source, type, include }` entry.
 */
function addTypeDeterminedSelection(
  filtered: SchemaSelect[],
  columnsToAdd: string[],
  columns: Column[],
  selectedModelValue: string,
  isTypeSource: boolean,
): void {
  const withTypes = columnsToAdd.map((colName) => {
    const info = columns.find((c) => c.name === colName);
    return { name: colName, type: info?.type || 'dimension' };
  });

  const hasDimensions = withTypes.some((c) => c.type === 'dimension');
  const hasFacts = withTypes.some((c) => c.type === 'fact');

  let determinedType: SelectionTypeValues;
  if (isTypeSource) {
    determinedType = SelectionType.ALL_FROM_SOURCE;
  } else if (hasDimensions && hasFacts) {
    determinedType = SelectionType.ALL_FROM_MODEL;
  } else if (hasFacts) {
    determinedType = SelectionType.FCTS_FROM_MODEL;
  } else {
    determinedType = SelectionType.DIMS_FROM_MODEL;
  }

  const baseSelection = isTypeSource
    ? { source: selectedModelValue, type: determinedType }
    : { model: selectedModelValue, type: determinedType };

  filtered.push({ ...baseSelection, include: columnsToAdd } as never);
}
