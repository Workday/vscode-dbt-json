import {
  ChevronDownIcon,
  ChevronRightIcon,
  TrashIcon,
} from '@heroicons/react/24/outline';
import type { SchemaModelSubquery } from '@shared/schema/types/model.subquery.schema';
import { Button, InputText } from '@web/elements';
import { SelectMulti } from '@web/elements/SelectMulti';
import { SelectSingle } from '@web/elements/SelectSingle';
import type { CteState } from '@web/stores/useModelStore';
import React, { useEffect, useMemo, useRef, useState } from 'react';

import { useSubqueryFromColumns } from '../hooks/useSubqueryFromColumns';

export type SubqueryCondition = {
  operator: SchemaModelSubquery['operator'];
  column: string;
  selectCols: string;
  fromType: 'model' | 'source' | 'cte';
  fromValue: string;
  innerWhere: string;
};

type DropdownOption = { label: string; value: string };

const operatorOptions: DropdownOption[] = [
  { label: 'IN', value: 'in' },
  { label: 'NOT IN', value: 'not_in' },
  { label: 'EXISTS', value: 'exists' },
  { label: 'NOT EXISTS', value: 'not_exists' },
  { label: '=', value: 'eq' },
  { label: '!=', value: 'neq' },
  { label: '>', value: 'gt' },
  { label: '>=', value: 'gte' },
  { label: '<', value: 'lt' },
  { label: '<=', value: 'lte' },
];

const existsOperators = new Set(['exists', 'not_exists']);
const setOperators = new Set(['in', 'not_in']);

interface SubqueryEditorProps {
  subquery: SubqueryCondition;
  onChange: (s: SubqueryCondition) => void;
  onRemove: () => void;
  modelOptions?: DropdownOption[];
  sourceOptions?: DropdownOption[];
  cteOptions?: DropdownOption[];
  columnOptions?: DropdownOption[];
  manifest?: Record<string, unknown> | null;
  ctes?: CteState[];
}

export const SubqueryEditor: React.FC<SubqueryEditorProps> = ({
  subquery,
  onChange,
  onRemove,
  modelOptions = [],
  sourceOptions = [],
  cteOptions = [],
  columnOptions = [],
  manifest = null,
  ctes = [],
}) => {
  const [collapsed, setCollapsed] = useState(false);
  const [selectMode, setSelectMode] = useState<'columns' | 'expression'>(
    'columns',
  );
  const needsColumn = !existsOperators.has(subquery.operator);
  const isExists = existsOperators.has(subquery.operator);
  const isSetOperator = setOperators.has(subquery.operator);

  const stopPropagation = (e: React.SyntheticEvent) => e.stopPropagation();

  const hasCtes = cteOptions.length > 0;

  // Resolve columns from the selected FROM entity
  const fromColumns = useSubqueryFromColumns(
    subquery.fromType,
    subquery.fromValue,
    manifest,
    ctes,
  );

  const fromColumnOptions: DropdownOption[] = useMemo(
    () => fromColumns.map((c) => ({ label: c, value: c })),
    [fromColumns],
  );

  // Track previous operator to handle switching logic
  const prevOperatorRef = useRef(subquery.operator);
  useEffect(() => {
    const prevOp = prevOperatorRef.current;
    prevOperatorRef.current = subquery.operator;

    // Switching TO exists/not_exists: auto-fill "1" if selectCols is empty
    if (
      existsOperators.has(subquery.operator) &&
      !existsOperators.has(prevOp)
    ) {
      if (!subquery.selectCols) {
        onChange({ ...subquery, selectCols: '1' });
      }
      setSelectMode('columns');
      return;
    }

    // Switching FROM exists/not_exists: clear "1" if that's the only value
    if (
      !existsOperators.has(subquery.operator) &&
      existsOperators.has(prevOp)
    ) {
      if (subquery.selectCols === '1') {
        onChange({ ...subquery, selectCols: '' });
      }
      setSelectMode('columns');
      return;
    }

    // Switching from set to scalar: keep only first value
    if (
      !setOperators.has(subquery.operator) &&
      !existsOperators.has(subquery.operator) &&
      setOperators.has(prevOp)
    ) {
      const firstVal = subquery.selectCols
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)[0];
      if (firstVal && firstVal !== subquery.selectCols.trim()) {
        onChange({ ...subquery, selectCols: firstVal });
      }
      setSelectMode('columns');
    }
  }, [subquery.operator]);

  const fromTypeOptions: DropdownOption[] = useMemo(
    () => [
      { label: 'Model', value: 'model' },
      { label: 'Source', value: 'source' },
      ...(hasCtes ? [{ label: 'CTE', value: 'cte' }] : []),
    ],
    [hasCtes],
  );

  const fromValueOptions: DropdownOption[] = useMemo(() => {
    if (subquery.fromType === 'model') return modelOptions;
    if (subquery.fromType === 'source') return sourceOptions;
    if (subquery.fromType === 'cte') return cteOptions;
    return [];
  }, [subquery.fromType, modelOptions, sourceOptions, cteOptions]);

  const headerSummary = useMemo(() => {
    const op = subquery.operator.toUpperCase().replace('_', ' ');
    const target = subquery.fromValue
      ? `${subquery.fromType}: ${subquery.fromValue}`
      : '';
    return target ? `${op} — ${target}` : op;
  }, [subquery.operator, subquery.fromType, subquery.fromValue]);

  // Parse selectCols into array for multi-select
  const selectColsArray = useMemo(
    () =>
      subquery.selectCols
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean),
    [subquery.selectCols],
  );

  const hasFromColumns = fromColumnOptions.length > 0;

  // Render the SELECT field based on operator type
  const renderSelectField = () => {
    // EXISTS/NOT EXISTS: simple InputText auto-filled with "1"
    if (isExists) {
      return (
        <InputText
          placeholder="e.g. 1 or *"
          value={subquery.selectCols}
          onChange={(e) =>
            onChange({ ...subquery, selectCols: e.target.value })
          }
          onClick={stopPropagation}
          onMouseDown={stopPropagation}
          onKeyDown={(e) => {
            e.stopPropagation();
            if (e.key === 'Enter') e.preventDefault();
          }}
        />
      );
    }

    // IN/NOT IN: SelectMulti with expression toggle
    if (isSetOperator) {
      if (selectMode === 'columns' && hasFromColumns) {
        return (
          <div className="flex items-center gap-2 w-full">
            <SelectMulti
              options={fromColumnOptions}
              value={selectColsArray}
              onChange={(values) =>
                onChange({ ...subquery, selectCols: values.join(', ') })
              }
              placeholder="Select columns"
              searchable={true}
              showSelectedTags={true}
              className="flex-1"
            />
            <button
              type="button"
              onClick={() => setSelectMode('expression')}
              className="text-xs text-primary hover:underline whitespace-nowrap"
              title="Switch to expression input"
            >
              Expr
            </button>
          </div>
        );
      }
      return (
        <div className="flex items-center gap-2 w-full">
          <InputText
            placeholder="e.g. col1, col2 (comma-separated)"
            value={subquery.selectCols}
            onChange={(e) =>
              onChange({ ...subquery, selectCols: e.target.value })
            }
            onClick={stopPropagation}
            onMouseDown={stopPropagation}
            onKeyDown={(e) => {
              e.stopPropagation();
              if (e.key === 'Enter') e.preventDefault();
            }}
          />
          {hasFromColumns && (
            <button
              type="button"
              onClick={() => setSelectMode('columns')}
              className="text-xs text-primary hover:underline whitespace-nowrap"
              title="Switch to column picker"
            >
              Columns
            </button>
          )}
        </div>
      );
    }

    // Scalar operators: SelectSingle with expression toggle
    if (selectMode === 'columns' && hasFromColumns) {
      return (
        <div className="flex items-center gap-2 w-full">
          <SelectSingle
            label=""
            options={fromColumnOptions}
            value={
              fromColumnOptions.find((o) => o.value === subquery.selectCols) ||
              null
            }
            onChange={(option) =>
              onChange({ ...subquery, selectCols: option?.value || '' })
            }
            onBlur={() => {}}
            placeholder="Select column"
            className="w-full text-xs h-10 bg-background text-foreground border border-neutral pl-2 rounded-lg"
            showClearButton={false}
          />
          <button
            type="button"
            onClick={() => setSelectMode('expression')}
            className="text-xs text-primary hover:underline whitespace-nowrap"
            title="Switch to expression input"
          >
            Expr
          </button>
        </div>
      );
    }

    return (
      <div className="flex items-center gap-2 w-full">
        <InputText
          placeholder="e.g. MAX(created_at)"
          value={subquery.selectCols}
          onChange={(e) =>
            onChange({ ...subquery, selectCols: e.target.value })
          }
          onClick={stopPropagation}
          onMouseDown={stopPropagation}
          onKeyDown={(e) => {
            e.stopPropagation();
            if (e.key === 'Enter') e.preventDefault();
          }}
        />
        {hasFromColumns && (
          <button
            type="button"
            onClick={() => setSelectMode('columns')}
            className="text-xs text-primary hover:underline whitespace-nowrap"
            title="Switch to column picker"
          >
            Columns
          </button>
        )}
      </div>
    );
  };

  return (
    <div className="border border-neutral rounded-md">
      <div className="flex items-center gap-2">
        <Button
          onClick={() => setCollapsed(!collapsed)}
          variant="iconButton"
          label=""
          title={collapsed ? 'Expand subquery' : 'Collapse subquery'}
          icon={
            collapsed ? (
              <ChevronRightIcon className="w-4 h-4 text-foreground" />
            ) : (
              <ChevronDownIcon className="w-4 h-4 text-foreground" />
            )
          }
        />
        <span className="text-sm font-medium text-muted-foreground">
          Subquery
        </span>
        <span className="text-xs text-muted-foreground truncate">
          {headerSummary}
        </span>
        <Button
          onClick={onRemove}
          variant="iconButton"
          label=""
          title="Remove subquery"
          icon={<TrashIcon className="w-5 h-5 text-error" />}
          className="ml-auto"
        />
      </div>
      {!collapsed && (
        <div className="flex flex-col gap-2 p-2">
          {/* Operator */}
          <div className="flex items-center gap-2">
            <p className="text-sm text-foreground w-20">Operator</p>
            <SelectSingle
              label=""
              options={operatorOptions}
              value={
                operatorOptions.find((o) => o.value === subquery.operator) ||
                null
              }
              onChange={(option) => {
                if (option) {
                  const newOp = option.value as SchemaModelSubquery['operator'];
                  const newIsExists = existsOperators.has(newOp);
                  onChange({
                    ...subquery,
                    operator: newOp,
                    ...(newIsExists ? { column: '' } : {}),
                  });
                }
              }}
              onBlur={() => {}}
              placeholder="Select operator"
              className="w-full text-xs h-10 bg-background text-foreground border border-neutral pl-2 rounded-lg"
              showClearButton={false}
            />
          </div>

          {/* Column (hidden for EXISTS/NOT EXISTS) */}
          {needsColumn && (
            <div className="flex items-center gap-2">
              <p className="text-sm text-foreground w-20">Column</p>
              {columnOptions.length > 0 ? (
                <SelectSingle
                  label=""
                  options={columnOptions}
                  value={
                    columnOptions.find((o) => o.value === subquery.column) ||
                    null
                  }
                  onChange={(option) =>
                    onChange({ ...subquery, column: option?.value || '' })
                  }
                  onBlur={() => {}}
                  placeholder="Select column"
                  className="w-full text-xs h-10 bg-background text-foreground border border-neutral pl-2 rounded-lg"
                  showClearButton={false}
                />
              ) : (
                <InputText
                  placeholder="e.g. account_id"
                  value={subquery.column}
                  onChange={(e) =>
                    onChange({ ...subquery, column: e.target.value })
                  }
                  onClick={stopPropagation}
                  onMouseDown={stopPropagation}
                  onKeyDown={(e) => {
                    e.stopPropagation();
                    if (e.key === 'Enter') e.preventDefault();
                  }}
                />
              )}
            </div>
          )}

          {/* FROM (moved before SELECT) */}
          <div className="flex items-center gap-2">
            <p className="text-sm text-foreground w-20">FROM</p>
            <div className="flex items-center gap-2 w-full">
              <div className="w-28 flex">
                <SelectSingle
                  label=""
                  options={fromTypeOptions}
                  value={
                    fromTypeOptions.find(
                      (o) => o.value === subquery.fromType,
                    ) || null
                  }
                  onChange={(option) => {
                    if (option) {
                      const newFromType =
                        option.value as SubqueryCondition['fromType'];
                      const typeChanged = newFromType !== subquery.fromType;
                      onChange({
                        ...subquery,
                        fromType: newFromType,
                        fromValue: typeChanged ? '' : subquery.fromValue,
                        selectCols: typeChanged ? '' : subquery.selectCols,
                      });
                      if (typeChanged) setSelectMode('columns');
                    }
                  }}
                  onBlur={() => {}}
                  placeholder="Type"
                  className="w-full text-xs h-10 bg-background text-foreground border border-neutral pl-2 rounded-lg"
                  showClearButton={false}
                />
              </div>
              {fromValueOptions.length > 0 ? (
                <SelectSingle
                  label=""
                  options={fromValueOptions}
                  value={
                    fromValueOptions.find(
                      (o) => o.value === subquery.fromValue,
                    ) || null
                  }
                  onChange={(option) => {
                    const valueChanged = option?.value !== subquery.fromValue;
                    onChange({
                      ...subquery,
                      fromValue: option?.value || '',
                      selectCols: valueChanged ? '' : subquery.selectCols,
                    });
                    if (valueChanged) setSelectMode('columns');
                  }}
                  onBlur={() => {}}
                  placeholder={`Select ${subquery.fromType}`}
                  className="w-full text-xs h-10 bg-background text-foreground border border-neutral pl-2 rounded-lg"
                  showClearButton={false}
                />
              ) : (
                <InputText
                  placeholder={`e.g. my_${subquery.fromType}`}
                  value={subquery.fromValue}
                  onChange={(e) =>
                    onChange({
                      ...subquery,
                      fromValue: e.target.value,
                      selectCols: '',
                    })
                  }
                  onClick={stopPropagation}
                  onMouseDown={stopPropagation}
                  onKeyDown={(e) => {
                    e.stopPropagation();
                    if (e.key === 'Enter') e.preventDefault();
                  }}
                />
              )}
            </div>
          </div>

          {/* SELECT (after FROM, with column suggestions) */}
          <div className="flex items-center gap-2">
            <p className="text-sm text-foreground w-20">SELECT</p>
            {renderSelectField()}
          </div>

          {/* WHERE */}
          <div className="flex items-center gap-2">
            <label className="text-sm text-foreground w-20">WHERE</label>
            <InputText
              placeholder="Optional inner WHERE condition"
              value={subquery.innerWhere}
              onChange={(e) =>
                onChange({ ...subquery, innerWhere: e.target.value })
              }
              onClick={stopPropagation}
              onMouseDown={stopPropagation}
              onKeyDown={(e) => {
                e.stopPropagation();
                if (e.key === 'Enter') e.preventDefault();
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
