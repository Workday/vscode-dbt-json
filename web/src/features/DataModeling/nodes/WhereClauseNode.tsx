import { XMarkIcon } from '@heroicons/react/20/solid';
import {
  ChevronDownIcon,
  ChevronRightIcon,
  PlusIcon,
  TrashIcon,
} from '@heroicons/react/24/outline';
import type { SchemaModelWhere } from '@shared/schema/types/model.schema';
import type { SchemaModelSubquery } from '@shared/schema/types/model.subquery.schema';
import { useApp } from '@web/context';
import type { RadioOption } from '@web/elements';
import { Button, InputText, RadioGroup } from '@web/elements';
import type { SubqueryCondition } from '@web/features/DataModeling/components/SubqueryEditor';
import { SubqueryEditor } from '@web/features/DataModeling/components/SubqueryEditor';
import { ActionType } from '@web/features/DataModeling/types';
import { useDebounce } from '@web/hooks/useDebounce';
import { useModelStore } from '@web/stores/useModelStore';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import { useEffect, useMemo, useRef, useState } from 'react';

const whereTypeOptions: RadioOption[] = [
  { label: 'Basic', value: 'basic' },
  { label: 'Advanced', value: 'advanced' },
];

const conditionTypeOptions: RadioOption[] = [
  { label: 'AND', value: 'and' },
  { label: 'OR', value: 'or' },
];

const conditionItemOptions: RadioOption[] = [
  { label: 'Expression', value: 'expr' },
  { label: 'Group', value: 'group' },
  { label: 'Subquery', value: 'subquery' },
];

type GroupCondition = {
  conditionType: 'and' | 'or';
  expressions: string[];
};

type ConditionItem =
  | { type: 'expr'; value: string }
  | { type: 'group'; value: GroupCondition }
  | { type: 'subquery'; value: SubqueryCondition };

function conditionsToSchema(
  conditionType: string,
  conditions: ConditionItem[],
): SchemaModelWhere | null {
  const items = conditions
    .map((c) => {
      if (c.type === 'expr' && c.value) {
        return { expr: c.value };
      }
      if (c.type === 'group') {
        const g = c.value;
        const exprs = g.expressions.filter(Boolean);
        if (exprs.length === 0) return null;
        return {
          group: {
            [g.conditionType]: exprs.map((e) => ({ expr: e })),
          },
        };
      }
      if (c.type === 'subquery') {
        const s = c.value;
        if (!s.selectCols || !s.fromValue) return null;
        const from: SchemaModelSubquery['from'] =
          s.fromType === 'model'
            ? { model: s.fromValue }
            : s.fromType === 'source'
              ? { source: s.fromValue }
              : { cte: s.fromValue };
        const subquery: SchemaModelSubquery = {
          operator: s.operator,
          select: s.selectCols.split(',').map((col) => col.trim()) as [
            string,
            ...string[],
          ],
          from,
          ...(s.column ? { column: s.column } : {}),
          ...(s.innerWhere ? { where: s.innerWhere } : {}),
        };
        return { subquery };
      }
      return null;
    })
    .filter(Boolean) as Array<{
    expr?: string;
    group?: SchemaModelWhere;
    subquery?: SchemaModelSubquery;
  }>;

  if (items.length === 0) return null;
  return { [conditionType]: items };
}

function schemaToConditions(whereData: SchemaModelWhere): {
  conditionType: string;
  conditions: ConditionItem[];
} {
  if (typeof whereData === 'string') {
    return { conditionType: 'and', conditions: [] };
  }

  const key = whereData.and ? 'and' : 'or';
  const items = whereData.and || whereData.or || [];
  const conditions: ConditionItem[] = items
    .map((item): ConditionItem | null => {
      if (item.subquery) {
        const s = item.subquery;
        const fromType: SubqueryCondition['fromType'] =
          'model' in s.from ? 'model' : 'source' in s.from ? 'source' : 'cte';
        const fromValue =
          'model' in s.from
            ? s.from.model
            : 'source' in s.from
              ? s.from.source
              : 'cte' in s.from
                ? s.from.cte
                : '';
        return {
          type: 'subquery',
          value: {
            operator: s.operator,
            column: s.column || '',
            selectCols: s.select.join(', '),
            fromType,
            fromValue,
            innerWhere: typeof s.where === 'string' ? s.where : '',
          },
        };
      }
      if (item.group) {
        const g =
          typeof item.group === 'string'
            ? { and: [{ expr: item.group }] }
            : item.group;
        const gKey = 'and' in g && g.and ? 'and' : 'or';
        const gItems = ('and' in g ? g.and : 'or' in g ? g.or : []) || [];
        return {
          type: 'group',
          value: {
            conditionType: gKey,
            expressions: gItems.map((gi) => gi.expr || '').filter(Boolean),
          },
        };
      }
      if (item.expr) {
        return { type: 'expr', value: item.expr };
      }
      return null;
    })
    .filter(Boolean) as ConditionItem[];

  return { conditionType: key, conditions };
}

function GroupEditor({
  group,
  onChange,
  onRemove,
}: {
  group: GroupCondition;
  onChange: (g: GroupCondition) => void;
  onRemove: () => void;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const [newExpr, setNewExpr] = useState('');

  return (
    <div className="border border-neutral rounded-md bg-card/50 p-2 flex flex-col gap-2">
      <div className="flex items-center gap-2">
        <Button
          onClick={() => setCollapsed(!collapsed)}
          variant="iconButton"
          label=""
          title={collapsed ? 'Expand group' : 'Collapse group'}
          icon={
            collapsed ? (
              <ChevronRightIcon className="w-4 h-4 text-foreground" />
            ) : (
              <ChevronDownIcon className="w-4 h-4 text-foreground" />
            )
          }
        />
        <span className="text-sm font-medium text-muted-foreground">Group</span>
        <RadioGroup
          name="group-condition-type"
          options={conditionTypeOptions}
          value={group.conditionType}
          onChange={(v) =>
            onChange({ ...group, conditionType: v as 'and' | 'or' })
          }
          className="basis-[8rem]"
        />
        <div className="flex-1" />
        <Button
          onClick={onRemove}
          variant="iconButton"
          label=""
          title="Remove group"
          icon={<TrashIcon className="w-5 h-5 text-error" />}
        />
      </div>
      {!collapsed && (
        <div className="flex flex-col gap-1 pl-4">
          {group.expressions.map((expr, i) => (
            <div key={i} className="flex items-center gap-2">
              <InputText
                placeholder="Enter expression"
                value={expr}
                onChange={(e) => {
                  const newExprs = [...group.expressions];
                  newExprs[i] = e.target.value;
                  onChange({ ...group, expressions: newExprs });
                }}
                onBlur={(e) => {
                  if (e.target.value.trim() === '') {
                    onChange({
                      ...group,
                      expressions: group.expressions.filter(
                        (_, idx) => idx !== i,
                      ),
                    });
                  }
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') e.preventDefault();
                }}
              />
              <Button
                onClick={() =>
                  onChange({
                    ...group,
                    expressions: group.expressions.filter(
                      (_, idx) => idx !== i,
                    ),
                  })
                }
                variant="iconButton"
                label=""
                title="Remove expression"
                icon={<TrashIcon className="w-5 h-5 text-error" />}
              />
            </div>
          ))}
          <div className="flex items-center gap-2">
            <InputText
              placeholder="Enter expression"
              value={newExpr}
              onChange={(e) => setNewExpr(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  if (newExpr.trim()) {
                    onChange({
                      ...group,
                      expressions: [...group.expressions, newExpr.trim()],
                    });
                    setNewExpr('');
                  }
                }
              }}
            />
            <Button
              onClick={() => {
                if (newExpr.trim()) {
                  onChange({
                    ...group,
                    expressions: [...group.expressions, newExpr.trim()],
                  });
                  setNewExpr('');
                }
              }}
              label="Add"
              title="Add expression to group"
              variant="outlineIconButton"
              icon={<PlusIcon className="w-5 h-5" />}
            />
          </div>
        </div>
      )}
    </div>
  );
}

export const WhereClauseNode: React.FC<NodeProps> = () => {
  const initRef = useRef(false);
  const { api } = useApp();

  const { setWhereState, setPendingRemovalAction } = useModelStore();
  const ctes = useModelStore((state) => state.ctes);

  const whereData = useModelStore((state) => state.where);

  const [subqueryModels, setSubqueryModels] = useState<string[]>([]);
  const [subquerySources, setSubquerySources] = useState<string[]>([]);
  const [manifest, setManifest] = useState<Record<string, unknown> | null>(
    null,
  );

  useEffect(() => {
    const fetchProjectData = async () => {
      try {
        const projectsResponse = await api.post({
          type: 'dbt-fetch-projects',
          request: null,
        });
        const projects = projectsResponse || [];
        if (projects.length === 0) return;

        const project = projects[0];
        if (project.manifest) {
          setManifest(project.manifest as Record<string, unknown>);
        }
        if (project.manifest?.nodes) {
          const modelNames = Object.keys(project.manifest.nodes)
            .filter(
              (key) =>
                key.startsWith('model.') ||
                key.startsWith('seed.') ||
                key.startsWith('source.'),
            )
            .map((key: string) => project.manifest.nodes[key]?.name)
            .filter((name: unknown): name is string => Boolean(name));
          setSubqueryModels(modelNames);
        }
        if (project.manifest?.sources) {
          const sourceNames = Object.keys(project.manifest.sources)
            .filter((key: string) => key.startsWith('source.'))
            .map((key: string) => {
              const source = project.manifest.sources[key];
              return source?.source_name && source?.name
                ? `${source.source_name}.${source.name}`
                : null;
            })
            .filter((name: unknown): name is string => Boolean(name));
          setSubquerySources(sourceNames);
        }
      } catch {
        // Silently fail - subquery dropdowns will fall back to text inputs
      }
    };
    void fetchProjectData();
  }, [api]);

  const subqueryModelOptions = useMemo(
    () => subqueryModels.map((m) => ({ label: m, value: m })),
    [subqueryModels],
  );
  const subquerySourceOptions = useMemo(
    () => subquerySources.map((s) => ({ label: s, value: s })),
    [subquerySources],
  );
  const subqueryCteOptions = useMemo(
    () => ctes.map((c) => ({ label: c.name, value: c.name })),
    [ctes],
  );

  const [type, setType] = useState<string>(whereTypeOptions[0].value);

  const [basicExpression, setBasicExpression] = useState<string>('');
  const debouncedBasicExpression = useDebounce(basicExpression, 500);

  const [conditionType, setConditionType] = useState<string>(
    conditionTypeOptions[0].value,
  );

  const [conditions, setConditions] = useState<ConditionItem[]>([]);

  const [addItemType, setAddItemType] = useState<string>(
    conditionItemOptions[0].value,
  );
  const [advancedInputExpression, setAdvancedInputExpression] =
    useState<string>('');

  useEffect(() => {
    if (whereData && !initRef.current) {
      initRef.current = true;
      if (typeof whereData === 'string') {
        setType(whereTypeOptions[0].value);
        setBasicExpression(whereData);
      } else {
        setType(whereTypeOptions[1].value);
        const parsed = schemaToConditions(whereData);
        setConditionType(parsed.conditionType);
        setConditions(parsed.conditions);
      }
    }
  }, [whereData]);

  const handleRemoveCondition = (index: number) => {
    setConditions(conditions.filter((_, i) => i !== index));
  };

  const handleAddExpr = (expression: string) => {
    if (expression.trim() === '') return;
    setConditions([...conditions, { type: 'expr', value: expression.trim() }]);
    setAdvancedInputExpression('');
  };

  const handleAddGroup = () => {
    setConditions([
      ...conditions,
      {
        type: 'group',
        value: { conditionType: 'or', expressions: [] },
      },
    ]);
  };

  const handleAddSubquery = () => {
    setConditions([
      ...conditions,
      {
        type: 'subquery',
        value: {
          operator: 'in',
          column: '',
          selectCols: '',
          fromType: 'model',
          fromValue: '',
          innerWhere: '',
        },
      },
    ]);
  };

  const handleRemoveWhereClause = () => {
    setPendingRemovalAction(ActionType.WHERE);
  };

  useEffect(() => {
    let expressionsToSync: SchemaModelWhere | null = null;

    if (type === 'advanced') {
      expressionsToSync = conditionsToSchema(conditionType, conditions);
    } else if (type === 'basic') {
      if (debouncedBasicExpression.trim() !== '') {
        expressionsToSync = debouncedBasicExpression.trim();
      }
    }

    setWhereState(expressionsToSync);
  }, [
    type,
    conditions,
    conditionType,
    debouncedBasicExpression,
    setWhereState,
  ]);

  return (
    <div
      className="bg-background border-2 rounded-lg border-neutral shadow-lg p-4 flex flex-col gap-4 w-[40rem] cursor-default"
      data-tutorial-id="where-node"
    >
      <Handle type="target" position={Position.Top} id="input" />
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-foreground">Where Clause</h2>
        <Button
          onClick={() => handleRemoveWhereClause()}
          variant="iconButton"
          title="Remove where clause"
          label=""
          icon={<XMarkIcon className="w-7 h-7 text-foreground" />}
        />
      </div>

      <div className="flex flex-col gap-3 p-3 border border-neutral rounded-md bg-card">
        <div className="flex items-center gap-4">
          <RadioGroup
            name="basic-type"
            options={[whereTypeOptions[0]]}
            value={type}
            onChange={(value) => setType(value)}
            className="basis-[8rem]"
          />
        </div>
        {type === 'basic' && (
          <InputText
            placeholder="Enter expression"
            value={basicExpression}
            onChange={(e) => setBasicExpression(e.target.value)}
            onBlur={(e) => {
              if (e.target.value.trim() === '') {
                setBasicExpression('');
              }
            }}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
              }
            }}
          />
        )}
      </div>

      <div className="flex flex-col gap-3 p-3 border border-neutral rounded-md bg-card">
        <div className="flex items-center gap-4">
          <RadioGroup
            name="advanced-type"
            options={[whereTypeOptions[1]]}
            value={type}
            onChange={(value) => setType(value)}
            className="basis-[8rem]"
          />
        </div>
        {type === 'advanced' && (
          <div className="flex flex-col gap-3">
            <div className="flex items-center gap-4">
              <p className="text-md text-foreground">Type</p>
              <RadioGroup
                name="Select Where Condition Type"
                options={conditionTypeOptions}
                value={conditionType}
                onChange={(value) => setConditionType(value)}
                className="basis-[8rem]"
              />
            </div>

            <h3 className="text-md text-foreground">Conditions</h3>
            <div className="flex flex-col gap-2 max-h-[300px] overflow-y-auto react-flow__node-scrollable">
              {conditions.length === 0 && (
                <div className="flex justify-center gap-2">
                  <p className="text-md text-muted-foreground text-center py-2">
                    No conditions added
                  </p>
                </div>
              )}
              {conditions.map((condition, index) => {
                if (condition.type === 'expr') {
                  return (
                    <div key={index} className="flex items-center gap-2">
                      <InputText
                        placeholder="Enter expression"
                        value={condition.value}
                        onChange={(e) =>
                          setConditions(
                            conditions.map((c, i) =>
                              i === index
                                ? { type: 'expr', value: e.target.value }
                                : c,
                            ),
                          )
                        }
                        onBlur={(e) => {
                          if (e.target.value.trim() === '') {
                            handleRemoveCondition(index);
                          }
                        }}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') {
                            e.preventDefault();
                          }
                        }}
                      />
                      <Button
                        onClick={(e) => {
                          e.stopPropagation();
                          handleRemoveCondition(index);
                        }}
                        variant="iconButton"
                        title="Remove expression"
                        label=""
                        icon={<TrashIcon className="w-6 h-6 text-error" />}
                      />
                    </div>
                  );
                }
                if (condition.type === 'group') {
                  return (
                    <GroupEditor
                      key={index}
                      group={condition.value}
                      onChange={(g) =>
                        setConditions(
                          conditions.map((c, i) =>
                            i === index ? { type: 'group', value: g } : c,
                          ),
                        )
                      }
                      onRemove={() => handleRemoveCondition(index)}
                    />
                  );
                }
                if (condition.type === 'subquery') {
                  return (
                    <SubqueryEditor
                      key={index}
                      subquery={condition.value}
                      onChange={(s) =>
                        setConditions(
                          conditions.map((c, i) =>
                            i === index ? { type: 'subquery', value: s } : c,
                          ),
                        )
                      }
                      onRemove={() => handleRemoveCondition(index)}
                      modelOptions={subqueryModelOptions}
                      sourceOptions={subquerySourceOptions}
                      cteOptions={subqueryCteOptions}
                      manifest={manifest}
                      ctes={ctes}
                    />
                  );
                }
                return null;
              })}
            </div>

            <div className="flex flex-col gap-4">
              <div className="flex">
                <RadioGroup
                  name="add-item-type"
                  options={conditionItemOptions}
                  value={addItemType}
                  onChange={(value) => setAddItemType(value)}
                  className="gap-4"
                />
              </div>
              {addItemType === 'expr' && (
                <div className="flex items-center gap-4">
                  <InputText
                    placeholder="Enter expression"
                    value={advancedInputExpression}
                    onChange={(e) => setAdvancedInputExpression(e.target.value)}
                    onMouseDown={(e) => e.stopPropagation()}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        handleAddExpr(advancedInputExpression);
                      }
                    }}
                  />
                  <Button
                    onClick={() => handleAddExpr(advancedInputExpression)}
                    label="Add"
                    title="Add expression"
                    variant="outlineIconButton"
                    icon={<PlusIcon className="w-6 h-6" />}
                  />
                </div>
              )}
              {addItemType === 'group' && (
                <Button
                  onClick={handleAddGroup}
                  label="Add Group"
                  title="Add a nested condition group"
                  variant="outlineIconButton"
                  icon={<PlusIcon className="w-6 h-6" />}
                />
              )}
              {addItemType === 'subquery' && (
                <Button
                  onClick={handleAddSubquery}
                  label="Add Subquery"
                  title="Add a subquery condition"
                  variant="outlineIconButton"
                  icon={<PlusIcon className="w-6 h-6" />}
                />
              )}
            </div>
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Bottom} id="output" />
    </div>
  );
};
