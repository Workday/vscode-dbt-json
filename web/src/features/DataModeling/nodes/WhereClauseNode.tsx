import { XMarkIcon } from '@heroicons/react/20/solid';
import { PlusIcon, TrashIcon } from '@heroicons/react/24/outline';
import type { SchemaModelWhere } from '@shared/schema/types/model.schema';
import type { RadioOption } from '@web/elements';
import { Button, InputText, RadioGroup } from '@web/elements';
import { ActionType } from '@web/features/DataModeling/types';
import { useDebounce } from '@web/hooks/useDebounce';
import { useModelStore } from '@web/stores/useModelStore';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import { useEffect, useRef, useState } from 'react';

const whereTypeOptions: RadioOption[] = [
  { label: 'Basic', value: 'basic' },
  { label: 'Advanced', value: 'advanced' },
];

const conditionTypeOptions: RadioOption[] = [
  { label: 'AND', value: 'and' },
  { label: 'OR', value: 'or' },
];

export const WhereClauseNode: React.FC<NodeProps> = () => {
  const initRef = useRef(false);

  const { setWhereState, setPendingRemovalAction } = useModelStore();

  // On load, get the where clause from the model store
  const whereData = useModelStore((state) => state.where);

  const [type, setType] = useState<string>(whereTypeOptions[0].value);

  const [basicExpression, setBasicExpression] = useState<string>('');
  // Debounce the expression for Basic mode to avoid syncing on every keystroke
  const debouncedBasicExpression = useDebounce(basicExpression, 500);

  const [advancedInputExpression, setAdvancedInputExpression] =
    useState<string>('');

  const [conditionType, setConditionType] = useState<string>(
    conditionTypeOptions[0].value,
  );

  const [expressions, setExpressions] = useState<string[]>([]);

  // Initialize from store - watch for whereData changes for tutorial prefill
  useEffect(() => {
    if (whereData && !initRef.current) {
      initRef.current = true;
      if (typeof whereData === 'string') {
        setType(whereTypeOptions[0].value);
        setBasicExpression(whereData);
      } else {
        setType(whereTypeOptions[1].value);
        const key = whereData.and ? 'and' : 'or';
        setConditionType(key);
        const items = whereData.and || whereData.or || [];
        setExpressions(items.map((item) => item.expr || '').filter(Boolean));
      }
    }
  }, [whereData]); // Watch whereData for tutorial prefill support

  const handleRemoveExpressions = (index: number) => {
    setExpressions(expressions.filter((_, i) => i !== index));
  };

  const handleAddExpressions = (expression: string) => {
    if (expression.trim() === '') return;
    setExpressions([...expressions, expression.trim()]);
    setAdvancedInputExpression('');
  };

  const handleRemoveWhereClause = () => {
    // Request removal via the store (ActionsBar will show the dialog)
    setPendingRemovalAction(ActionType.WHERE);
  };

  // Sync to store when user makes changes
  useEffect(() => {
    let expressionsToSync: SchemaModelWhere | null = null;

    if (type === 'advanced') {
      // Advanced mode: sync immediately on add/remove
      if (expressions.length > 0) {
        expressionsToSync = {
          [conditionType]: expressions.map((expression) => ({
            expr: expression,
          })),
        };
      }
    } else if (type === 'basic') {
      // Basic mode: use debounced value to avoid syncing on every keystroke
      if (debouncedBasicExpression.trim() !== '') {
        expressionsToSync = debouncedBasicExpression.trim();
      }
    }

    setWhereState(expressionsToSync);
  }, [
    type,
    expressions,
    conditionType,
    debouncedBasicExpression,
    setWhereState,
  ]);

  return (
    <div
      className="bg-background border-2 rounded-lg border-surface shadow-lg p-4 flex flex-col gap-4 w-[40rem] cursor-default"
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

      {/* Basic Section */}
      <div className="flex flex-col gap-3 p-3 border border-surface rounded-md bg-card">
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

      {/* Advanced Section */}
      <div className="flex flex-col gap-3 p-3 border border-surface rounded-md bg-card">
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
            {/* Condition Type */}
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

            <h3 className="text-md text-foreground">Expressions</h3>
            <div className="flex flex-col gap-2 max-h-[200px] overflow-y-auto p-1 react-flow__node-scrollable">
              {expressions.length === 0 && (
                <div className="flex justify-center gap-2">
                  <p className="text-md text-muted-foreground text-center py-2">
                    No expressions added
                  </p>
                </div>
              )}
              {expressions.map((expression, index) => (
                <div key={index} className="flex items-center gap-2">
                  <InputText
                    placeholder="Enter expression"
                    value={expression}
                    onChange={(e) =>
                      setExpressions(
                        expressions.map((exp, i) =>
                          i === index ? e.target.value : exp,
                        ),
                      )
                    }
                    onBlur={(e) => {
                      if (e.target.value.trim() === '') {
                        handleRemoveExpressions(index);
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
                      handleRemoveExpressions(index);
                    }}
                    variant="iconButton"
                    title="Remove expression"
                    label=""
                    icon={<TrashIcon className="w-6 h-6 text-error" />}
                  />
                </div>
              ))}
            </div>
            <div className="flex items-center gap-4">
              <InputText
                placeholder="Enter expression"
                value={advancedInputExpression}
                onChange={(e) => setAdvancedInputExpression(e.target.value)}
                onMouseDown={(e) => e.stopPropagation()}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault();
                    handleAddExpressions(advancedInputExpression);
                  }
                }}
              />
              <Button
                onClick={() => handleAddExpressions(advancedInputExpression)}
                label="Add"
                title="Add expression"
                variant="outlineIconButton"
                icon={<PlusIcon className="w-6 h-6" />}
              />
            </div>
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Bottom} id="output" />
    </div>
  );
};
