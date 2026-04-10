import { XMarkIcon } from '@heroicons/react/20/solid';
import { Button, EditableList, ListItem } from '@web/elements';
import { ActionType } from '@web/features/DataModeling/types';
import { useModelStore } from '@web/stores/useModelStore';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import { useCallback } from 'react';

export const GroupByNode: React.FC<NodeProps> = () => {
  const { groupBy, setGroupByExpressions, setPendingRemovalAction } =
    useModelStore();

  const handleRemoveGroupBy = useCallback(() => {
    setPendingRemovalAction(ActionType.GROUPBY);
  }, [setPendingRemovalAction]);

  return (
    <div
      className="bg-background border-2 rounded-lg border-neutral shadow-lg p-4 flex flex-col gap-4 w-[32rem] cursor-default"
      data-tutorial-id="groupby-node"
    >
      <Handle type="target" position={Position.Top} id="input" />
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-foreground">Group By</h2>
        <Button
          onClick={handleRemoveGroupBy}
          variant="iconButton"
          title="Remove group by"
          label=""
          icon={<XMarkIcon className="w-7 h-7 text-foreground" />}
        />
      </div>

      {groupBy.dimensions && (
        <div className="flex flex-col gap-2 p-4 border border-neutral rounded-lg">
          <h3 className="text-md text-foreground">Group by All Dimensions</h3>
          <p className="text-xs text-muted-foreground">
            All dimension columns will be grouped automatically
          </p>
        </div>
      )}

      {groupBy.columns.length > 0 && (
        <div className="flex flex-col gap-2 p-4 border border-neutral rounded-lg">
          <h3 className="text-md text-foreground">Group by Columns</h3>
          <div className="flex flex-col gap-2 max-h-[200px] overflow-y-auto react-flow__node-scrollable">
            {groupBy.columns.map((column) => (
              <ListItem key={column}>{column}</ListItem>
            ))}
          </div>
        </div>
      )}

      <div className="flex flex-col gap-2 p-4 border border-neutral rounded-lg">
        <h3 className="text-md text-foreground">Group by Expressions</h3>
        <EditableList
          items={groupBy.expressions}
          onChange={setGroupByExpressions}
          placeholder="Enter expression"
          emptyText="No expressions added"
          addButtonLabel="Add"
          iconSize="w-6 h-6"
        />
      </div>
      <Handle type="source" position={Position.Bottom} id="output" />
    </div>
  );
};
