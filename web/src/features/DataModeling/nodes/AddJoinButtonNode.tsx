import { Button } from '@headlessui/react';
import { PlusIcon } from '@heroicons/react/20/solid';
import type { SchemaModelFromJoinModels } from '@shared/schema/types/model.type.int_join_models.schema';
import { useModelStore } from '@web/stores/useModelStore';
import { useTutorialStore } from '@web/stores/useTutorialStore';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import React, { useCallback } from 'react';
import { v4 as uuidv4 } from 'uuid';

export const AddJoinButtonNode: React.FC<NodeProps> = () => {
  const { modelingState, setModelingState } = useModelStore();

  // Tutorial store integration
  const isPlayTutorialActive = useTutorialStore(
    (state) => state.isPlayTutorialActive,
  );

  const handleAddJoin = useCallback(
    (event: React.MouseEvent) => {
      event.stopPropagation();
      event.preventDefault();

      // During tutorial, prevent user from adding joins
      if (isPlayTutorialActive) {
        return;
      }

      // Create a new default join entry matching SchemaModelFromJoinModels structure
      const newJoin = {
        model: '',
        type: 'left' as const, // Default join type
        on: {
          and: [], // Empty join conditions array
        },
        _uuid: uuidv4(),
      };

      // Add new join to modelingState.join array - handle null case properly
      let updatedJoins: SchemaModelFromJoinModels;

      if (
        modelingState.join === null ||
        (Array.isArray(modelingState.join) && modelingState.join.length === 0)
      ) {
        updatedJoins = [newJoin] as SchemaModelFromJoinModels;
      } else if (Array.isArray(modelingState.join)) {
        updatedJoins = [
          ...modelingState.join,
          newJoin,
        ] as SchemaModelFromJoinModels;
      } else {
        // If it's not an array (int_join_column case), start fresh with new join
        updatedJoins = [newJoin] as SchemaModelFromJoinModels;
      }

      const newModelingState = {
        ...modelingState,
        join: updatedJoins,
      };

      setModelingState(newModelingState);
    },
    [modelingState, setModelingState, isPlayTutorialActive],
  );

  return (
    <div className="relative" data-tutorial-id="add-join-button-node">
      <Handle
        type="target"
        position={Position.Top}
        id="input"
        className="w-3 h-3 !bg-border"
      />

      <div className="bg-background border-2 border-dashed border-border p-2 rounded-sm hover:border-primary/50 transition-colors">
        <Button
          onClick={handleAddJoin}
          className="w-full px-6 py-3 flex gap-1 items-center justify-center text-sm text-primary-contrast font-bold bg-primary hover:bg-primary/90 rounded-md transition-colors"
          title="Add another join node"
        >
          <PlusIcon className="w-6 h-6" />
          <div>ADD JOIN</div>
        </Button>
      </div>

      <Handle
        type="source"
        position={Position.Bottom}
        id="output"
        className="w-3 h-3 !bg-border"
      />
    </div>
  );
};
