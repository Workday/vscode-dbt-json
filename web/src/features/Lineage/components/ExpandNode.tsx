import { Button } from '@web/elements/Button';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import React, { memo } from 'react';

/** Direction for expand node */
export type ExpandDirection = 'upstream' | 'downstream';

/** Data passed to ExpandNode */
export interface ExpandNodeData {
  direction: ExpandDirection;
  onClick: () => void;
}

/**
 * Compact expand node for lineage DAG boundaries.
 * Shows a '+' icon and triggers expansion when clicked.
 */
export const ExpandNode: React.FC<NodeProps> = memo(({ data }) => {
  const { direction, onClick } = data as unknown as ExpandNodeData;
  const isUpstream = direction === 'upstream';

  return (
    <div className="relative w-16 h-16 flex items-center justify-center">
      {/* Handle on the side facing the DAG */}
      {isUpstream ? (
        <Handle
          type="source"
          position={Position.Right}
          className="!w-2 !h-2 !bg-surface-contrast !border-neutral"
        />
      ) : (
        <Handle
          type="target"
          position={Position.Left}
          className="!w-2 !h-2 !bg-surface-contrast !border-neutral"
        />
      )}

      <Button
        variant="secondary"
        label="+"
        onClick={onClick}
        title={
          isUpstream
            ? 'Expand upstream by 1 level'
            : 'Expand downstream by 1 level'
        }
        className="w-12 h-12 !text-xl !font-bold"
      />
    </div>
  );
});

ExpandNode.displayName = 'ExpandNode';
