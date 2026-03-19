import { PlusCircleIcon } from '@heroicons/react/24/outline';
import type { EdgeProps } from '@xyflow/react';
import { BaseEdge, EdgeLabelRenderer, getBezierPath } from '@xyflow/react';
import React from 'react';

interface JoinEdgeData {
  onAddJoin?: () => void;
  label?: string;
}

export const JoinEdge: React.FC<EdgeProps> = ({
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  data,
  markerEnd,
}) => {
  const edgeData = data as JoinEdgeData;

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const handleAddJoin = (event: React.MouseEvent) => {
    event.stopPropagation();
    event.preventDefault();
    if (edgeData?.onAddJoin) {
      edgeData.onAddJoin();
    }
  };

  return (
    <>
      <BaseEdge
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          ...style,
          strokeWidth: 3,
          stroke: '#111',
        }}
      />

      <EdgeLabelRenderer>
        <div
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
            fontSize: 12,
            pointerEvents: 'all',
          }}
          className="nodrag nopan"
        >
          <div className="flex items-center gap-2 bg-background border border-border rounded-lg px-2 py-1 shadow-md">
            <button
              onClick={handleAddJoin}
              className="flex items-center gap-1 px-2 py-1 text-xs text-primary-foreground hover:bg-primary/90 rounded-md transition-colors"
              title="Add another join"
            >
              <PlusCircleIcon className="w-3 h-3" />
              Add Join
            </button>
          </div>
        </div>
      </EdgeLabelRenderer>
    </>
  );
};
