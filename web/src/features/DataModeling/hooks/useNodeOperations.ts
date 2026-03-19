import type { Edge, Node } from '@xyflow/react';
import { useCallback } from 'react';

interface UpdateFlowParams {
  id: string;
  type: string;
  position: { x: number; y: number };
  data?: Record<string, unknown>;
  sourceNodeId?: string;
  sourceHandle?: string;
  targetHandle?: string;
}

export const useNodeOperations = () => {
  // Common function to update flow with new nodes and edges
  const updateFlow = useCallback(
    (
      params: UpdateFlowParams,
      currentNodes: Node[],
      currentEdges: Edge[],
      edgeStyle: Record<string, unknown> = {
        strokeWidth: 4,
        stroke: 'var(--color-surface-contrast)',
      },
    ): { nodes: Node[]; edges: Edge[] } => {
      const {
        id,
        type,
        position,
        data = {},
        sourceNodeId,
        sourceHandle = 'output',
        targetHandle = 'input',
      } = params;

      // Create new node
      const newNode: Node = {
        id,
        type,
        position,
        data,
      };

      const updatedNodes = [...currentNodes, newNode];
      let updatedEdges = [...currentEdges];

      // Create edge if source node is specified
      if (sourceNodeId) {
        const newEdge: Edge = {
          id: `${sourceNodeId}-${id}`,
          source: sourceNodeId,
          target: id,
          sourceHandle,
          targetHandle,
          style: edgeStyle,
        };
        updatedEdges = [...updatedEdges, newEdge];
      }

      return {
        nodes: updatedNodes,
        edges: updatedEdges,
      };
    },
    [],
  );

  // Batch update flow for multiple connections to the same target
  const updateFlowWithMultipleSources = useCallback(
    (
      params: UpdateFlowParams,
      currentNodes: Node[],
      currentEdges: Edge[],
      sourceNodeIds: string[],
      edgeStyle: Record<string, unknown> = {
        strokeWidth: 4,
        stroke: 'var(--color-surface-contrast)',
      },
    ): { nodes: Node[]; edges: Edge[] } => {
      const {
        id,
        type,
        position,
        data = {},
        sourceHandle = 'output',
        targetHandle = 'input',
      } = params;

      // Create new node
      const newNode: Node = {
        id,
        type,
        position,
        data,
      };

      const updatedNodes = [...currentNodes, newNode];
      let updatedEdges = [...currentEdges];

      // Create edges for all source nodes
      sourceNodeIds.forEach((sourceNodeId, index) => {
        const newEdge: Edge = {
          id: `${sourceNodeId}-${id}${index > 0 ? `-${index}` : ''}`,
          source: sourceNodeId,
          target: id,
          sourceHandle,
          targetHandle,
          style: edgeStyle,
        };
        updatedEdges = [...updatedEdges, newEdge];
      });

      return {
        nodes: updatedNodes,
        edges: updatedEdges,
      };
    },
    [],
  );

  return {
    updateFlow,
    updateFlowWithMultipleSources,
  };
};
