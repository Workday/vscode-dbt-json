import type { Edge, Node } from '@xyflow/react';
import { Position } from '@xyflow/react';
import { useCallback } from 'react';

const LAYOUT_CONFIG = {
  direction: 'TB',

  nodeWidth: 400,
  nodeHeight: 200,

  stages: {
    source: {
      rankSep: 600,
      nodeSep: 450,
    },
    transformation: {
      rankSep: 600,
      nodeSep: 480,
    },
    columnSelection: {
      rankSep: 200,
      nodeSep: 160,
    },
    finalProcessing: {
      rankSep: 600,
      nodeSep: 500,
    },
  },

  nodeTypes: {
    selectNode: { stage: 'source', priority: 1 },
    joinNode: { stage: 'transformation', priority: 2 },
    joinColumnNode: { stage: 'transformation', priority: 2 },
    addJoinButtonNode: { stage: 'source', priority: 1.5 },
    rollupNode: { stage: 'transformation', priority: 2 },
    lookbackNode: { stage: 'transformation', priority: 2 },
    unionNode: { stage: 'transformation', priority: 2 },
    columnSelectionNode: { stage: 'columnSelection', priority: 3 }, // Dynamic priority based on context
    whereNode: { stage: 'finalProcessing', priority: 4 },
    groupByNode: { stage: 'finalProcessing', priority: 4 },
    lightdashNode: { stage: 'finalProcessing', priority: 4 }, // Default priority, can be overridden
  },
} as const;

// Function to get dynamic priority for LightdashNode based on modelType
const getLightdashNodePriority = (modelType?: string | null): number => {
  if (!modelType) return 4; // Default priority

  if (
    modelType.includes('select_model') ||
    modelType.includes('select_source')
  ) {
    return 3;
  }

  return 4; // For all other modelTypes
};

// Function to get dynamic priority for ColumnSelectionNode based on context
const getColumnSelectionPriority = (nodes: Node[]): number => {
  // Check for transformation nodes (rollup, lookback, union, join)
  const hasTransformationNodes = nodes.some(
    (n) =>
      n.type === 'rollupNode' ||
      n.type === 'lookbackNode' ||
      n.type === 'unionNode' ||
      n.type === 'joinNode' ||
      n.type === 'joinColumnNode',
  );

  // If there are transformation nodes, column selection should be priority 3
  // If no transformation nodes, column selection should be priority 2
  return hasTransformationNodes ? 3 : 2;
};

export const useLayoutManager = () => {
  const analyzeNodeStages = useCallback(
    (nodes: Node[], layoutConfig = LAYOUT_CONFIG) => {
      const stageAnalysis = {
        source: nodes.filter(
          (node) =>
            layoutConfig.nodeTypes[
              node.type as keyof typeof layoutConfig.nodeTypes
            ]?.stage === 'source',
        ),
        transformation: nodes.filter(
          (node) =>
            layoutConfig.nodeTypes[
              node.type as keyof typeof layoutConfig.nodeTypes
            ]?.stage === 'transformation',
        ),
        columnSelection: nodes.filter(
          (node) =>
            layoutConfig.nodeTypes[
              node.type as keyof typeof layoutConfig.nodeTypes
            ]?.stage === 'columnSelection',
        ),
        finalProcessing: nodes.filter(
          (node) =>
            layoutConfig.nodeTypes[
              node.type as keyof typeof layoutConfig.nodeTypes
            ]?.stage === 'finalProcessing',
        ),
      };

      return stageAnalysis;
    },
    [],
  );

  const calculateLayoutConfig = useCallback(
    (nodes: Node[], layoutConfig = LAYOUT_CONFIG) => {
      const stages = analyzeNodeStages(nodes, layoutConfig);

      const stageWithMostNodes = Object.entries(stages).reduce(
        (max, [stageName, stageNodes]) => {
          return stageNodes.length > max.nodes
            ? { stage: stageName, nodes: stageNodes.length }
            : max;
        },
        { stage: 'source', nodes: 0 },
      );

      const baseConfig =
        layoutConfig.stages[
          stageWithMostNodes.stage as keyof typeof layoutConfig.stages
        ];

      const totalNodes = nodes.length;
      const complexityMultiplier = Math.min(1 + (totalNodes - 1) * 0.05, 1.5);

      return {
        ...baseConfig,
        rankSep: Math.round(baseConfig.rankSep * complexityMultiplier),
        nodeSep: Math.round(baseConfig.nodeSep * complexityMultiplier),
      };
    },
    [analyzeNodeStages],
  );

  const calculateCustomLayout = useCallback(
    (
      nodes: Node[],
      _edges: Edge[],
      direction: string,
      customLayoutConfig?: typeof LAYOUT_CONFIG,
      modelType?: string | null,
    ) => {
      if (nodes.length === 0) return nodes;

      const layoutConfig = customLayoutConfig || LAYOUT_CONFIG;

      const nodesByStage = new Map<number, Node[]>();
      nodes.forEach((node) => {
        const nodeTypeConfig =
          layoutConfig.nodeTypes[
            node.type as keyof typeof layoutConfig.nodeTypes
          ];

        let priority = nodeTypeConfig?.priority || 1;

        if (node.type === 'lightdashNode') {
          priority = getLightdashNodePriority(modelType) as 1 | 1.5 | 2 | 4;
        }

        if (node.type === 'columnSelectionNode') {
          priority = getColumnSelectionPriority(nodes) as 2 | 3;
        }

        if (!nodesByStage.has(priority)) {
          nodesByStage.set(priority, []);
        }
        nodesByStage.get(priority)!.push(node);
      });

      const dynamicLayoutConfig = calculateLayoutConfig(nodes, layoutConfig);
      const { nodeWidth, nodeHeight } = layoutConfig;
      const { rankSep, nodeSep } = dynamicLayoutConfig;

      const layoutedNodes: Node[] = [];
      let currentStagePosition = 0;

      const sortedStages = Array.from(nodesByStage.keys()).sort(
        (a, b) => a - b,
      );

      sortedStages.forEach((priority, stageIndex) => {
        const stageNodes = nodesByStage.get(priority)!;
        const nodesInStage = stageNodes.length;

        if (direction === 'TB') {
          const totalWidth = (nodesInStage - 1) * (nodeWidth + nodeSep);
          stageNodes.forEach((node, nodeIndex) => {
            let x = nodeIndex * (nodeWidth + nodeSep) - totalWidth / 2;
            let y = currentStagePosition;

            if (nodesInStage === 1) {
              x = 0;
            }

            if (node.type === 'selectNode' && stageIndex === 0) {
              y = 24;
            }

            layoutedNodes.push({
              ...node,
              position: { x, y },
              targetPosition: Position.Top,
              sourcePosition: Position.Bottom,
            });
          });

          let stageSpacing = rankSep;

          const hasAddJoinButton = stageNodes.some(
            (node) => node.type === 'addJoinButtonNode',
          );
          const nextStageIndex = stageIndex + 1;
          const nextStage =
            nextStageIndex < sortedStages.length
              ? nodesByStage.get(sortedStages[nextStageIndex])
              : null;
          const nextStageHasJoinNodes =
            nextStage?.some((node) => node.type === 'joinNode') || false;
          const nextStageHasColumnSelection =
            nextStage?.some((node) => node.type === 'columnSelectionNode') ||
            false;
          const nextStageHasFinalProcessingNodes =
            nextStage?.some(
              (node) =>
                node.type === 'lightdashNode' ||
                node.type === 'groupByNode' ||
                node.type === 'whereNode',
            ) || false;

          const hasTransformationNodes = stageNodes.some(
            (node) =>
              node.type === 'joinNode' ||
              node.type === 'joinColumnNode' ||
              node.type === 'rollupNode' ||
              node.type === 'lookbackNode' ||
              node.type === 'unionNode',
          );
          const hasJoinNodes = stageNodes.some(
            (node) => node.type === 'joinNode',
          );
          const hasColumnSelection = stageNodes.some(
            (node) => node.type === 'columnSelectionNode',
          );

          const hasRollupNode = stageNodes.some(
            (node) => node.type === 'rollupNode',
          );

          // Adjust spacing based on node types and relationships
          if (hasAddJoinButton && nextStageHasJoinNodes) {
            stageSpacing = rankSep * 0.2;
          } else if (hasTransformationNodes && nextStageHasColumnSelection) {
            if (hasJoinNodes) {
              stageSpacing = rankSep * 1.5;
            } else if (hasRollupNode) {
              stageSpacing = 0;
            } else {
              stageSpacing = rankSep * 0.6;
            }
          } else if (hasColumnSelection && nextStageHasFinalProcessingNodes) {
            // Extra spacing between column selection and final processing nodes
            stageSpacing = rankSep * 2.0;
          }

          currentStagePosition += nodeHeight + stageSpacing;
        } else {
          const totalHeight = (nodesInStage - 1) * (nodeHeight + nodeSep);
          stageNodes.forEach((node, nodeIndex) => {
            const x = currentStagePosition;
            const y = nodeIndex * (nodeHeight + nodeSep) - totalHeight / 2;
            layoutedNodes.push({
              ...node,
              position: { x, y },
              targetPosition: Position.Top,
              sourcePosition: Position.Bottom,
            });
          });

          let stageSpacing = rankSep;

          const hasAddJoinButton = stageNodes.some(
            (node) => node.type === 'addJoinButtonNode',
          );
          const nextStageIndex = stageIndex + 1;
          const nextStage =
            nextStageIndex < sortedStages.length
              ? nodesByStage.get(sortedStages[nextStageIndex])
              : null;
          const nextStageHasJoinNodes =
            nextStage?.some((node) => node.type === 'joinNode') || false;
          const nextStageHasColumnSelection =
            nextStage?.some((node) => node.type === 'columnSelectionNode') ||
            false;
          const nextStageHasFinalProcessingNodes =
            nextStage?.some(
              (node) =>
                node.type === 'lightdashNode' ||
                node.type === 'groupByNode' ||
                node.type === 'whereNode',
            ) || false;

          const hasTransformationNodes = stageNodes.some(
            (node) =>
              node.type === 'joinNode' ||
              node.type === 'joinColumnNode' ||
              node.type === 'rollupNode' ||
              node.type === 'lookbackNode' ||
              node.type === 'unionNode',
          );
          const hasColumnSelection = stageNodes.some(
            (node) => node.type === 'columnSelectionNode',
          );
          if (hasAddJoinButton && nextStageHasJoinNodes) {
            stageSpacing = rankSep * 0.2;
          } else if (hasTransformationNodes && nextStageHasColumnSelection) {
            stageSpacing = rankSep * 1.6;
          } else if (hasColumnSelection && nextStageHasFinalProcessingNodes) {
            stageSpacing = rankSep * 2.0;
          }

          currentStagePosition += nodeWidth + stageSpacing;
        }
      });

      return layoutedNodes;
    },
    [calculateLayoutConfig],
  );

  // Align nodes with center X position and handle positioning for join nodes and column selection
  const alignNodesWithCenterX = useCallback(
    (layoutedNodes: Node[], currentModelType: string) => {
      const layoutedSelectNode = layoutedNodes.find(
        (n) => n.type === 'selectNode',
      );
      const centerX = layoutedSelectNode?.position.x || 100;

      return layoutedNodes.map((node) => {
        if (node.id === 'column-selection') {
          let correctY = 400; // Default

          if (currentModelType === 'int_join_column') {
            // Position below JoinColumnNode - needs more space due to larger node size
            const joinColumnNodes = layoutedNodes.filter(
              (n) => n.type === 'joinColumnNode',
            );
            if (joinColumnNodes.length > 0) {
              correctY = joinColumnNodes[0].position.y + 1000; // Increased from 800 to 1000
            }
          } else if (currentModelType.includes('join')) {
            // Position below all join nodes
            const joinNodes = layoutedNodes.filter(
              (n) => n.type === 'joinNode',
            );
            if (joinNodes.length > 0) {
              const maxJoinY = Math.max(...joinNodes.map((n) => n.position.y));
              correctY = maxJoinY + 1450;
            }
          } else if (
            currentModelType.includes('rollup') ||
            currentModelType.includes('lookback') ||
            currentModelType.includes('union')
          ) {
            // Position below transformation node
            const transformNodes = layoutedNodes.filter(
              (n) =>
                n.type === 'rollupNode' ||
                n.type === 'lookbackNode' ||
                n.type === 'unionNode',
            );
            if (transformNodes.length > 0) {
              correctY = transformNodes[0].position.y + 550;
            }
          } else if (currentModelType.includes('select')) {
            // Position below SelectNode
            if (layoutedSelectNode) {
              correctY = layoutedSelectNode.position.y + 950;
            }
          }

          return {
            ...node,
            position: { x: centerX, y: correctY },
          };
        } else if (node.type === 'joinNode') {
          // Position join nodes horizontally in a row with gaps
          const joinNodes = layoutedNodes.filter((n) => n.type === 'joinNode');
          const joinIndex = parseInt(node.id.replace('join-', '')) - 1;
          const joinGap = 750;
          const startX = centerX - ((joinNodes.length - 1) * joinGap) / 2;
          const joinX = startX + joinIndex * joinGap;

          return {
            ...node,
            position: { x: joinX, y: node.position.y },
          };
        } else if (node.type === 'columnConfigurationNode') {
          // ColumnConfigurationNode is positioned horizontally to the right of column-selection
          // Find column-selection node to get its Y position
          const columnSelectionNode = layoutedNodes.find(
            (n) => n.id === 'column-selection',
          );
          if (columnSelectionNode) {
            return {
              ...node,
              position: {
                x: columnSelectionNode.position.x + 1200,
                y: columnSelectionNode.position.y,
              },
            };
          }
          return node;
        } else if (
          node.type === 'lightdashNode' ||
          node.type === 'groupByNode' ||
          node.type === 'whereNode'
        ) {
          // Preserve horizontal positioning for final processing nodes (already calculated in layout)
          // Keep the x position as calculated by the layout algorithm to maintain horizontal spacing
          return {
            ...node,
            // Keep the original x position calculated by the layout to maintain spacing
            position: { x: node.position.x, y: node.position.y },
          };
        } else {
          // Center all other nodes horizontally with SelectNode
          return {
            ...node,
            position: { x: centerX, y: node.position.y },
          };
        }
      });
    },
    [],
  );

  // Find the most relevant node to center the view on
  const findNodeToCenter = useCallback(
    (finalNodes: Node[], currentModelType: string): Node | undefined => {
      if (currentModelType.includes('join')) {
        // For join models: Center on the last (newest) join node
        const joinNodes = finalNodes.filter((n) => n.type === 'joinNode');
        if (joinNodes.length > 0) {
          return joinNodes.reduce((latest, current) => {
            const latestNum = parseInt(latest.id.replace('join-', ''));
            const currentNum = parseInt(current.id.replace('join-', ''));
            return currentNum > latestNum ? current : latest;
          });
        }
      } else {
        // For other model types: Center on ColumnSelectionNode if exists, otherwise transformation node
        const columnSelectionNode = finalNodes.find(
          (n) => n.id === 'column-selection',
        );
        if (columnSelectionNode) {
          return columnSelectionNode;
        } else {
          // Fallback to transformation node or SelectNode
          return (
            finalNodes.find(
              (n) =>
                n.type === 'rollupNode' ||
                n.type === 'lookbackNode' ||
                n.type === 'unionNode' ||
                n.type === 'joinColumnNode',
            ) || finalNodes.find((n) => n.type === 'selectNode')
          );
        }
      }
      return undefined;
    },
    [],
  );

  return {
    calculateCustomLayout,
    alignNodesWithCenterX,
    findNodeToCenter,
  };
};
