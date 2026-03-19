import '@xyflow/react/dist/style.css';
import '../DataModeling/DataModeling.css';

import { MapIcon } from '@heroicons/react/20/solid';
import type { SchemaModelFromJoinModels } from '@shared/schema/types/model.type.int_join_models.schema';
import { Button } from '@web/elements';
import {
  AddJoinButtonNode,
  ColumnConfigurationNode,
  ColumnSelectionNode,
  CteNode,
  GroupByNode,
  JoinColumnNode,
  JoinNode,
  LightdashNode,
  LookbackNode,
  RollupNode,
  SelectNode,
  UnionNode,
  WhereClauseNode,
} from '@web/features/DataModeling/nodes';
import { useModelStore } from '@web/stores/useModelStore';
import {
  TutorialComponentState,
  useTutorialStore,
} from '@web/stores/useTutorialStore';
import { calculateAllowedActionTypes } from '@web/stores/utils';
import type { Edge, Node } from '@xyflow/react';
import {
  Background,
  BackgroundVariant,
  ConnectionMode,
  Controls,
  MiniMap,
  PanOnScrollMode,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from '@xyflow/react';
import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import { v4 as uuidv4 } from 'uuid';

import { ACTION_SPECS } from '../DataModeling/actionRegistry';
import { ActionsBar } from '../DataModeling/components/ActionsBar';
import { MiniMapNode } from '../DataModeling/components/MiniMapNode';
import { NavigationBar } from '../DataModeling/components/NavigationBar';
import { useLayoutManager } from '../DataModeling/hooks/useLayoutManager';
import { useNodeOperations } from '../DataModeling/hooks/useNodeOperations';
import { NODE_TYPES } from '../DataModeling/types';

type JoinWithUUID = SchemaModelFromJoinModels[0] & {
  _uuid?: string;
};

interface DataModelingProps {
  config?: {
    modelType?: string;
  };
  mode?: 'create' | 'edit';
}

const nodeTypes = {
  selectNode: SelectNode,
  joinNode: JoinNode,
  joinColumnNode: JoinColumnNode,
  rollupNode: RollupNode,
  lookbackNode: LookbackNode,
  unionNode: UnionNode,
  addJoinButtonNode: AddJoinButtonNode,
  columnSelectionNode: ColumnSelectionNode,
  columnConfigurationNode: ColumnConfigurationNode,
  lightdashNode: LightdashNode,
  whereNode: WhereClauseNode,
  groupByNode: GroupByNode,
  cteNode: CteNode,
};

const edgeTypes = {};

const DataModelingFlow: React.FC<DataModelingProps> = ({ config }) => {
  const { calculateCustomLayout, alignNodesWithCenterX } = useLayoutManager();

  const { setCenter } = useReactFlow();

  // ModelStore integration - use stable selectors
  // Data is already loaded by ModelCreate.tsx - just read from store
  const basicFieldsSource = useModelStore((state) => state.basicFields.source);
  const basicFieldsType = useModelStore((state) => state.basicFields.type);
  const modelingState = useModelStore((state) => state.modelingState);
  const setModelingState = useModelStore((state) => state.setModelingState);
  const activeActions = useModelStore((state) => state.activeActions);
  const ctes = useModelStore((state) => state.ctes);
  const navigationNodeType = useModelStore((state) => state.navigationNodeType);
  const setNavigationNodeType = useModelStore(
    (state) => state.setNavigationNodeType,
  );
  const showColumnConfiguration = useModelStore(
    (state) => state.showColumnConfiguration,
  );
  const isMinimapVisible = useModelStore((state) => state.isMinimapVisible);
  const toggleMinimap = useModelStore((state) => state.toggleMinimap);

  // Tutorial store integration - NEW: Use enum-based state
  const {
    isPlayTutorialActive,
    assistModeEnabled,
    currentAssistStepIndex,
    currentComponentState,
    tutorialActiveActions,
    tutorialNavigationNodeType,
    tutorialNavigationZoomLevel,
  } = useTutorialStore((state) => ({
    isPlayTutorialActive: state.isPlayTutorialActive,
    assistModeEnabled: state.assistModeEnabled,
    currentAssistStepIndex: state.currentAssistStepIndex,
    currentComponentState: state.currentComponentState,
    tutorialActiveActions: state.tutorialActiveActions,
    tutorialNavigationNodeType: state.tutorialNavigationNodeType,
    tutorialNavigationZoomLevel: state.tutorialNavigationZoomLevel,
  }));

  // Derived values - memoize to prevent unnecessary recalculations
  const currentModelType = useMemo(
    () => basicFieldsType || config?.modelType || '',
    [basicFieldsType, config?.modelType],
  );

  // Compute action types based on source - lightdash not applicable for staging
  const allowedActions = useMemo(
    () => calculateAllowedActionTypes(basicFieldsSource, basicFieldsType),
    [basicFieldsSource, basicFieldsType],
  );

  // Node operations hook (keeping only what we need)
  const { updateFlow, updateFlowWithMultipleSources } = useNodeOperations();

  // nodes and edges state
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);

  // Use ref to store latest nodes to avoid dependency issues
  const nodesRef = useRef<Node[]>([]);
  useEffect(() => {
    nodesRef.current = nodes;
  }, [nodes]);

  const handleNavigateToNode = useCallback(
    (nodeType: string, customZoom?: number) => {
      // Use ref to access latest nodes without adding to dependencies
      const targetNodes = nodesRef.current.filter(
        (node) => node.type === nodeType,
      );
      if (targetNodes.length > 0) {
        // For most nodes, pick the first one. For nodes that can have multiples, pick the first to start
        const targetNode = targetNodes[targetNodes.length - 1];

        // Use custom zoom if provided, otherwise default to 1
        // Lower zoom = zoomed out (see more), Higher zoom = zoomed in (see less)
        const zoomLevel = customZoom ?? 1;

        void setCenter(
          targetNode.position.x + 300,
          targetNode.position.y + 200,
          {
            zoom: zoomLevel,
            duration: 850,
          },
        );
      } else {
        console.warn(`No nodes found with type: ${nodeType}`);
      }
    },
    [setCenter],
  );

  // Create nodes and edges progressively based on modeling state
  const createNodesAndEdges = useCallback(() => {
    let currentFlow = { nodes: [] as Node[], edges: [] as Edge[] };
    const edgeStyle = {
      strokeWidth: 4,
      stroke: 'var(--color-surface-contrast)',
    };

    // EntryNode: SelectNode
    currentFlow = updateFlow(
      {
        id: 'select-1',
        type: NODE_TYPES.SELECT,
        position: { x: 100, y: 100 },
        data: {},
      },
      currentFlow.nodes,
      currentFlow.edges,
      edgeStyle,
    );
    const hasFromSource =
      modelingState.from.model ||
      modelingState.from.source ||
      modelingState.from.cte;

    if (hasFromSource && currentModelType) {
      if (currentModelType.includes('join')) {
        // Check if this is int_join_column type for special handling
        const isJoinColumnType = currentModelType === 'int_join_column';

        if (isJoinColumnType) {
          // Flow: SelectNode → JoinColumnNode (no AddJoinButtonNode needed)
          currentFlow = updateFlow(
            {
              id: 'join-column-1',
              type: 'joinColumnNode',
              position: { x: 100, y: 200 },
              data: {
                joinId: 'join-column-1',
              },
              sourceNodeId: 'select-1',
            },
            currentFlow.nodes,
            currentFlow.edges,
            edgeStyle,
          );
        } else {
          // Flow: SelectNode → AddJoinButtonNode → JoinNode(s)
          // Ancestor for all JoinNodes, all join nodes connect from this
          currentFlow = updateFlow(
            {
              id: 'add-join-button',
              type: NODE_TYPES.ADD_JOIN_BUTTON,
              position: { x: 100, y: 200 },
              data: {},
              sourceNodeId: 'select-1',
            },
            currentFlow.nodes,
            currentFlow.edges,
            edgeStyle,
          );

          // Create JoinNodes based on actual modelingState.join
          if (
            modelingState.join &&
            Array.isArray(modelingState.join) &&
            modelingState.join.length > 0
          ) {
            // Check if any joins are missing UUIDs and add them inline for rendering
            // Don't update state here to avoid infinite loops
            let needsUpdate = false;
            const joinsWithUUIDs = modelingState.join.map(
              (join: JoinWithUUID) => {
                if (!join._uuid) {
                  needsUpdate = true;
                  return { ...join, _uuid: uuidv4() };
                }
                return join;
              },
            );

            // Update the state if any UUIDs were added
            if (needsUpdate) {
              setModelingState({
                ...modelingState,
                join: joinsWithUUIDs as SchemaModelFromJoinModels,
              });
            }

            modelingState.join.forEach((join: JoinWithUUID, index: number) => {
              // Use simple sequential node ID to avoid edge connection issues
              const nodeId = `join-${index + 1}`;

              currentFlow = updateFlow(
                {
                  id: nodeId,
                  type: NODE_TYPES.JOIN,
                  position: { x: 0, y: 300 + index * 100 },
                  data: {
                    joinId: join._uuid,
                  },
                  sourceNodeId: 'add-join-button',
                },
                currentFlow.nodes,
                currentFlow.edges,
                edgeStyle,
              );
            });
          }
        }
      } else if (currentModelType.includes('rollup')) {
        // Flow: SelectNode → RollupNode
        currentFlow = updateFlow(
          {
            id: 'rollup-1',
            type: NODE_TYPES.ROLLUP,
            position: { x: 100, y: 200 },
            data: {},
            sourceNodeId: 'select-1',
          },
          currentFlow.nodes,
          currentFlow.edges,
          edgeStyle,
        );
      } else if (currentModelType.includes('lookback')) {
        // Flow: SelectNode → LookbackNode
        currentFlow = updateFlow(
          {
            id: 'lookback-1',
            type: NODE_TYPES.LOOKBACK,
            position: { x: 100, y: 200 },
            data: {},
            sourceNodeId: 'select-1',
          },
          currentFlow.nodes,
          currentFlow.edges,
          edgeStyle,
        );
      } else if (currentModelType.includes('union')) {
        // Flow: SelectNode → UnionNode
        currentFlow = updateFlow(
          {
            id: 'union-1',
            type: NODE_TYPES.UNION,
            position: { x: 100, y: 200 },
            data: {},
            sourceNodeId: 'select-1',
          },
          currentFlow.nodes,
          currentFlow.edges,
          edgeStyle,
        );
      }
    }

    // Insert CTE node between the last transform node and column selection.
    // This gives users a visual anchor to manage CTE definitions in the flow.
    if (hasFromSource && ctes.length > 0) {
      const lastTransformNode = currentFlow.nodes.find(
        (n) =>
          n.type === 'joinNode' ||
          n.type === 'joinColumnNode' ||
          n.type === 'rollupNode' ||
          n.type === 'lookbackNode' ||
          n.type === 'unionNode',
      );
      currentFlow = updateFlow(
        {
          id: 'cte-1',
          type: NODE_TYPES.CTE,
          position: { x: 100, y: 400 },
          data: {},
          sourceNodeId: lastTransformNode?.id || 'select-1',
        },
        currentFlow.nodes,
        currentFlow.edges,
        edgeStyle,
      );
    }

    // Always show columnSelectionNode when source model is set
    if (hasFromSource) {
      const cteNode = currentFlow.nodes.find((n) => n.type === 'cteNode');

      if (currentModelType.includes('join') && !cteNode) {
        if (currentModelType === 'int_join_column') {
          currentFlow = updateFlow(
            {
              id: 'column-selection',
              type: NODE_TYPES.COLUMN_SELECTION,
              position: { x: 100, y: 500 },
              data: { columns: [] },
              sourceNodeId: 'join-column-1',
            },
            currentFlow.nodes,
            currentFlow.edges,
            edgeStyle,
          );
        } else {
          const joinNodes = currentFlow.nodes.filter(
            (n) => n.type === 'joinNode',
          );
          const joinNodeIds = joinNodes.map((node) => node.id);
          currentFlow = updateFlowWithMultipleSources(
            {
              id: 'column-selection',
              type: NODE_TYPES.COLUMN_SELECTION,
              position: { x: 100, y: 500 },
              data: { columns: [] },
            },
            currentFlow.nodes,
            currentFlow.edges,
            joinNodeIds,
            edgeStyle,
          );
        }
      } else if (cteNode) {
        // When CTEs exist, column selection connects from the CTE node
        currentFlow = updateFlow(
          {
            id: 'column-selection',
            type: NODE_TYPES.COLUMN_SELECTION,
            position: { x: 100, y: 500 },
            data: { columns: [] },
            sourceNodeId: cteNode.id,
          },
          currentFlow.nodes,
          currentFlow.edges,
          edgeStyle,
        );
      } else {
        const transformationNode = currentFlow.nodes.find(
          (n) =>
            n.type === 'rollupNode' ||
            n.type === 'lookbackNode' ||
            n.type === 'unionNode',
        );
        currentFlow = updateFlow(
          {
            id: 'column-selection',
            type: NODE_TYPES.COLUMN_SELECTION,
            position: { x: 100, y: 500 },
            data: { columns: [] },
            sourceNodeId: transformationNode?.id || 'select-1',
          },
          currentFlow.nodes,
          currentFlow.edges,
          edgeStyle,
        );
      }
    }

    // Create ColumnConfigurationNode horizontally next to columnSelectionNode if showColumnConfiguration is true
    const columnNode = currentFlow.nodes.find(
      (n) => n.id === 'column-selection',
    );

    // NEW: Check enum state for Column Configuration visibility
    const isColumnConfigOpen =
      currentComponentState === TutorialComponentState.COLUMN_CONFIG_OVERVIEW ||
      currentComponentState === TutorialComponentState.COLUMN_CONFIG_TABS ||
      currentComponentState ===
        TutorialComponentState.COLUMN_CONFIG_GENERAL_TAB ||
      currentComponentState ===
        TutorialComponentState.COLUMN_CONFIG_LIGHTDASH_TAB ||
      currentComponentState ===
        TutorialComponentState.COLUMN_CONFIG_OTHERS_TAB ||
      currentComponentState === TutorialComponentState.LIGHTDASH_DIMENSION ||
      currentComponentState ===
        TutorialComponentState.LIGHTDASH_STANDARD_METRICS ||
      currentComponentState ===
        TutorialComponentState.LIGHTDASH_CUSTOM_METRICS ||
      currentComponentState ===
        TutorialComponentState.LIGHTDASH_METRICS_MERGE ||
      currentComponentState === TutorialComponentState.OTHERS_EXCLUDE_GROUPBY ||
      currentComponentState === TutorialComponentState.OTHERS_INTERVAL ||
      currentComponentState === TutorialComponentState.OTHERS_DATA_TESTS;

    const shouldShowColumnConfig = isPlayTutorialActive
      ? isColumnConfigOpen
      : showColumnConfiguration;

    if (columnNode && shouldShowColumnConfig) {
      // Add ColumnConfigurationNode to the right of ColumnSelectionNode using updateFlow
      if (!currentFlow.nodes.some((n) => n.id === 'column-configuration')) {
        // We'll set position to 0,0 initially and adjust after layout
        const result = updateFlow(
          {
            id: 'column-configuration',
            type: NODE_TYPES.COLUMN_CONFIGURATION,
            position: { x: 0, y: 0 },
            data: {},
            sourceNodeId: 'column-selection',
            sourceHandle: 'right-output',
            targetHandle: 'left-input',
          },
          currentFlow.nodes,
          currentFlow.edges,
          { ...edgeStyle, type: 'straight' },
        );
        currentFlow.nodes = result.nodes;
        currentFlow.edges = result.edges;
      }
    } else {
      // Remove ColumnConfigurationNode if shouldShowColumnConfig is false
      currentFlow.nodes = currentFlow.nodes.filter(
        (n) => n.id !== 'column-configuration',
      );
      currentFlow.edges = currentFlow.edges.filter(
        (e) =>
          !(
            e.source === 'column-selection' &&
            e.target === 'column-configuration'
          ),
      );
    }

    // Create GroupByNode / LightdashNode / WhereNode after columnSelectionNode
    if (columnNode) {
      // Use tutorial state when tutorial is active
      const effectiveActiveActions = isPlayTutorialActive
        ? tutorialActiveActions
        : activeActions;

      allowedActions.forEach((action) => {
        // Check if this action is enabled via the activeActions set
        if (effectiveActiveActions.has(action)) {
          const spec = ACTION_SPECS[action];
          if (!currentFlow.nodes.some((n) => n.id === spec.nodeId)) {
            const builtNodes = spec.buildNodes();
            currentFlow.nodes.push(...builtNodes);
          }
          spec.buildEdges().forEach((edge) => {
            if (!currentFlow.edges.some((e) => e.id === edge.id)) {
              currentFlow.edges.push(edge);
            }
          });
        }
      });
    }

    // Apply layout using layout manager - it handles all positioning including action nodes
    const layoutedNodes = calculateCustomLayout(
      currentFlow.nodes,
      currentFlow.edges,
      'TB',
    );

    // Apply custom positioning using layout manager
    const finalNodes = alignNodesWithCenterX(layoutedNodes, currentModelType);

    setNodes(finalNodes);
    setEdges(currentFlow.edges);
  }, [
    modelingState,
    allowedActions,
    activeActions,
    ctes,
    currentModelType,
    showColumnConfiguration,
    // Tutorial state
    isPlayTutorialActive,
    currentComponentState,
    tutorialActiveActions,
    // Stable functions from hooks - these don't change between renders
    handleNavigateToNode,
    calculateCustomLayout,
    alignNodesWithCenterX,
    updateFlow,
    updateFlowWithMultipleSources,
    // Note: Intentionally omitted to avoid infinite loops:
    // - setModelingState (called conditionally for UUID updates)
    // - setNodes, setEdges (React Flow setters that would cause re-renders)
  ]);

  const joinLength = Array.isArray(modelingState?.join)
    ? modelingState.join.length
    : 0;

  useEffect(() => {
    // navigate to from
    setTimeout(() => {
      handleNavigateToNode(NODE_TYPES.JOIN);
    }, 200);
  }, [handleNavigateToNode, joinLength]);

  useEffect(() => {
    // navigate to last column action or column selection if none
    const lastAction = Array.from(activeActions).pop();
    if (lastAction) {
      setTimeout(() => {
        handleNavigateToNode(
          NODE_TYPES[lastAction.toUpperCase() as keyof typeof NODE_TYPES],
        );
      }, 200);
    } else {
      setTimeout(() => {
        handleNavigateToNode(NODE_TYPES.COLUMN_SELECTION);
      }, 200);
    }
  }, [handleNavigateToNode, activeActions.size, activeActions]);

  useEffect(() => {
    // Check both model store and tutorial store for navigation
    const nodeToNavigate = navigationNodeType || tutorialNavigationNodeType;

    if (nodeToNavigate) {
      setTimeout(() => {
        // In tutorial mode (Play Tutorial OR Assist Mode OR Assist Tutorial running), use tutorial zoom level, otherwise use default
        const isTutorialMode =
          isPlayTutorialActive ||
          assistModeEnabled ||
          currentAssistStepIndex > 0;
        console.log('isTutorialMode', isTutorialMode);

        console.log('tutorialNavigationZoomLevel', tutorialNavigationZoomLevel);

        const zoomLevel = isTutorialMode
          ? tutorialNavigationZoomLevel
          : undefined;

        console.log('zoomLevel', zoomLevel);

        handleNavigateToNode(nodeToNavigate, zoomLevel);

        // Clear the navigation state after navigation
        if (navigationNodeType) {
          setNavigationNodeType(null);
        }
        if (tutorialNavigationNodeType) {
          useTutorialStore.getState().setTutorialNavigationNodeType(null);
        }
      }, 200);
    }
  }, [
    navigationNodeType,
    tutorialNavigationNodeType,
    assistModeEnabled,
    currentAssistStepIndex,
  ]);

  // Update nodes and edges when modelingState changes
  useEffect(() => {
    createNodesAndEdges();
  }, [createNodesAndEdges]);

  return (
    <div
      className="data-modeling-container"
      style={{
        width: '100%',
        height: '80vh',
      }}
      data-tutorial-id="data-modeling-container"
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        connectionMode={ConnectionMode.Strict}
        fitView
        fitViewOptions={{
          padding: 1,
          includeHiddenNodes: false,
          maxZoom: 0.8,
        }}
        defaultViewport={{ x: 0, y: 0, zoom: 0.5 }}
        proOptions={{ hideAttribution: true }}
        className="bg-background"
        selectionOnDrag={false}
        nodesDraggable={false}
        nodesConnectable={false}
        minZoom={0.5}
        maxZoom={2}
        zoomOnScroll={false}
        zoomOnPinch={true}
        panOnScroll={true}
        panOnScrollMode={PanOnScrollMode.Free}
        preventScrolling={true}
        noWheelClassName="react-flow__node-scrollable"
        onPaneClick={(e) => {
          console.log('event');
          e.stopPropagation();
        }}
      >
        <Controls
          className="bg-card border-surface text-surface-contrast shadow-sm"
          showInteractive={false}
        >
          <Button
            onClick={() => toggleMinimap(!isMinimapVisible)}
            className="react-flow__controls-button hover:bg-gray-100 transition-colors"
            title={isMinimapVisible ? 'Hide Minimap' : 'Show Minimap'}
            aria-label={isMinimapVisible ? 'Hide Minimap' : 'Show Minimap'}
            variant="iconButton"
            label=""
            icon={
              <MapIcon
                className={`h-6 w-6 ${isMinimapVisible ? 'text-blue-500' : ''}`}
              />
            }
          ></Button>
        </Controls>

        <Background
          variant={BackgroundVariant.Cross}
          gap={20}
          size={1}
          color="var(--border)"
          bgColor="transparent"
        />

        <NavigationBar nodes={nodes} currentModelType={currentModelType} />

        {/* Show ActionBar based on visibility conditions */}
        {allowedActions.length > 0 && (
          <ActionsBar actionTypes={allowedActions} />
        )}
        {isMinimapVisible && (
          <MiniMap
            nodeComponent={MiniMapNode}
            nodeStrokeWidth={2}
            maskColor="rgba(0, 0, 0, 0.1)"
            zoomable={true}
            draggable={true}
            zoomStep={0.8}
            pannable
          />
        )}
      </ReactFlow>
    </div>
  );
};

export const DataModeling: React.FC<DataModelingProps> = (props) => {
  return (
    <ReactFlowProvider>
      <DataModelingFlow {...props} />
    </ReactFlowProvider>
  );
};
