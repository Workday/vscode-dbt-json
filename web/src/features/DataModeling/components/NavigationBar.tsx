import {
  ArrowPathIcon,
  ChartBarIcon,
  ClockIcon,
  CubeIcon,
  FunnelIcon,
  LinkIcon,
  RectangleGroupIcon,
  Square3Stack3DIcon,
  TableCellsIcon,
} from '@heroicons/react/24/solid';
import { Button, Icon } from '@web/elements';
import { mapNodeTypeToGuideStep } from '@web/features/Tutorial/config/assistMe/assistSteps';
import { useTutorialStore } from '@web/stores/useTutorialStore';
import { Panel, useReactFlow } from '@xyflow/react';
import React, { useCallback } from 'react';

import { MODEL_TYPES } from '../types';
import { HoverTooltip } from './HoverTooltip';

interface NavigationItem {
  id: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  nodeType: string;
}

interface NavigationBarProps {
  nodes: Array<{
    id: string;
    type?: string;
    position: { x: number; y: number };
  }>;
  currentModelType?: string | null;
}

export const NavigationBar: React.FC<NavigationBarProps> = ({
  nodes,
  currentModelType,
}) => {
  const { setCenter } = useReactFlow();

  // Tutorial store integration for self-exploratory guides
  const {
    assistModeEnabled,
    currentAssistStepIndex,
    setDataModelingGuideStep,
  } = useTutorialStore((state) => ({
    assistModeEnabled: state.assistModeEnabled,
    currentAssistStepIndex: state.currentAssistStepIndex,
    setDataModelingGuideStep: state.setDataModelingGuideStep,
  }));

  const getTransformationNode = useCallback((): NavigationItem | null => {
    if (!currentModelType) return null;

    if (currentModelType.includes(MODEL_TYPES.JOIN)) {
      // Check if it's int_join_column type for special handling
      const isJoinColumnType = currentModelType === 'int_join_column';

      return {
        id: MODEL_TYPES.JOIN,
        label: isJoinColumnType ? 'Cross Join Unnest' : 'Join',
        icon: LinkIcon,
        nodeType: isJoinColumnType ? 'joinColumnNode' : 'joinNode',
      };
    }

    if (currentModelType.includes(MODEL_TYPES.ROLLUP)) {
      return {
        id: MODEL_TYPES.ROLLUP,
        label: 'Rollup',
        icon: ArrowPathIcon,
        nodeType: 'rollupNode',
      };
    }

    if (currentModelType.includes(MODEL_TYPES.LOOKBACK)) {
      return {
        id: MODEL_TYPES.LOOKBACK,
        label: 'Lookback',
        icon: ClockIcon,
        nodeType: 'lookbackNode',
      };
    }

    if (currentModelType.includes(MODEL_TYPES.UNION)) {
      return {
        id: MODEL_TYPES.UNION,
        label: 'Union',
        icon: Square3Stack3DIcon,
        nodeType: 'unionNode',
      };
    }

    return null;
  }, [currentModelType]);

  // Only show CTE navigation for model types that support inline CTE definitions
  const supportsCtes =
    currentModelType &&
    (currentModelType.includes('int_select_model') ||
      currentModelType.includes('int_join_models') ||
      currentModelType.includes('mart_select_model') ||
      currentModelType.includes('mart_join_models'));

  const navigationItems: NavigationItem[] = [
    {
      id: 'model',
      label: 'Model',
      icon: CubeIcon,
      nodeType: 'selectNode',
    },
    ...(supportsCtes
      ? [
          {
            id: MODEL_TYPES.CTE,
            label: 'CTEs',
            icon: Square3Stack3DIcon,
            nodeType: 'cteNode',
          },
        ]
      : []),
    ...(getTransformationNode() ? [getTransformationNode()!] : []),
    {
      id: 'columns',
      label: 'Columns',
      icon: TableCellsIcon,
      nodeType: 'columnSelectionNode',
    },
    {
      id: 'lightdash',
      label: 'Lightdash',
      icon: ChartBarIcon,
      nodeType: 'lightdashNode',
    },
    {
      id: 'groupby',
      label: 'Group By',
      icon: RectangleGroupIcon,
      nodeType: 'groupByNode',
    },
    {
      id: 'where',
      label: 'Where',
      icon: FunnelIcon,
      nodeType: 'whereNode',
    },
  ];

  const handleNavigateToNode = useCallback(
    (nodeType: string) => {
      const targetNode = nodes.find((node) => node.type === nodeType);
      if (targetNode) {
        void setCenter(
          targetNode.position.x + 200,
          targetNode.position.y + 100,
          {
            zoom: 1,
            duration: 800,
          },
        );

        // Show guide if assist mode is enabled OR assist tutorial is running
        const isAssistActive = assistModeEnabled || currentAssistStepIndex > 0;
        if (isAssistActive) {
          const guideStep = mapNodeTypeToGuideStep(nodeType);
          setDataModelingGuideStep(guideStep);
        }
      }
    },
    [
      nodes,
      setCenter,
      assistModeEnabled,
      currentAssistStepIndex,
      setDataModelingGuideStep,
    ],
  );

  const isNodeCompleted = useCallback(
    (nodeType: string) => {
      const hasNode = nodes.some((node) => node.type === nodeType);

      if (currentModelType) {
        if (nodeType === 'joinNode') {
          return currentModelType.includes(MODEL_TYPES.JOIN) && hasNode;
        }
        if (nodeType === 'rollupNode') {
          return currentModelType.includes(MODEL_TYPES.ROLLUP) && hasNode;
        }
        if (nodeType === 'lookbackNode') {
          return currentModelType.includes(MODEL_TYPES.LOOKBACK) && hasNode;
        }
        if (nodeType === 'unionNode') {
          return currentModelType.includes(MODEL_TYPES.UNION) && hasNode;
        }
      }

      return hasNode;
    },
    [nodes, currentModelType],
  );

  const completedItems = navigationItems.filter((item) =>
    isNodeCompleted(item.nodeType),
  );

  return (
    <Panel position="center-left" style={{ top: '55%' }}>
      <div
        className="bg-card border border-surface rounded-sm shadow-sm"
        data-tutorial-id="navigation-bar"
      >
        {/* Title */}
        <div className="px-2 py-2 border-b border-surface bg-primary/10 flex items-center justify-center">
          <HoverTooltip
            content="Navigation"
            description="Quick jump to different sections of your model"
            placement="right"
          >
            <Icon name="compass" className="w-4 h-4 text-background-contrast" />
          </HoverTooltip>
        </div>

        {/* Navigation Items */}
        <div className="flex flex-col">
          {completedItems.map((item) => {
            const isCompleted = isNodeCompleted(item.nodeType);

            return (
              <HoverTooltip
                key={item.id}
                content={`Navigate to ${item.label}`}
                disabled={!isCompleted}
                placement="right"
              >
                <Button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    e.preventDefault();
                    handleNavigateToNode(item.nodeType);
                  }}
                  variant="iconButton"
                  label=""
                  disabled={!isCompleted}
                  className={`
                    flex flex-col items-center justify-center py-2 px-1 transition-all duration-200 group w-full
                    ${
                      isCompleted
                        ? 'cursor-pointer hover:bg-surface/50'
                        : 'cursor-not-allowed opacity-50'
                    }
                  `}
                  icon={
                    <item.icon
                      className={`w-4 h-4 transition-colors ${
                        isCompleted
                          ? 'text-background-contrast group-hover:text-primary'
                          : 'text-muted-foreground'
                      }`}
                    />
                  }
                />
              </HoverTooltip>
            );
          })}
        </div>
      </div>
    </Panel>
  );
};
