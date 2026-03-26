import {
  CircleStackIcon,
  ClockIcon,
  Cog6ToothIcon,
  CubeIcon,
  FunnelIcon,
  PlusCircleIcon,
  RectangleGroupIcon,
  SparklesIcon,
  TableCellsIcon,
} from '@heroicons/react/24/outline';
import type { MiniMapNodeProps } from '@xyflow/react';
import React from 'react';

// Unified color for all nodes
const MINIMAP_NODE_COLOR = '#E6E6E6';

// Map node types to their icons and labels
const nodeConfig: Record<
  string,
  {
    icon: React.ComponentType<{ className?: string }>;
    label: string;
  }
> = {
  selectNode: {
    icon: CircleStackIcon,
    label: 'Select',
  },
  joinNode: {
    icon: TableCellsIcon,
    label: 'Join',
  },
  joinColumnNode: {
    icon: CircleStackIcon,
    label: 'Join Column',
  },
  rollupNode: {
    icon: CubeIcon,
    label: 'Rollup',
  },
  lookbackNode: {
    icon: ClockIcon,
    label: 'Lookback',
  },
  unionNode: {
    icon: RectangleGroupIcon,
    label: 'Union',
  },
  columnSelectionNode: {
    icon: TableCellsIcon,
    label: 'Columns',
  },
  columnConfigurationNode: {
    icon: Cog6ToothIcon,
    label: 'Config',
  },
  lightdashNode: {
    icon: SparklesIcon,
    label: 'Lightdash',
  },
  whereNode: {
    icon: FunnelIcon,
    label: 'Where',
  },
  groupByNode: {
    icon: RectangleGroupIcon,
    label: 'Group By',
  },
  addJoinButtonNode: {
    icon: PlusCircleIcon,
    label: 'Add Join',
  },
};

// Helper function to determine node type from node ID
const getNodeTypeFromId = (id: string): string => {
  // Map common ID patterns to node types
  const idPatterns: Record<string, string> = {
    'select-': 'selectNode',
    'join-': 'joinNode',
    'join-column': 'joinColumnNode',
    rollup: 'rollupNode',
    lookback: 'lookbackNode',
    union: 'unionNode',
    'column-selection': 'columnSelectionNode',
    'column-configuration': 'columnConfigurationNode',
    lightdash: 'lightdashNode',
    where: 'whereNode',
    'group-by': 'groupByNode',
    'add-join': 'addJoinButtonNode',
  };

  // Find the first matching pattern
  for (const [pattern, nodeType] of Object.entries(idPatterns)) {
    if (id.includes(pattern)) {
      return nodeType;
    }
  }

  return 'default';
};

export const MiniMapNode: React.FC<MiniMapNodeProps> = ({
  x,
  y,
  width,
  height,
  style,
  ...nodeProps
}) => {
  const nodeId = (nodeProps as { id?: string }).id || '';
  const nodeType = getNodeTypeFromId(nodeId);
  const config = nodeConfig[nodeType];

  if (!config) {
    // Fallback for unknown node types
    return (
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={MINIMAP_NODE_COLOR}
        fillOpacity="0.3"
        stroke={MINIMAP_NODE_COLOR}
        strokeWidth="2"
        rx="4"
        style={style}
      />
    );
  }

  const Icon = config.icon;
  // Adjust icon size and positioning to make room for label
  const iconSize = Math.min(width, height) * 0.4;
  const iconX = x + (width - iconSize) / 2;
  const iconY = y + (height - iconSize) / 2 - 6; // Move icon up slightly for label

  // Calculate font size: proportional to height but with min/max bounds
  const fontSize = Math.max(6, Math.min(75, height * 5.15));

  return (
    <g>
      {/* Background rect with uniform color */}
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={MINIMAP_NODE_COLOR}
        fillOpacity="0.3"
        stroke={MINIMAP_NODE_COLOR}
        strokeWidth="2"
        rx="4"
      />

      {/* Icon - render directly as SVG path */}
      <g transform={`translate(${iconX}, ${iconY})`}>
        <svg
          width={iconSize}
          height={iconSize}
          viewBox="0 0 24 24"
          fill="none"
          stroke={MINIMAP_NODE_COLOR}
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <Icon />
        </svg>
      </g>

      {/* Label - always show */}
      <text
        x={x + width / 2}
        y={y + height - fontSize / 2}
        fill="black"
        fontSize={fontSize}
        fontWeight="600"
        textAnchor="middle"
        style={{ pointerEvents: 'none', userSelect: 'none' }}
      >
        {config.label}
      </text>
    </g>
  );
};
