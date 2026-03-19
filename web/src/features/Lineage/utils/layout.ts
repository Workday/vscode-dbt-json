import dagre from '@dagrejs/dagre';
import type { ColumnLineageDAGSerialized } from '@shared/lineage';
import type { Edge, Node } from '@xyflow/react';

/**
 * Fixed node dimensions matching ColumnLineageNode component.
 * w-96 = 384px, height fixed assuming 2-line model names (line-clamp-2)
 */
const NODE_WIDTH = 384;
const NODE_HEIGHT = 128;

/** Expand node dimensions (smaller than regular nodes) */
const EXPAND_NODE_SIZE = 64;

/** Style for expand node edges (dashed line) */
const EXPAND_EDGE_STYLE = {
  stroke: 'var(--color-border-contrast)',
  strokeWidth: 1.5,
  strokeDasharray: '5,5',
} as const;

/** Options for expand node injection */
export interface ExpandNodeOptions {
  /** Whether to show upstream expand node */
  showUpstreamExpand: boolean;
  /** Whether to show downstream expand node */
  showDownstreamExpand: boolean;
  /** Callback when upstream expand is clicked */
  onExpandUpstream: () => void;
  /** Callback when downstream expand is clicked */
  onExpandDownstream: () => void;
}

/**
 * Convert column lineage DAG to React Flow nodes and edges with dagre layout.
 * Sources are forced to rank 0 (leftmost column).
 * Optionally injects expand nodes at boundaries.
 */
export function convertDAGToReactFlow(
  dag: ColumnLineageDAGSerialized,
  expandOptions?: ExpandNodeOptions,
): {
  nodes: Node[];
  edges: Edge[];
} {
  if (!dag.nodes.length) {
    return { nodes: [], edges: [] };
  }

  // Create dagre graph
  const dagreGraph = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));

  // Configure for left-to-right layout with generous spacing
  dagreGraph.setGraph({
    rankdir: 'LR',
    nodesep: 80, // Vertical spacing between nodes
    ranksep: 150, // Horizontal spacing between ranks/columns
    marginx: 40,
    marginy: 40,
  });

  // Add nodes to dagre with fixed dimensions
  dag.nodes.forEach((node) => {
    dagreGraph.setNode(node.id, {
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
      // Force source nodes to rank 0
      ...(node.modelLayer === 'source' ? { rank: 0 } : {}),
    });
  });

  // Add edges to dagre
  dag.edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  // Use expand options directly (business logic is handled by the caller)
  const hasUpstreamExpand =
    expandOptions?.showUpstreamExpand && dag.roots.length > 0;
  const hasDownstreamExpand =
    expandOptions?.showDownstreamExpand && dag.leaves.length > 0;

  // Add expand nodes to dagre if needed
  if (hasUpstreamExpand) {
    dagreGraph.setNode('expand-upstream', {
      width: EXPAND_NODE_SIZE,
      height: EXPAND_NODE_SIZE,
      rank: -1, // Force to leftmost position
    });
    dag.roots.forEach((rootId) => {
      dagreGraph.setEdge('expand-upstream', rootId);
    });
  }

  if (hasDownstreamExpand) {
    dagreGraph.setNode('expand-downstream', {
      width: EXPAND_NODE_SIZE,
      height: EXPAND_NODE_SIZE,
    });
    dag.leaves.forEach((leafId) => {
      dagreGraph.setEdge(leafId, 'expand-downstream');
    });
  }

  // Run dagre layout
  dagre.layout(dagreGraph);

  // Convert to React Flow nodes
  const nodes: Node[] = dag.nodes.map((node) => {
    const dagreNode = dagreGraph.node(node.id);
    const isTarget = node.id === dag.targetColumn;

    return {
      id: node.id,
      type: 'columnLineageNode',
      position: {
        // Dagre positions are center-based, React Flow uses top-left
        x: dagreNode.x - NODE_WIDTH / 2,
        y: dagreNode.y - NODE_HEIGHT / 2,
      },
      data: { data: node, isTarget },
    };
  });

  // Add expand nodes to the nodes array
  if (hasUpstreamExpand) {
    const upstreamDagreNode = dagreGraph.node('expand-upstream');
    nodes.push({
      id: 'expand-upstream',
      type: 'expandNode',
      position: {
        x: upstreamDagreNode.x - EXPAND_NODE_SIZE / 2,
        y: upstreamDagreNode.y - EXPAND_NODE_SIZE / 2,
      },
      data: {
        direction: 'upstream',
        onClick: expandOptions.onExpandUpstream,
      },
    });
  }

  if (hasDownstreamExpand) {
    const downstreamDagreNode = dagreGraph.node('expand-downstream');
    nodes.push({
      id: 'expand-downstream',
      type: 'expandNode',
      position: {
        x: downstreamDagreNode.x - EXPAND_NODE_SIZE / 2,
        y: downstreamDagreNode.y - EXPAND_NODE_SIZE / 2,
      },
      data: {
        direction: 'downstream',
        onClick: expandOptions.onExpandDownstream,
      },
    });
  }

  // Convert to React Flow edges using bezier curves for better visual separation
  const edges: Edge[] = dag.edges.map((edge, index) => ({
    id: `edge-${index}-${edge.source}-${edge.target}`,
    source: edge.source,
    target: edge.target,
    type: 'default', // Bezier curve
    animated: false,
    style: {
      stroke: 'var(--color-border-contrast)',
      strokeWidth: 1.5,
    },
  }));

  // Add edges for expand nodes
  if (hasUpstreamExpand) {
    dag.roots.forEach((rootId) => {
      edges.push({
        id: `edge-expand-upstream-${rootId}`,
        source: 'expand-upstream',
        target: rootId,
        type: 'default',
        animated: false,
        style: EXPAND_EDGE_STYLE,
      });
    });
  }

  if (hasDownstreamExpand) {
    dag.leaves.forEach((leafId) => {
      edges.push({
        id: `edge-expand-downstream-${leafId}`,
        source: leafId,
        target: 'expand-downstream',
        type: 'default',
        animated: false,
        style: EXPAND_EDGE_STYLE,
      });
    });
  }

  return { nodes, edges };
}
