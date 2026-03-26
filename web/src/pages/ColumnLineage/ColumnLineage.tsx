import '@xyflow/react/dist/style.css';

import type { FrameworkColumn } from '@shared/framework/types';
import type {
  ColumnLineageDAGSerialized,
  ColumnLineageNode as ColumnLineageNodeData,
} from '@shared/lineage';
import SwapIcon from '@web/assets/icons/swap.svg?react';
import { useApp } from '@web/context';
import { Button } from '@web/elements/Button';
import { Message } from '@web/elements/Message';
import { Spinner } from '@web/elements/Spinner';
import { Switch } from '@web/elements/Switch';
import {
  ColumnLineageNode,
  ColumnSelectionPanel,
  ExpandNode,
  ExportDropdown,
  LineageDepthPopover,
} from '@web/features/Lineage';
import { convertDAGToReactFlow } from '@web/features/Lineage/utils';
import {
  Background,
  BackgroundVariant,
  Controls,
  type Edge,
  type Node,
  type NodeMouseHandler,
  Panel,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from '@xyflow/react';
import type { ErrorInfo } from 'react';
import { Component, useCallback, useEffect, useMemo, useState } from 'react';

/**
 * Wraps an async operation with a timeout to prevent stuck loading states.
 * If the operation takes longer than the timeout, it will reject with a timeout error.
 */
function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number = 30000,
  timeoutMessage: string = 'Operation timed out',
): Promise<T> {
  return Promise.race([
    operation,
    new Promise<never>((_, reject) => {
      setTimeout(() => reject(new Error(timeoutMessage)), timeoutMs);
    }),
  ]);
}

type StatusVariant = 'info' | 'warning' | 'error';

interface SourceTable {
  name: string;
  columnCount: number;
}

interface ColumnLineageState {
  filePath?: string;
  modelName?: string;
  columns?: FrameworkColumn[];
  selectedColumn?: string;
  dag?: ColumnLineageDAGSerialized;
  loading?: boolean;
  loadingMessage?: string;
  status?: { message: string; variant: StatusVariant };
  // Source-specific state
  isSource?: boolean;
  sourceName?: string;
  tables?: SourceTable[];
  selectedTable?: string;
}

/** Error boundary to catch and handle unhandled errors in ColumnLineage */
class ColumnLineageErrorBoundary extends Component<
  { children: React.ReactNode },
  { hasError: boolean; error?: Error }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error(
      'ColumnLineage error boundary caught an error:',
      error,
      errorInfo,
    );
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="w-full h-screen flex items-center justify-center bg-gray-50">
          <div className="max-w-md text-center">
            <div className="bg-red-50 border border-red-200 rounded-lg p-6">
              <div className="flex gap-3">
                <div className="w-6 h-6 text-red-600 flex-shrink-0">
                  <svg fill="currentColor" viewBox="0 0 20 20">
                    <path
                      fillRule="evenodd"
                      d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                      clipRule="evenodd"
                    />
                  </svg>
                </div>
                <div>
                  <h3 className="font-semibold text-red-900 mb-2">
                    Column Lineage Error
                  </h3>
                  <p className="text-sm text-red-700 mb-4">
                    An unexpected error occurred in the column lineage panel.
                    Try reloading the panel or reopening the column lineage
                    view.
                  </p>
                  <button
                    onClick={() =>
                      this.setState({ hasError: false, error: undefined })
                    }
                    className="px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700 transition-colors text-sm"
                  >
                    Try Again
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

/** Custom node types for React Flow */
const nodeTypes = {
  columnLineageNode: ColumnLineageNode,
  expandNode: ExpandNode,
} as const;

/**
 * Inner component that uses React Flow hooks.
 * Must be wrapped in ReactFlowProvider.
 */
function ColumnLineageFlow() {
  const { api } = useApp();
  const { fitView } = useReactFlow();
  const [state, setState] = useState<ColumnLineageState>({});
  const [autoRefreshEnabled, setAutoRefreshEnabled] = useState(true);
  const [isSavingPreference, setIsSavingPreference] = useState(false);
  const [showColumnPanel, setShowColumnPanel] = useState(true);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  // Track when navigating from node click to prevent column panel flash
  const [isNavigatingFromNode, setIsNavigatingFromNode] = useState(false);
  // Track pending column computation (when column is pre-selected from Model Lineage)
  const [pendingColumnCompute, setPendingColumnCompute] = useState<
    string | null
  >(null);
  // Track which expand button was clicked to zoom appropriately
  const [expandDirection, setExpandDirection] = useState<
    'upstream' | 'downstream' | null
  >(null);

  // Level settings - persisted across model/column changes
  const [upstreamLevels, setUpstreamLevels] = useState(2);
  const [downstreamLevels, setDownstreamLevels] = useState(2);
  const [isFullLineage, setIsFullLineage] = useState(false);

  // Global error handler to reset loading state on unhandled errors
  useEffect(() => {
    const handleUnhandledRejection = (event: PromiseRejectionEvent) => {
      console.error(
        'Unhandled promise rejection in ColumnLineage:',
        event.reason,
      );
      // Reset loading state if it's stuck
      setState((prev) => {
        if (prev.loading) {
          return {
            ...prev,
            loading: false,
            status: {
              message: 'An unexpected error occurred. Please try again.',
              variant: 'error',
            },
          };
        }
        return prev;
      });
    };

    const handleError = (event: ErrorEvent) => {
      console.error('Unhandled error in ColumnLineage:', event.error);
      // Reset loading state if it's stuck
      setState((prev) => {
        if (prev.loading) {
          return {
            ...prev,
            loading: false,
            status: {
              message: 'An unexpected error occurred. Please try again.',
              variant: 'error',
            },
          };
        }
        return prev;
      });
    };

    window.addEventListener('unhandledrejection', handleUnhandledRejection);
    window.addEventListener('error', handleError);

    return () => {
      window.removeEventListener(
        'unhandledrejection',
        handleUnhandledRejection,
      );
      window.removeEventListener('error', handleError);
    };
  }, []);

  // Handle messages from the extension
  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      const message = event.data;

      if (message.type === 'column-lineage-init') {
        const preSelectedColumn = message.selectedColumn;

        // Preserve level settings across model changes
        setState((prev) => ({
          ...prev,
          filePath: message.filePath,
          modelName: message.modelName,
          columns: message.columns,
          selectedColumn: preSelectedColumn
            ? preSelectedColumn
            : prev.modelName === message.modelName
              ? prev.selectedColumn
              : undefined,
          dag: preSelectedColumn
            ? undefined
            : prev.modelName === message.modelName
              ? prev.dag
              : undefined,
          status: undefined,
          // Clear source-specific state
          isSource: false,
          sourceName: undefined,
          tables: undefined,
          selectedTable: undefined,
        }));

        // If a column was pre-selected, trigger lineage computation
        if (preSelectedColumn) {
          setShowColumnPanel(false);
          setPendingColumnCompute(preSelectedColumn as string);
        } else if (
          state.modelName !== message.modelName &&
          !isNavigatingFromNode
        ) {
          // Only show column panel if switching to a different model
          // AND not navigating from a node click (which will send lineage result next)
          setShowColumnPanel(true);
        }
        return;
      }

      // Handle source file initialization
      if (message.type === 'column-lineage-source-init') {
        setState((prev) => ({
          ...prev,
          filePath: message.filePath,
          sourceName: message.sourceName,
          tables:
            typeof message.tables === 'string'
              ? JSON.parse(message.tables)
              : message.tables,
          selectedTable: message.selectedTable,
          isSource: true,
          // Clear model-specific state
          modelName: undefined,
          columns: undefined,
          selectedColumn: undefined,
          dag: undefined,
          status: undefined,
        }));
        setShowColumnPanel(true);
        return;
      }

      if (message.type === 'column-lineage-config') {
        if (typeof message.autoRefresh === 'boolean') {
          setAutoRefreshEnabled(message.autoRefresh as boolean);
        }
        return;
      }

      if (message.type === 'column-lineage-status') {
        if (message.message) {
          setState((prev) => ({
            ...prev,
            status: {
              message: message.message,
              variant: (message.variant as StatusVariant) || 'info',
            },
          }));
        } else {
          setState((prev) => ({ ...prev, status: undefined }));
        }
        return;
      }

      // Handle lineage result from node click navigation
      if (message.type === 'column-lineage-result') {
        setState((prev) => ({
          ...prev,
          selectedColumn: message.columnName,
          dag: message.dag,
          isSource: message.isSource ?? prev.isSource, // Update isSource flag
          loading: false,
          status: undefined,
        }));
        setShowColumnPanel(false);
        setIsNavigatingFromNode(false); // Reset navigation flag
      }
    };

    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [state.modelName, isNavigatingFromNode]);

  // Notify extension that webview is ready and fetch initial preferences
  useEffect(() => {
    void (async () => {
      try {
        const result = await api.post({
          type: 'framework-preferences',
          request: {
            action: 'get',
            context: 'column-lineage',
          },
        });
        if (result.success && typeof result.value === 'boolean') {
          setAutoRefreshEnabled(result.value);
        }
        // Signal that the webview is ready to receive data
        await api.post({
          type: 'framework-column-lineage',
          request: { action: 'webview-ready' },
        });
      } catch (err) {
        console.error('Failed to initialize column lineage webview', err);
      }
    })();
  }, [api]);

  // Safety mechanism: reset loading state after 5 minutes to prevent permanent stuck state
  useEffect(() => {
    if (!state.loading) return;

    const timeout = setTimeout(
      () => {
        console.warn(
          'ColumnLineage loading state was stuck for 5 minutes, resetting...',
        );
        setState((prev) => ({
          ...prev,
          loading: false,
          status: {
            message: 'Operation timed out. Please try again.',
            variant: 'error',
          },
        }));
      },
      5 * 60 * 1000,
    ); // 5 minutes

    return () => clearTimeout(timeout);
  }, [state.loading]);

  // Fetch columns when a table is pre-selected (e.g., from source node click)
  useEffect(() => {
    if (
      state.isSource &&
      state.selectedTable &&
      state.filePath &&
      !state.columns
    ) {
      // Fetch columns for the pre-selected table
      void (async () => {
        setState((prev) => ({ ...prev, loading: true }));
        try {
          const result = await withTimeout(
            api.post({
              type: 'framework-column-lineage',
              request: {
                action: 'get-source-columns',
                filePath: state.filePath,
                tableName: state.selectedTable,
              },
            }),
            15000,
            'Fetching columns timed out',
          );

          if (result.success && result.columns) {
            setState((prev) => ({
              ...prev,
              columns: result.columns,
              modelName: result.modelName,
              loading: false,
            }));
          } else {
            setState((prev) => ({
              ...prev,
              status: {
                message: result.error || 'Failed to fetch columns',
                variant: 'error',
              },
              loading: false,
            }));
          }
        } catch (err) {
          setState((prev) => ({
            ...prev,
            status: {
              message: err instanceof Error ? err.message : 'Unknown error',
              variant: 'error',
            },
            loading: false,
          }));
        }
      })();
    }
  }, [api, state.isSource, state.selectedTable, state.filePath, state.columns]);

  // Handle preference toggle
  const handlePreferenceToggle = useCallback(
    async (checked: boolean) => {
      setAutoRefreshEnabled(checked);
      setIsSavingPreference(true);
      try {
        const result = await api.post({
          type: 'framework-preferences',
          request: {
            action: 'set',
            context: 'column-lineage',
            value: checked,
          },
        });
        if (!result.success) {
          throw new Error(result.error || 'Failed to update preference');
        }
      } catch (err) {
        setState((prev) => ({
          ...prev,
          status: {
            message:
              err instanceof Error
                ? err.message
                : 'Failed to update auto-refresh preference',
            variant: 'error',
          },
        }));
        setAutoRefreshEnabled((prev) => !prev);
      } finally {
        setIsSavingPreference(false);
      }
    },
    [api],
  );

  // Handle table selection for source files
  const handleTableSelect = useCallback(
    async (tableName: string) => {
      if (!state.filePath) return;

      setState((prev) => ({
        ...prev,
        selectedTable: tableName,
        columns: undefined,
        selectedColumn: undefined,
        dag: undefined,
        loading: true,
      }));

      try {
        const result = await withTimeout(
          api.post({
            type: 'framework-column-lineage',
            request: {
              action: 'get-source-columns',
              filePath: state.filePath,
              tableName,
            },
          }),
          15000,
          'Fetching columns timed out',
        );

        if (result.success && result.columns) {
          setState((prev) => ({
            ...prev,
            columns: result.columns,
            modelName: result.modelName, // Will be "sourceName.tableName"
            loading: false,
          }));
        } else {
          setState((prev) => ({
            ...prev,
            status: {
              message: result.error || 'Failed to fetch columns',
              variant: 'error',
            },
            loading: false,
          }));
        }
      } catch (err) {
        setState((prev) => ({
          ...prev,
          status: {
            message: err instanceof Error ? err.message : 'Unknown error',
            variant: 'error',
          },
          loading: false,
        }));
      }
    },
    [api, state.filePath],
  );

  // Compute lineage with current level settings
  const computeLineage = useCallback(
    async (
      columnName: string,
      upLevels: number,
      downLevels: number,
      showLoading = true,
    ) => {
      if (!state.filePath) {
        setState((prev) => ({
          ...prev,
          status: { message: 'No file path available', variant: 'error' },
          loading: false,
        }));
        return;
      }

      if (showLoading) {
        setState((prev) => ({
          ...prev,
          selectedColumn: columnName,
          dag: undefined,
          status: undefined,
          loading: true,
        }));
      }

      try {
        // Use different action for source files
        const action = state.isSource ? 'compute-source-lineage' : 'compute';
        const result = await withTimeout(
          api.post({
            type: 'framework-column-lineage',
            request: {
              action,
              filePath: state.filePath,
              columnName: columnName,
              tableName: state.selectedTable, // Only used for source files
              upstreamLevels: state.isSource ? undefined : upLevels, // Sources don't have upstream
              downstreamLevels: downLevels,
            },
          }),
          30000,
          'Computing lineage timed out',
        );

        if (result.success && result.dag) {
          setState((prev) => ({
            ...prev,
            selectedColumn: columnName,
            dag: result.dag,
            loading: false,
            status: undefined,
          }));
        } else {
          setState((prev) => ({
            ...prev,
            status: {
              message: result.error || 'Failed to compute lineage',
              variant: 'error',
            },
            loading: false,
          }));
        }
      } catch (err) {
        setState((prev) => ({
          ...prev,
          status: {
            message: err instanceof Error ? err.message : 'Unknown error',
            variant: 'error',
          },
          loading: false,
        }));
      }
    },
    [api, state.filePath, state.isSource, state.selectedTable],
  );

  // Handle column selection
  const handleColumnSelect = useCallback(
    async (columnName: string) => {
      setShowColumnPanel(false);
      // Preserve current depth settings (full lineage or custom levels)
      const upLevels = isFullLineage ? -1 : upstreamLevels;
      const downLevels = isFullLineage ? -1 : downstreamLevels;
      await computeLineage(columnName, upLevels, downLevels);
    },
    [computeLineage, upstreamLevels, downstreamLevels, isFullLineage],
  );

  // Handle pending column computation (triggered from Model Lineage click)
  useEffect(() => {
    if (pendingColumnCompute && state.filePath) {
      const columnToCompute = pendingColumnCompute;
      setPendingColumnCompute(null);
      void handleColumnSelect(columnToCompute);
    }
  }, [pendingColumnCompute, state.filePath, handleColumnSelect]);

  // Handle custom levels apply from popover
  const handleLevelsApply = useCallback(
    async (upstream: number, downstream: number) => {
      setUpstreamLevels(upstream);
      setDownstreamLevels(downstream);
      setIsFullLineage(false);
      if (state.selectedColumn) {
        await computeLineage(state.selectedColumn, upstream, downstream);
      }
    },
    [computeLineage, state.selectedColumn],
  );

  // Handle expand upstream by 1 level (from node '+' button)
  const handleExpandUpstream = useCallback(async () => {
    if (!state.selectedColumn) return;
    const newUpstreamLevels = upstreamLevels + 1;
    setUpstreamLevels(newUpstreamLevels);
    setExpandDirection('upstream');
    await computeLineage(
      state.selectedColumn,
      newUpstreamLevels,
      downstreamLevels,
    );
  }, [computeLineage, state.selectedColumn, upstreamLevels, downstreamLevels]);

  // Handle expand downstream by 1 level (from node '+' button)
  const handleExpandDownstream = useCallback(async () => {
    if (!state.selectedColumn) return;
    const newDownstreamLevels = downstreamLevels + 1;
    setDownstreamLevels(newDownstreamLevels);
    setExpandDirection('downstream');
    await computeLineage(
      state.selectedColumn,
      upstreamLevels,
      newDownstreamLevels,
    );
  }, [computeLineage, state.selectedColumn, upstreamLevels, downstreamLevels]);

  // Convert DAG to React Flow nodes and edges (with expand nodes when not in full lineage mode)
  const { nodes, edges: baseEdges } = useMemo<{
    nodes: Node[];
    edges: Edge[];
  }>(() => {
    if (!state.dag) {
      return { nodes: [], edges: [] };
    }

    // Compute expand button visibility using backend-provided flags
    // Upstream: show if not full lineage, not source lineage, and backend says there's more
    const showUpstreamExpand =
      !isFullLineage && !state.isSource && state.dag.hasMoreUpstream;

    // Downstream: show if not full lineage and backend says there's more
    const showDownstreamExpand = !isFullLineage && state.dag.hasMoreDownstream;

    return convertDAGToReactFlow(state.dag, {
      showUpstreamExpand,
      showDownstreamExpand,
      onExpandUpstream: () => {
        void handleExpandUpstream();
      },
      onExpandDownstream: () => {
        void handleExpandDownstream();
      },
    });
  }, [
    state.dag,
    isFullLineage,
    state.isSource,
    handleExpandUpstream,
    handleExpandDownstream,
  ]);

  // Apply hover highlighting to edges
  const edges = useMemo<Edge[]>(() => {
    if (!hoveredNodeId) {
      return baseEdges;
    }

    return baseEdges.map((edge) => {
      const isConnected =
        edge.source === hoveredNodeId || edge.target === hoveredNodeId;

      return {
        ...edge,
        style: {
          ...edge.style,
          stroke: isConnected
            ? 'var(--color-primary)'
            : 'var(--color-border-contrast)',
          strokeWidth: isConnected ? 2.5 : 1,
          opacity: isConnected ? 1 : 0.3,
          transition: 'stroke 0.15s, stroke-width 0.15s, opacity 0.15s',
        },
        zIndex: isConnected ? 1000 : 0,
      };
    });
  }, [baseEdges, hoveredNodeId]);

  // Fit view when DAG changes
  useEffect(() => {
    if (nodes.length > 0) {
      // Small delay to ensure nodes are rendered
      const timer = setTimeout(() => {
        if (expandDirection) {
          // Zoom to specific nodes based on expand direction
          let targetNodes: Node[] = [];

          if (expandDirection === 'upstream') {
            // Find root nodes (leftmost/upstream)
            // Root nodes are those that are in dag.roots or are expand-upstream nodes
            targetNodes = nodes.filter(
              (node) =>
                node.id === 'expand-upstream' ||
                (state.dag?.roots.includes(node.id) ?? false),
            );
          } else if (expandDirection === 'downstream') {
            // Find leaf nodes (rightmost/downstream)
            // Leaf nodes are those that are in dag.leaves or are expand-downstream nodes
            targetNodes = nodes.filter(
              (node) =>
                node.id === 'expand-downstream' ||
                (state.dag?.leaves.includes(node.id) ?? false),
            );
          }

          if (targetNodes.length > 0) {
            void fitView({
              padding: 0.2,
              duration: 200,
              nodes: targetNodes,
            });
          } else {
            // Fallback to normal fitView if no specific nodes found
            void fitView({ padding: 0.2, duration: 200 });
          }

          // Reset expand direction after zooming
          setExpandDirection(null);
        } else {
          // Normal fitView for non-expand changes
          void fitView({ padding: 0.2, duration: 200 });
        }
      }, 50);
      return () => clearTimeout(timer);
    }
  }, [nodes, fitView, expandDirection, state.dag]);

  // Handle Full Lineage from popover
  const handleFullLineage = useCallback(async () => {
    if (!state.selectedColumn || !state.filePath) return;

    setIsFullLineage(true);
    setState((prev) => ({ ...prev, loading: true }));

    try {
      // Use different action for source files
      const action = state.isSource ? 'compute-source-lineage' : 'compute';
      const result = await withTimeout(
        api.post({
          type: 'framework-column-lineage',
          request: {
            action,
            filePath: state.filePath,
            columnName: state.selectedColumn,
            tableName: state.selectedTable, // Only used for source files
            upstreamLevels: state.isSource ? undefined : -1, // Sources don't have upstream
            downstreamLevels: -1, // Unlimited
          },
        }),
        60000,
        'Computing full lineage timed out',
      );

      if (result.success && result.dag) {
        setState((prev) => ({
          ...prev,
          dag: result.dag,
          loading: false,
          status: undefined,
        }));
      } else {
        setState((prev) => ({
          ...prev,
          loading: false,
          status: {
            message: result.error || 'Failed to compute full lineage',
            variant: 'error',
          },
        }));
      }
    } catch (err) {
      setState((prev) => ({
        ...prev,
        loading: false,
        status: {
          message:
            err instanceof Error
              ? err.message
              : 'Failed to compute full lineage',
          variant: 'error',
        },
      }));
    }
  }, [
    api,
    state.selectedColumn,
    state.filePath,
    state.isSource,
    state.selectedTable,
  ]);

  // Handle export (current column or all columns)
  const handleExport = useCallback(
    async (exportAll: boolean) => {
      if (!state.filePath) return;
      if (!exportAll && !state.selectedColumn) return;
      if (state.isSource && !state.selectedTable) return;

      setState((prev) => ({
        ...prev,
        loading: true,
        loadingMessage: 'Exporting CSV...',
      }));

      try {
        const action = state.isSource
          ? 'export-source-lineage'
          : 'export-lineage';
        const result = await withTimeout(
          api.post({
            type: 'framework-column-lineage',
            request: {
              action,
              filePath: state.filePath,
              columnName: exportAll ? undefined : state.selectedColumn,
              tableName: state.selectedTable,
            },
          }),
          30000,
          'Export operation timed out',
        );

        if (result.success && result.csvContent && result.suggestedFilename) {
          await api.post({
            type: 'framework-column-lineage',
            request: {
              action: 'save-csv',
              csvContent: result.csvContent,
              suggestedFilename: result.suggestedFilename,
            },
          });
        } else {
          setState((prev) => ({
            ...prev,
            status: {
              message: result.error || 'Failed to export lineage',
              variant: 'error',
            },
          }));
        }
      } catch (err) {
        setState((prev) => ({
          ...prev,
          status: {
            message:
              err instanceof Error ? err.message : 'Failed to export lineage',
            variant: 'error',
          },
        }));
      } finally {
        setState((prev) => ({
          ...prev,
          loading: false,
          loadingMessage: undefined,
        }));
      }
    },
    [
      api,
      state.filePath,
      state.selectedColumn,
      state.isSource,
      state.selectedTable,
    ],
  );

  // Handle node click - open model/source file and switch to that column's lineage
  const handleNodeClick: NodeMouseHandler = useCallback(
    (_event, node) => {
      // Skip expand nodes (they have their own onClick handlers)
      if (node.type === 'expandNode') {
        return;
      }

      const nodeData = node.data.data as ColumnLineageNodeData;

      // Skip if no file path available
      if (!nodeData?.filePath) {
        return;
      }

      // Set flag to prevent column panel flash during navigation
      setIsNavigatingFromNode(true);

      // Handle source nodes differently - show forward lineage
      if (nodeData.modelLayer === 'source') {
        // Extract table name from modelName (format: "schema.table")
        const tableName = nodeData.modelName.split('.').pop() || '';

        void api.post({
          type: 'framework-column-lineage',
          request: {
            action: 'switch-to-source-column',
            filePath: nodeData.filePath,
            columnName: nodeData.columnName,
            tableName,
            downstreamLevels: isFullLineage ? -1 : downstreamLevels,
          },
        });
        return;
      }

      // Send message to extension to switch to this column in its model
      // Preserve current lineage mode (full vs custom levels)
      void api.post({
        type: 'framework-column-lineage',
        request: {
          action: 'switch-to-model-column',
          filePath: nodeData.filePath,
          columnName: nodeData.columnName,
          upstreamLevels: isFullLineage ? -1 : upstreamLevels,
          downstreamLevels: isFullLineage ? -1 : downstreamLevels,
        },
      });
    },
    [api, isFullLineage, upstreamLevels, downstreamLevels],
  );

  // Convert columns for the panel
  const panelColumns = useMemo(() => {
    return (
      state.columns?.map((col) => ({
        name: col.name,
        data_type: col.data_type,
        description: col.description,
      })) ?? []
    );
  }, [state.columns]);

  // Determine if we should show the column panel
  const hasColumns = panelColumns.length > 0;
  const hasTables = (state.tables?.length ?? 0) > 0;
  const hasDag = nodes.length > 0;

  // For sources, we need a table selected before showing columns
  const showTableSelector = state.isSource && hasTables && !state.selectedTable;
  const showColumnSelector =
    hasColumns && showColumnPanel && !showTableSelector;

  // Fallback UI for unexpected state combinations
  const hasUnexpectedState =
    state.loading &&
    !state.filePath &&
    !state.selectedColumn &&
    !state.dag &&
    !state.columns &&
    !state.status;

  if (hasUnexpectedState) {
    return (
      <div className="w-full h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center max-w-md">
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
            <div className="flex gap-3">
              <div className="w-6 h-6 text-yellow-600 flex-shrink-0">
                <svg fill="currentColor" viewBox="0 0 20 20">
                  <path
                    fillRule="evenodd"
                    d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z"
                    clipRule="evenodd"
                  />
                </svg>
              </div>
              <div>
                <h3 className="font-semibold text-yellow-900 mb-2">
                  Unexpected State
                </h3>
                <p className="text-sm text-yellow-700 mb-4">
                  The column lineage panel is in an unexpected state. This may
                  be due to an error during initialization.
                </p>
                <button
                  onClick={() => {
                    // Reset to clean state
                    setState({});
                    setShowColumnPanel(false);
                    setHoveredNodeId(null);
                    setPendingColumnCompute(null);
                    setIsNavigatingFromNode(false);
                    // Try to reinitialize
                    void (async () => {
                      try {
                        await api.post({
                          type: 'framework-column-lineage',
                          request: { action: 'webview-ready' },
                        });
                      } catch (err) {
                        console.error(
                          'Failed to reinitialize column lineage:',
                          err,
                        );
                      }
                    })();
                  }}
                  className="px-4 py-2 bg-yellow-600 text-white rounded hover:bg-yellow-700 transition-colors text-sm"
                >
                  Reset Panel
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="w-full h-screen flex flex-col font-sans">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: true }}
        className="flex-1"
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        panOnScroll
        zoomOnScroll
        onNodeClick={handleNodeClick}
        onNodeMouseEnter={(_event, node) => setHoveredNodeId(node.id)}
        onNodeMouseLeave={() => setHoveredNodeId(null)}
      >
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
        <Controls
          showInteractive={false}
          className="!bg-card !border-neutral !rounded"
        />

        {/* Header panel with controls */}
        <Panel
          position="top-left"
          className="p-2 bg-card rounded border border-neutral flex items-center gap-4"
        >
          {/* Table selector for sources */}
          {state.isSource && hasTables && (
            <>
              <select
                value={state.selectedTable || ''}
                onChange={(e) => {
                  void handleTableSelect(e.target.value);
                }}
                className="bg-surface text-surface-contrast text-xs px-2 py-1 rounded border border-neutral focus:outline-none focus:border-primary"
              >
                <option value="" disabled>
                  Select table...
                </option>
                {state.tables?.map((table) => (
                  <option key={table.name} value={table.name}>
                    {table.name} ({table.columnCount} cols)
                  </option>
                ))}
              </select>
              <div className="h-4 w-px border-l border-neutral" />
            </>
          )}

          {/* Column selector button */}
          <Button
            variant="iconButton"
            label={state.selectedColumn ? 'Change Column' : 'Select Column'}
            icon={<SwapIcon className="w-4 h-4" />}
            onClick={() => setShowColumnPanel(true)}
            disabled={!hasColumns || (state.isSource && !state.selectedTable)}
            className="!p-0"
          />

          {/* Vertical divider */}
          <div className="h-4 w-px border-l border-neutral" />

          <Switch
            checked={autoRefreshEnabled}
            onChange={(checked) => {
              const value =
                typeof checked === 'boolean' ? checked : checked.target.checked;
              void handlePreferenceToggle(value);
            }}
            position="right"
            disabled={isSavingPreference}
            label="Auto-sync on model change"
            className="text-xs"
            size="sm"
          />

          {/* Lineage depth popover - only when a column is selected */}
          <div className="h-4 w-px border-l border-neutral" />
          <LineageDepthPopover
            upstreamLevels={upstreamLevels}
            downstreamLevels={downstreamLevels}
            isFullLineage={isFullLineage}
            disabled={state.loading || !state.selectedColumn}
            onApply={(upstream, downstream) => {
              void handleLevelsApply(upstream, downstream);
            }}
            onFullLineage={() => {
              void handleFullLineage();
            }}
          />

          {/* Export dropdown */}
          <div className="h-4 w-px border-l border-neutral" />
          <ExportDropdown
            disabled={state.loading || !hasColumns}
            options={[
              {
                id: 'current',
                label: 'Export Current Column',
                disabled: !state.selectedColumn,
                onClick: () => {
                  void handleExport(false);
                },
              },
              {
                id: 'all',
                label: 'Export All Columns',
                onClick: () => {
                  void handleExport(true);
                },
              },
            ]}
          />
        </Panel>

        {/* Status/error banner - always top-right */}
        {!state.loading && state.status && (
          <Panel position="top-right" className="!m-4">
            <Message
              variant={state.status.variant === 'error' ? 'error' : 'info'}
              className="!p-3 text-sm"
            >
              {state.status.message}
            </Message>
          </Panel>
        )}

        {/* Guidance message - center, only when no DAG */}
        {!hasDag && !state.loading && !state.status && (
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 text-center text-sm pointer-events-none text-surface-contrast">
            {state.isSource && !state.selectedTable
              ? 'Select a table from the dropdown above'
              : hasColumns
                ? 'Choose column to see its lineage'
                : state.filePath
                  ? 'No columns available for this model'
                  : 'Open a .model.json, .source.json, .sql, or .yml file to view column lineage'}
          </div>
        )}

        {/* Loading indicator */}
        {state.loading && (
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 pointer-events-none ">
            <Spinner
              label={state.loadingMessage || 'Computing lineage...'}
              inline
            />
          </div>
        )}

        {/* Column selection panel */}
        {showColumnSelector && (
          <ColumnSelectionPanel
            modelName={
              state.isSource
                ? `${state.sourceName}.${state.selectedTable}`
                : state.modelName
            }
            columns={panelColumns}
            selectedColumn={state.selectedColumn}
            onColumnSelect={(col) => {
              void handleColumnSelect(col);
            }}
            onClose={() => setShowColumnPanel(false)}
          />
        )}
      </ReactFlow>
    </div>
  );
}

/**
 * Column Lineage page component.
 * Displays a React Flow canvas with column selection panel.
 */
export function ColumnLineage() {
  return (
    <ColumnLineageErrorBoundary>
      <ReactFlowProvider>
        <ColumnLineageFlow />
      </ReactFlowProvider>
    </ColumnLineageErrorBoundary>
  );
}
