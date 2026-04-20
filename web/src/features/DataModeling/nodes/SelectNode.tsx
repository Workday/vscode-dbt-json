import { CircleStackIcon } from '@heroicons/react/24/outline';
import type { DbtProject } from '@shared/dbt/types';
import DataSearchIcon from '@web/assets/icons/data-search.svg?react';
import { useApp } from '@web/context';
import { Button, SelectSingle, Tooltip } from '@web/elements';
import { type SchemaSelect, useModelStore } from '@web/stores/useModelStore';
import { useTutorialStore } from '@web/stores/useTutorialStore';
import {
  filterAvailableModels,
  getUsedModelsForSelect,
} from '@web/utils/dataModeling';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';

import { ModelColumns } from '../components/ModelColumns';
import type { Column, SelectionType, SelectionTypeValues } from '../types';
import { supportsColumnName, supportsExprOnly } from '../types';
import { extractColumnsFromNode } from '../utils/manifestColumns';
import { buildUpdatedSelections } from '../utils/selectionUtils';

export interface AvailableModel {
  label: string;
  value: string;
}

export type { Column };

export const SelectNode: React.FC<NodeProps> = ({ data: _data }) => {
  const { api } = useApp();

  const { modelingState, updateFromState, updateSelectState, basicFields } =
    useModelStore();

  // Tutorial integration
  const { isPlayTutorialActive } = useTutorialStore((state) => ({
    isPlayTutorialActive: state.isPlayTutorialActive,
  }));

  // Ref for programmatically opening dropdown in tutorial
  const selectWrapperRef = useRef<HTMLDivElement>(null);

  const isTypeSource =
    (basicFields.type as string) === 'stg_select_source' ||
    (basicFields.type as string) === 'stg_union_sources';

  const [selectedModel, setSelectedModel] = useState<AvailableModel | null>(
    null,
  );
  const [models, setModels] = useState<string[]>([]);
  const [modelsLoading, setModelsLoading] = useState<boolean>(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [columns, setColumns] = useState<Column[]>([]);
  const [currentProject, setCurrentProject] = useState<DbtProject | null>(null);

  // Get list of already selected models to exclude from dropdown
  const usedModels = useMemo(
    () => getUsedModelsForSelect(modelingState),
    [modelingState],
  );

  const modelOptions = useMemo(() => {
    const storedIdentifier = isTypeSource
      ? modelingState.from?.source
      : modelingState.from?.model || modelingState.from?.cte || undefined;

    const currentSelectedModel = selectedModel?.value || storedIdentifier;

    const currentSelections: string[] = currentSelectedModel
      ? [currentSelectedModel as string]
      : [];

    return filterAvailableModels(models, usedModels, currentSelections);
  }, [
    models,
    usedModels,
    selectedModel,
    isTypeSource,
    modelingState.from.model,
    modelingState.from.source,
    modelingState.from.cte,
  ]);

  // Memoize the default value - only depend on selectedModel, not modelingState.select
  // This prevents the defaultValue from changing when other components update the store
  const columnDefaultValue = useMemo(() => {
    if (selectedModel) {
      const modelType = basicFields.type;

      // For model types that support SchemaColumnName (plain strings),
      if (supportsColumnName(modelType)) {
        const modelColumnNames = columns.map((col) => col.name);
        const individualColumns = modelingState.select.filter(
          (s) => typeof s === 'string' && modelColumnNames.includes(s),
        ) as string[];

        if (individualColumns.length > 0) {
          return {
            filterType: '' as SelectionType, // Keep in individual mode
            include: individualColumns,
            exclude: undefined,
          };
        }
      }

      // For model types that support SchemaModelSelectExpr only,
      if (supportsExprOnly(modelType)) {
        const modelColumnNames = columns.map((col) => col.name);
        const exprColumns = modelingState.select.filter(
          (s) =>
            typeof s !== 'string' &&
            'name' in s &&
            'expr' in s &&
            modelColumnNames.includes(s.name) &&
            s.expr === s.name, // Only simple column references, not complex expressions
        ) as { name: string; expr: string }[];

        if (exprColumns.length > 0) {
          return {
            filterType: '' as SelectionType, // Keep in individual mode
            include: exprColumns.map((c) => c.name),
            exclude: undefined,
          };
        }
      }

      // Get model/source-based selection (all_from_model, dims_from_model, etc.)
      const modelSelection = modelingState.select.find((s) => {
        // Skip string column names
        if (typeof s === 'string') {
          return false;
        }

        let value;
        if (isTypeSource) {
          value = 'source' in s ? s.source : undefined;
        } else {
          value = 'model' in s ? s.model : undefined;
        }
        return value === selectedModel.value;
      });

      if (modelSelection && typeof modelSelection !== 'string') {
        const result: {
          filterType: SelectionType;
          include?: string[];
          exclude?: string[];
        } = {
          filterType:
            (modelSelection.type as SelectionType) || ('' as SelectionType),
        };

        // Only add include if it has data
        if (
          'include' in modelSelection &&
          modelSelection.include &&
          modelSelection.include.length > 0
        ) {
          result.include = modelSelection.include;
        }

        // Only add exclude if it has data
        if (
          'exclude' in modelSelection &&
          modelSelection.exclude &&
          modelSelection.exclude.length > 0
        ) {
          result.exclude = modelSelection.exclude;
        }

        return result;
      }

      // Fallback: Check for individual column names (strings) that belong to THIS MODEL
      // This handles cases not covered by schema-aware checks above
      const modelColumnNames = columns.map((col) => col.name);
      const individualColumns = modelingState.select.filter(
        (s) => typeof s === 'string' && modelColumnNames.includes(s),
      ) as string[];

      if (individualColumns.length > 0) {
        return {
          filterType: '' as SelectionType,
          include: individualColumns,
          exclude: undefined,
        };
      }
    }
    return undefined;
  }, [
    selectedModel,
    isTypeSource,
    columns,
    modelingState.select,
    basicFields.type,
  ]);

  // Memoize the selection change handler to prevent unnecessary re-renders
  const handleSelectionChange = useCallback(
    (
      selectionType: SelectionType | '',
      selection: { include?: string[]; exclude?: string[] },
      shouldClear?: boolean,
    ) => {
      if (!selectedModel) {
        return;
      }

      const { basicFields, modelingState: ms } = useModelStore.getState();
      const hasJoins = Array.isArray(ms.join) ? ms.join.length > 0 : !!ms.join;
      const updated = buildUpdatedSelections(ms.select, {
        qualifier: hasJoins ? selectedModel.value : '',
        modelColumnNames: new Set(columns.map((c) => c.name)),
        modelType: basicFields.type,
        selectionType,
        selection,
        shouldClear: shouldClear ?? false,
        selectedModelValue: selectedModel.value,
        isTypeSource,
        columns,
      });
      updateSelectState(updated);
    },
    [selectedModel, updateSelectState, isTypeSource, columns],
  );

  const fetchInitialData = useCallback(async () => {
    setLoading(true);
    setError(null);
    setModelsLoading(true);
    try {
      let projects: DbtProject[] = [];

      const projectsResponse = await api.post({
        type: 'dbt-fetch-projects',
        request: null,
      });
      projects = projectsResponse || [];

      if (projects.length === 0) {
        setCurrentProject(null);
        setModels([]);
        return;
      }

      const project = projects[0];
      setCurrentProject(project);

      if (!project.manifest?.nodes) {
        setModels([]);
        return;
      }

      // Check if we should fetch sources or models based on model type
      if (isTypeSource) {
        // Fetch sources from manifest.nodes (filter by source.)
        const sourceNames = Object.keys(project.manifest.sources)
          .filter((key) => key.startsWith('source.'))
          .map((key) => {
            const source = project.manifest.sources[key];
            return source?.source_name && source?.name
              ? `${source.source_name}.${source.name}`
              : null;
          })
          .filter((name): name is string => Boolean(name));

        setModels(sourceNames);
      } else {
        // Fetch models and seeds from manifest.nodes
        const modelNames = Object.keys(project.manifest.nodes)
          .filter((key) => key.startsWith('model.') || key.startsWith('seed.'))
          .map((key) => project.manifest.nodes[key]?.name)
          .filter((name): name is string => Boolean(name));

        setModels(modelNames);
      }
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : 'Failed to fetch data';
      setError(errorMessage);
      setModels([]);

      setCurrentProject(null);
    } finally {
      setLoading(false);
      setModelsLoading(false);
    }
  }, [api, isTypeSource]);

  useEffect(() => {
    void fetchInitialData();
  }, [fetchInitialData]);

  const handleModelChange = useCallback(
    (option: AvailableModel | null) => {
      // Store the previous model/source and its selection type to preserve it
      const previousModel = selectedModel?.value;
      let previousSelectionType: SelectionType | undefined;

      // Get the previous model's selection to preserve the filter type
      if (previousModel) {
        const currentSelections = useModelStore.getState().modelingState.select;
        const previousSelection = currentSelections.find(
          (existingSelection) =>
            typeof existingSelection !== 'string' &&
            'model' in existingSelection &&
            existingSelection.model === previousModel,
        );

        if (previousSelection && typeof previousSelection !== 'string') {
          previousSelectionType = previousSelection.type as SelectionType;
        }

        // Remove the previous model's selection AND any string column names (SchemaColumnName)
        const filteredSelections = currentSelections.filter(
          (existingSelection) => {
            // Remove string column names
            if (typeof existingSelection === 'string') {
              return false;
            }
            // Remove previous model's selection
            if ('model' in existingSelection || 'source' in existingSelection) {
              const existingModel =
                'model' in existingSelection
                  ? existingSelection.model
                  : 'source' in existingSelection
                    ? existingSelection.source
                    : undefined;
              return existingModel !== previousModel;
            }
            // Keep everything else (expr-based columns, etc.)
            return true;
          },
        );
        updateSelectState(filteredSelections);
      }

      setSelectedModel(option);

      if (!option?.value || !currentProject?.manifest) {
        setColumns([]);
        updateFromState({ model: '', source: '' });
        return;
      }

      const allColumns: Column[] = [];

      if (isTypeSource) {
        // Handle sources from manifest.sources
        if (!currentProject.manifest.sources) {
          setColumns([]);
          updateFromState({ model: '', source: option.value });
          return;
        }

        const sourceKey = Object.keys(currentProject.manifest.sources).find(
          (key) => {
            const source = currentProject.manifest.sources[key];
            if (
              !key.startsWith('source.') ||
              !source?.source_name ||
              !source?.name
            ) {
              return false;
            }
            const fullSourceName = `${source.source_name}.${source.name}`;
            return fullSourceName === option.value;
          },
        );

        if (sourceKey && currentProject.manifest.sources[sourceKey]) {
          allColumns.push(
            ...extractColumnsFromNode(
              currentProject.manifest.sources[sourceKey],
            ),
          );
        }

        setColumns(allColumns);
        // Update ModelStore with source
        updateFromState({ model: '', source: option.value });
      } else {
        // Handle models
        if (!currentProject.manifest.nodes) {
          setColumns([]);
          updateFromState({ model: '', source: '' });
          return;
        }

        const modelKey = Object.keys(currentProject.manifest.nodes).find(
          (key) => {
            const node = currentProject.manifest.nodes?.[key];
            return (
              (key.startsWith('model.') || key.startsWith('seed.')) &&
              node?.name === option.value
            );
          },
        );

        if (modelKey && currentProject.manifest.nodes[modelKey]) {
          allColumns.push(
            ...extractColumnsFromNode(currentProject.manifest.nodes[modelKey]),
          );
        }

        setColumns(allColumns);
        // Update ModelStore with model
        updateFromState({ model: option.value, source: '' });
      }

      // Apply the preserved filter type to the new model if it existed
      if (option?.value && previousSelectionType) {
        const currentSelections = useModelStore.getState().modelingState.select;

        // Add the new model with the previous filter type but no column selections
        const newSelection = isTypeSource
          ? {
              source: option.value,
              type: previousSelectionType as SelectionTypeValues,
            }
          : {
              model: option.value,
              type: previousSelectionType as SelectionTypeValues,
            };

        updateSelectState([...currentSelections, newSelection as SchemaSelect]);
      }
      if (option?.value && !isTypeSource && dataExplorerOpenedRef.current) {
        api
          .post({
            type: 'data-explorer-open-with-model',
            request: {
              projectName: currentProject?.name || '',
              modelName: option.value,
            },
          })
          .catch(console.error);
      }
    },
    [
      api,
      currentProject,
      updateFromState,
      isTypeSource,
      selectedModel,
      updateSelectState,
    ],
  );

  // Use ref to track if we've already prepopulated to prevent infinite loops
  const hasPrefilledRef = useRef(false);

  // Track if Data Explorer has been opened for this component
  const dataExplorerOpenedRef = useRef(false);

  useEffect(() => {
    const identifierToUse = isTypeSource
      ? modelingState.from.source
      : modelingState.from.model || modelingState.from.cte;

    if (
      identifierToUse &&
      models.length > 0 &&
      !selectedModel &&
      !hasPrefilledRef.current
    ) {
      const prefilledOption = modelOptions.find(
        (option) => option.value === identifierToUse,
      );

      if (prefilledOption) {
        hasPrefilledRef.current = true;
        void handleModelChange(prefilledOption);
      }
    }
  }, [
    isTypeSource,
    modelingState.from.model,
    modelingState.from.source,
    modelingState.from.cte,
    models,
    modelOptions,
    selectedModel,
    handleModelChange,
  ]);

  // Tutorial: Auto-open dropdown when tutorial highlights this node
  useEffect(() => {
    if (isPlayTutorialActive && selectWrapperRef.current && !selectedModel) {
      // Wait for Driver.js to highlight, then open dropdown
      const timer = setTimeout(() => {
        const selectControl = selectWrapperRef.current?.querySelector(
          '.react-select__control',
        );
        if (selectControl) {
          (selectControl as HTMLElement).click();
        }
      }, 800); // Wait for highlight animation

      return () => clearTimeout(timer);
    }
  }, [isPlayTutorialActive, selectedModel]);

  // Handler for opening Data Explorer with the selected model
  const handleOpenDataExplorer = useCallback(() => {
    if (!selectedModel || !currentProject) {
      return;
    }

    // Mark that Data Explorer has been opened
    dataExplorerOpenedRef.current = true;

    void api.post({
      type: 'data-explorer-open-with-model',
      request: {
        modelName: selectedModel.value,
        projectName: currentProject.name,
      },
    });
  }, [api, selectedModel, currentProject]);

  // Handler for opening Column Lineage with a specific column
  const handleColumnLineageClick = useCallback(
    (columnName: string) => {
      if (!selectedModel || !currentProject?.manifest) {
        return;
      }

      // Find the model/source in manifest to get file path
      let relativePath: string | undefined;
      let tableName: string | undefined;

      if (isTypeSource) {
        // For sources, find in manifest.sources
        const sourceKey = Object.keys(
          currentProject.manifest.sources || {},
        ).find((key) => {
          const source = currentProject.manifest.sources[key];
          if (!source?.source_name || !source?.name) {
            return false;
          }
          const fullSourceName = `${source.source_name}.${source.name}`;
          return fullSourceName === selectedModel.value;
        });
        if (sourceKey) {
          const source = currentProject.manifest.sources[sourceKey];
          // original_file_path is a .yml file, convert to .source.json
          const originalPath = source?.original_file_path;
          if (originalPath) {
            relativePath = originalPath.replace(/\.yml$/, '.source.json');
          }
          tableName = source?.name; // Table name within the source
        }
      } else {
        // For models, find in manifest.nodes
        const modelKey = Object.keys(currentProject.manifest.nodes || {}).find(
          (key) => {
            const node = currentProject.manifest.nodes?.[key];
            return (
              (key.startsWith('model.') || key.startsWith('seed.')) &&
              node?.name === selectedModel.value
            );
          },
        );
        if (modelKey) {
          const node = currentProject.manifest.nodes[modelKey];
          // original_file_path is a .sql file, convert to .model.json
          const originalPath = node?.original_file_path;
          if (originalPath) {
            relativePath = originalPath.replace(/\.sql$/, '.model.json');
          }
        }
      }

      if (!relativePath) {
        console.warn('Could not find file path for column lineage');
        return;
      }

      // Construct absolute path by joining project pathSystem with relative path
      const absolutePath = `${currentProject.pathSystem}/${relativePath}`;

      if (isTypeSource && tableName) {
        // For sources, use switch-to-source-column action
        void api.post({
          type: 'framework-column-lineage',
          request: {
            action: 'switch-to-source-column',
            filePath: absolutePath,
            tableName,
            columnName,
            downstreamLevels: 2,
            skipOpenFile: true, // Don't open file when triggered from UI
          },
        });
      } else {
        // For models, use switch-to-model-column action
        void api.post({
          type: 'framework-column-lineage',
          request: {
            action: 'switch-to-model-column',
            filePath: absolutePath,
            columnName,
            upstreamLevels: 2,
            downstreamLevels: 2,
            skipOpenFile: true, // Don't open file when triggered from UI
          },
        });
      }
    },
    [api, selectedModel, currentProject, isTypeSource],
  );

  return (
    <div
      className={`flex flex-col gap-4 py-4 shadow-lg rounded-lg bg-background border-2 min-w-[400px] border-neutral`}
      onClick={(e) => e.stopPropagation()}
      onDoubleClick={(e) => e.stopPropagation()}
      data-tutorial-id="select-node"
    >
      <div className="flex-1 px-4">
        <div className="flex items-center justify-between mb-2">
          <div className="webkit-font-smoothing-antialiased font-bold text-xs text-muted-foreground pl-1 flex items-center gap-1">
            {isTypeSource ? 'SELECT FROM SOURCE' : 'SELECT FROM'}
            <Tooltip
              content={
                isTypeSource
                  ? 'Choose a source table from your data warehouse to build your model from'
                  : 'Select an existing dbt model to build upon'
              }
              variant="outline"
            />
          </div>
          {selectedModel && !isTypeSource && (
            <Button
              onClick={(e) => {
                e.stopPropagation();
                handleOpenDataExplorer();
              }}
              disabled={!selectedModel}
              variant="iconButton"
              label="DATA EXPLORER"
              icon={<DataSearchIcon className="w-3 h-3" />}
              iconLabelClassName="text-xs"
              className="text-tiny bg-primary text-white hover:text-white font-bold p-0.5 px-2"
            />
          )}
        </div>

        <div
          className="flex items-center gap-2 border border-neutral rounded pl-2"
          ref={selectWrapperRef}
        >
          <CircleStackIcon className="w-4 h-4 text-foreground flex-shrink-0" />
          {modelsLoading ? (
            <div className="text-sm text-muted-foreground py-1 pl-1 flex-1">
              Loading models...
            </div>
          ) : (
            <SelectSingle
              label=""
              options={modelOptions}
              value={selectedModel}
              onChange={(option) => {
                void handleModelChange(option);
              }}
              placeholder="Start typing the model name..."
              onBlur={() => {}}
              error={error || undefined}
              disabled={loading}
              className="w-full flex-1 bg-transparent h-8 py-1 pl-1 text-background-contrast text-sm ring-0 border-0 shadow-none focus:ring-0 focus:border-0 focus:outline-none"
            />
          )}
        </div>
      </div>

      {selectedModel && (
        <ModelColumns
          columns={columns}
          nodeId="1"
          isSourceModel={isTypeSource}
          onSelectionChange={handleSelectionChange}
          defaultValue={columnDefaultValue}
          onColumnLineageClick={handleColumnLineageClick}
        />
      )}

      <Handle
        type="source"
        position={Position.Bottom}
        id="output"
        style={{
          background: '#757575',
          border: '1px solid #757575',
          width: '8px',
          height: '8px',
        }}
        className="bg-muted"
      />
    </div>
  );
};
