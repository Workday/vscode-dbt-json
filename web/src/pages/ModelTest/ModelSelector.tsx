import {
  ArrowPathIcon,
  Cog6ToothIcon,
  FunnelIcon,
  MagnifyingGlassIcon,
} from '@heroicons/react/24/outline';
import { makeClassName } from '@web';
import { useApp } from '@web/context';
import { Button, Checkbox, DialogBox, InputText } from '@web/elements';
import type { AvailableModel } from '@web/stores/useModelTestStore';
import { useModelTestStore } from '@web/stores/useModelTestStore';
import { useEffect, useMemo, useState } from 'react';

import {
  AddTestsButton,
  ConfigButton,
  InlineLineageToggles,
  RunConfigTabContent,
  TabbedConfigPopover,
  TabButton,
  TestCountPopover,
  TestsTabContent,
} from './ConfigComponents';

// Structure type options for filter
type StructureType =
  | 'all'
  | 'join'
  | 'rollup'
  | 'select'
  | 'union'
  | 'lookback'
  | 'join_column';

// Helper to extract structure type from model (outside component to avoid useMemo warning)
function getStructureType(
  model: AvailableModel,
):
  | 'join'
  | 'rollup'
  | 'select'
  | 'union'
  | 'lookback'
  | 'join_column'
  | 'other' {
  // Check modelType field if available (e.g., "int_join_models", "stg_rollup_models")
  if (model.modelType) {
    if (model.modelType.includes('lookback')) return 'lookback';
    if (model.modelType.includes('join_column')) return 'join_column';
    if (model.modelType.includes('union')) return 'union';
    if (model.modelType.includes('join')) return 'join';
    if (model.modelType.includes('rollup')) return 'rollup';
    if (model.modelType.includes('select')) return 'select';
  }
  // Fallback: check based on hasJoins
  if (model.hasJoins) return 'join';
  if (model.hasAggregates && !model.hasJoins) return 'rollup';
  return 'select';
}

// Section Header Component
function SectionHeader({
  title,
  count,
  showCheckbox,
  isChecked,
  onToggle,
  children,
}: {
  title: string;
  count: number;
  showCheckbox?: boolean;
  isChecked?: boolean;
  onToggle: () => void;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between px-4 py-2 mt-4 mb-1 shrink-0 relative">
      <div className="flex items-center gap-3">
        {showCheckbox && (
          <Checkbox checked={isChecked ?? false} onChange={onToggle} />
        )}
        <div className="flex items-center gap-2">
          <h3 className="text-[12px] font-bold text-surface-contrast/50 uppercase tracking-widest">
            {title}
          </h3>
          <span className="text-[11px] bg-surface text-surface-contrast/70 px-2 py-0.5 rounded font-mono font-bold">
            {count}
          </span>
        </div>
      </div>
      <div className="flex items-center gap-3 shrink-0">{children}</div>
    </div>
  );
}

export function ModelSelector() {
  const { api } = useApp();

  const {
    projectName,
    searchTerm,
    setSearchTerm,
    activeChanges,
    availableModels,
    toggleModelCheck,
    addToActiveChanges,
    revertAll,
    bulkAddAvailable,
    updateModelConfig,
    toggleModelConfigField,
    applyBulkConfig,
    addTestsWithBackend,
    applyBulkTestsWithBackend,
    revertAllWithBackend,
    revertModelWithBackend,
  } = useModelTestStore();

  // Revert confirmation dialog state
  const [showRevertDialog, setShowRevertDialog] = useState(false);
  const [modelToRevert, setModelToRevert] = useState<string | null>(null);

  // Individual model config popover state
  const [openConfigId, setOpenConfigId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'tests' | 'runConfig'>('tests');
  const [draftTests, setDraftTests] = useState<string[]>([]);
  const [draftUpLimit, setDraftUpLimit] = useState(0);
  const [draftDownLimit, setDraftDownLimit] = useState(0);

  // Bulk config popover state
  const [isBulkConfigOpen, setIsBulkConfigOpen] = useState(false);
  const [bulkActiveTab, setBulkActiveTab] = useState<'tests' | 'runConfig'>(
    'tests',
  );
  const [draftBulkTests, setDraftBulkTests] = useState<string[]>([]);
  const [bulkUpstream, setBulkUpstream] = useState(false);
  const [bulkDownstream, setBulkDownstream] = useState(false);
  const [bulkFullLineage, setBulkFullLineage] = useState(false);
  const [bulkUpLimit, setBulkUpLimit] = useState(0);
  const [bulkDownLimit, setBulkDownLimit] = useState(0);

  // Available models filter and sort state
  const [modelLayerFilter, setModelLayerFilter] = useState<
    'all' | 'int' | 'stg'
  >('all');
  const [structureTypeFilter, setStructureTypeFilter] =
    useState<StructureType>('all');
  const [sortOption] = useState<
    'name-asc' | 'name-desc' | 'tests-asc' | 'tests-desc'
  >('name-asc');

  // Combined filter popover state
  const [filterPopoverOpen, setFilterPopoverOpen] = useState(false);

  // Close filter popover when clicking outside
  useEffect(() => {
    if (!filterPopoverOpen) return;

    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      if (!target.closest('[data-filter-popover]')) {
        setFilterPopoverOpen(false);
      }
    };

    // Use setTimeout to avoid immediate closing on the same click
    const timeoutId = setTimeout(() => {
      document.addEventListener('click', handleClickOutside);
    }, 0);

    return () => {
      clearTimeout(timeoutId);
      document.removeEventListener('click', handleClickOutside);
    };
  }, [filterPopoverOpen]);

  // Toggle test in draft
  const toggleDraftTest = (test: string) => {
    setDraftTests((prev) =>
      prev.includes(test) ? prev.filter((t) => t !== test) : [...prev, test],
    );
  };

  // Toggle test in bulk draft
  const toggleBulkDraftTest = (test: string) => {
    setDraftBulkTests((prev) =>
      prev.includes(test) ? prev.filter((t) => t !== test) : [...prev, test],
    );
  };

  // Open individual config popover
  const handleOpenConfig = (modelName: string) => {
    const model = activeChanges.find((m) => m.name === modelName);
    if (!model) return;

    setOpenConfigId(modelName);
    setActiveTab('tests');
    // Pre-populate draft tests from existingDataTests (already persisted to JSON)
    const existingTestTypes = model.existingDataTests.map((t) => t.type);
    setDraftTests([...existingTestTypes]);
    setDraftUpLimit(model.config.upLimit);
    setDraftDownLimit(model.config.downLimit);
    setIsBulkConfigOpen(false);
  };

  // Build test payload with required fields based on test type and model info
  const buildTestPayload = (
    testId: string,
    model: (typeof activeChanges)[0],
  ) => {
    // For row count tests, include compare_model and join_type
    if (testId === 'equal_row_count' || testId === 'equal_or_lower_row_count') {
      return {
        type: testId,
        compare_model: model.fromModel
          ? `ref('${model.fromModel}')`
          : undefined,
        join_type: model.firstJoinType || 'left',
      };
    }
    // For no_null_aggregates, column_name will be auto-detected by backend
    return { type: testId };
  };

  // Commit individual config
  const commitConfig = async () => {
    if (openConfigId) {
      const model = activeChanges.find((m) => m.name === openConfigId);
      if (!model) return;

      // Update local config state (upstream/downstream limits)
      updateModelConfig(openConfigId, {
        upLimit: draftUpLimit,
        downLimit: draftDownLimit,
      });

      // Get existing test types from model
      const existingTestTypes = model.existingDataTests.map((t) => t.type);

      // Filter to only NEW tests that don't already exist
      const newTests = draftTests.filter(
        (testId) => !existingTestTypes.includes(testId),
      );

      // Persist only new tests to backend
      if (newTests.length > 0 && projectName) {
        const testsPayload = newTests.map((testId) =>
          buildTestPayload(testId, model),
        );
        await addTestsWithBackend(
          api.post,
          projectName,
          openConfigId,
          false,
          testsPayload,
        );
      }
    }
    setOpenConfigId(null);
  };

  // Open bulk config popover
  const handleOpenBulkConfig = () => {
    setIsBulkConfigOpen(!isBulkConfigOpen);
    setBulkActiveTab('tests');
    setOpenConfigId(null);
    setDraftBulkTests([]);
    setBulkUpstream(false);
    setBulkDownstream(false);
    setBulkFullLineage(false);
    setBulkUpLimit(0);
    setBulkDownLimit(0);
  };

  // Commit bulk config
  const commitBulkConfig = async () => {
    // Apply bulk config to local state first
    applyBulkConfig({
      upstream: bulkUpstream,
      downstream: bulkDownstream,
      fullLineage: bulkFullLineage,
      upLimit: bulkUpLimit,
      downLimit: bulkDownLimit,
    });

    // Persist tests to backend for all checked models if there are selected tests
    if (draftBulkTests.length > 0 && projectName) {
      await applyBulkTestsWithBackend(api.post, projectName, draftBulkTests);
    }
    setIsBulkConfigOpen(false);
  };

  // Add default tests to a model using smart auto-detection
  const applyDefaultsToModel = async (modelName: string) => {
    if (!projectName) return;
    // Use autoDetect=true to let the backend smart-detect applicable tests
    await addTestsWithBackend(api.post, projectName, modelName, true);
  };

  // Revert a single model (remove all data tests) - show confirmation first
  const handleRevertModel = (modelName: string) => {
    setModelToRevert(modelName);
  };

  // Confirm individual model revert
  const confirmRevertModel = async () => {
    if (!projectName || !modelToRevert) return;
    await revertModelWithBackend(api.post, projectName, modelToRevert);
    setModelToRevert(null);
  };

  // Filter models based on search term
  const filteredActiveChanges = useMemo(() => {
    if (!searchTerm) return activeChanges;
    const search = searchTerm.toLowerCase();
    return activeChanges.filter((model) =>
      model.name.toLowerCase().includes(search),
    );
  }, [activeChanges, searchTerm]);

  const filteredAvailable = useMemo(() => {
    let result = availableModels;

    // Filter by search term
    if (searchTerm) {
      const search = searchTerm.toLowerCase();
      result = result.filter((model) =>
        model.name.toLowerCase().includes(search),
      );
    }

    // Filter by model layer (int/stg)
    if (modelLayerFilter !== 'all') {
      result = result.filter((model) =>
        model.name.startsWith(`${modelLayerFilter}__`),
      );
    }

    // Filter by structure type (join/rollup/select)
    if (structureTypeFilter !== 'all') {
      result = result.filter(
        (model) => getStructureType(model) === structureTypeFilter,
      );
    }

    // Sort
    result = [...result].sort((a, b) => {
      switch (sortOption) {
        case 'name-asc':
          return a.name.localeCompare(b.name);
        case 'name-desc':
          return b.name.localeCompare(a.name);
        case 'tests-asc':
          return a.testCount - b.testCount;
        case 'tests-desc':
          return b.testCount - a.testCount;
        default:
          return 0;
      }
    });

    return result;
  }, [
    availableModels,
    searchTerm,
    modelLayerFilter,
    structureTypeFilter,
    sortOption,
  ]);

  const allActiveChangesChecked =
    filteredActiveChanges.length > 0 &&
    filteredActiveChanges.every((m) => m.checked);

  return (
    <div className="flex flex-col h-full bg-card">
      {/* Search */}
      <div className="h-[52px] px-3 border-b border-surface bg-card sticky top-0 z-30 flex items-center">
        <div className="relative group w-full">
          <MagnifyingGlassIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-surface-contrast/50 group-focus-within:text-primary transition-colors z-10" />
          <InputText
            placeholder="Search models..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            inputClassName="pl-9"
          />
        </div>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-y-auto pb-6">
        {/* Active Changes Section */}
        <SectionHeader
          title="Active Changes"
          count={filteredActiveChanges.length}
          showCheckbox={filteredActiveChanges.length > 0}
          isChecked={allActiveChangesChecked}
          onToggle={() => void revertAll()}
        >
          {activeChanges.length > 0 && (
            <div className="flex items-center gap-4">
              <button
                onClick={() => setShowRevertDialog(true)}
                className="text-[11px] text-surface-contrast/50 hover:text-red-500 font-bold uppercase tracking-tighter whitespace-nowrap flex items-center gap-1 transition-colors"
              >
                <ArrowPathIcon className="w-3 h-3" />
                Revert
              </button>
              <div className="relative">
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    handleOpenBulkConfig();
                  }}
                  className={makeClassName(
                    'text-[11px] flex items-center gap-1 font-bold uppercase tracking-tighter whitespace-nowrap transition-colors',
                    isBulkConfigOpen
                      ? 'text-primary'
                      : 'text-surface-contrast/50 hover:text-primary',
                  )}
                >
                  <Cog6ToothIcon className="w-3.5 h-3.5" />
                  Config All
                </button>

                {/* Bulk Config Popover */}
                {isBulkConfigOpen && (
                  <div
                    onClick={(e) => e.stopPropagation()}
                    className="absolute right-0 top-full mt-2 w-80 bg-card border border-surface shadow-2xl rounded-xl z-[150] overflow-hidden"
                  >
                    {/* Tabs */}
                    <div className="flex border-b border-surface/50">
                      <TabButton
                        label="Tests"
                        isActive={bulkActiveTab === 'tests'}
                        onClick={() => setBulkActiveTab('tests')}
                      />
                      <TabButton
                        label="Run Config"
                        isActive={bulkActiveTab === 'runConfig'}
                        onClick={() => setBulkActiveTab('runConfig')}
                      />
                    </div>

                    {/* Tab Content */}
                    <div className="p-4">
                      {bulkActiveTab === 'tests' ? (
                        <TestsTabContent
                          selectedTests={draftBulkTests}
                          onToggleTest={toggleBulkDraftTest}
                          hasJoins={activeChanges
                            .filter((m) => m.checked)
                            .some((m) => m.hasJoins)}
                          hasAggregates={activeChanges
                            .filter((m) => m.checked)
                            .some((m) => m.hasAggregates)}
                          hasPortalPartitionDaily={activeChanges
                            .filter((m) => m.checked)
                            .some((m) => m.hasPortalPartitionDaily)}
                          existingDataTests={[]}
                        />
                      ) : (
                        <RunConfigTabContent
                          upstream={bulkUpstream}
                          downstream={bulkDownstream}
                          fullLineage={bulkFullLineage}
                          upLimit={bulkUpLimit}
                          downLimit={bulkDownLimit}
                          onToggleUpstream={() =>
                            setBulkUpstream(!bulkUpstream)
                          }
                          onToggleDownstream={() =>
                            setBulkDownstream(!bulkDownstream)
                          }
                          onToggleFullLineage={() =>
                            setBulkFullLineage(!bulkFullLineage)
                          }
                          onUpLimitChange={setBulkUpLimit}
                          onDownLimitChange={setBulkDownLimit}
                        />
                      )}
                    </div>

                    {/* Footer */}
                    <div className="flex items-center gap-2 p-3 border-t border-surface/50 bg-surface/30">
                      <Button
                        label="Done"
                        onClick={() => void commitBulkConfig()}
                        className="flex-1 text-[11px] py-2"
                      />
                      <Button
                        label="Discard"
                        variant="secondary"
                        onClick={() => setIsBulkConfigOpen(false)}
                        className="flex-1 text-[11px] py-2"
                      />
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </SectionHeader>

        {/* Active Changes List */}
        <div className="space-y-0.5">
          {filteredActiveChanges.length === 0 ? (
            <div className="p-4 text-center text-sm text-surface-contrast/50">
              {activeChanges.length === 0
                ? 'No active changes'
                : 'No models match your search'}
            </div>
          ) : (
            filteredActiveChanges.map((item) => (
              <div
                key={item.name}
                className={makeClassName(
                  'group flex flex-col px-4 py-3 hover:bg-surface/30 transition-colors border-l-4 border-transparent hover:border-primary/30 relative',
                  item.checked && 'bg-surface/20',
                )}
              >
                {/* Main Row */}
                <div className="flex items-center justify-between w-full">
                  <div className="flex items-center min-w-0 flex-1">
                    <Checkbox
                      checked={item.checked}
                      onChange={() => toggleModelCheck(item.name)}
                    />
                    <div className="ml-3 min-w-0">
                      <p
                        title={item.name}
                        className={makeClassName(
                          'text-[14px] font-mono truncate font-medium',
                          item.checked
                            ? 'text-surface-contrast'
                            : 'text-surface-contrast/50',
                        )}
                      >
                        {item.name}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <TestCountPopover
                      testDetails={item.testDetails || []}
                      testCount={item.testCount}
                    />
                    {/* Individual Revert Button */}
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleRevertModel(item.name);
                      }}
                      title="Revert model (remove all data tests)"
                      className="p-1 rounded hover:bg-red-500/10 text-surface-contrast/30 hover:text-red-500 transition-colors"
                    >
                      <ArrowPathIcon className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>

                {/* Action Buttons Row */}
                <div className="flex items-center justify-between mt-1.5 ml-[28px]">
                  <div className="flex items-center gap-1.5">
                    {/* Config Button */}
                    <div className="relative">
                      <ConfigButton
                        isActive={openConfigId === item.name}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleOpenConfig(item.name);
                        }}
                      />

                      {/* Individual Config Popover */}
                      <TabbedConfigPopover
                        isOpen={openConfigId === item.name}
                        onClose={() => setOpenConfigId(null)}
                        onCommit={() => void commitConfig()}
                        position="left"
                        activeTab={activeTab}
                        onTabChange={setActiveTab}
                        testsContent={
                          <TestsTabContent
                            selectedTests={draftTests}
                            onToggleTest={toggleDraftTest}
                            hasJoins={item.hasJoins}
                            hasAggregates={item.hasAggregates}
                            hasPortalPartitionDaily={
                              item.hasPortalPartitionDaily
                            }
                            existingDataTests={item.existingDataTests}
                          />
                        }
                        runConfigContent={
                          <RunConfigTabContent
                            upstream={item.config.upstream}
                            downstream={item.config.downstream}
                            fullLineage={item.config.fullLineage}
                            upLimit={draftUpLimit}
                            downLimit={draftDownLimit}
                            onToggleUpstream={() =>
                              toggleModelConfigField(item.name, 'upstream')
                            }
                            onToggleDownstream={() =>
                              toggleModelConfigField(item.name, 'downstream')
                            }
                            onToggleFullLineage={() =>
                              toggleModelConfigField(item.name, 'fullLineage')
                            }
                            onUpLimitChange={setDraftUpLimit}
                            onDownLimitChange={setDraftDownLimit}
                          />
                        }
                      />
                    </div>

                    {/* ADD TESTS Button */}
                    <AddTestsButton
                      onClick={(e) => {
                        e.stopPropagation();
                        void applyDefaultsToModel(item.name);
                      }}
                      disabled={
                        (!item.hasJoins && !item.hasAggregates) ||
                        (item.hasJoins && !item.hasPortalPartitionDaily)
                      }
                      disabledTooltip={
                        !item.hasJoins && !item.hasAggregates
                          ? 'No applicable tests (requires joins or aggregates)'
                          : item.hasJoins && !item.hasPortalPartitionDaily
                            ? 'Row count tests require portal_partition_daily column'
                            : undefined
                      }
                    />
                  </div>

                  {/* Inline Lineage Toggles */}
                  <InlineLineageToggles
                    upstream={item.config.upstream}
                    downstream={item.config.downstream}
                    fullLineage={item.config.fullLineage}
                    onToggleUpstream={() =>
                      toggleModelConfigField(item.name, 'upstream')
                    }
                    onToggleDownstream={() =>
                      toggleModelConfigField(item.name, 'downstream')
                    }
                    onToggleFullLineage={() =>
                      toggleModelConfigField(item.name, 'fullLineage')
                    }
                  />
                </div>
              </div>
            ))
          )}
        </div>

        {/* Available Models Section */}
        <SectionHeader
          title="Available Models"
          count={filteredAvailable.length}
          showCheckbox={filteredAvailable.length > 0}
          isChecked={false}
          onToggle={() => void bulkAddAvailable()}
        >
          {/* Combined Filter Icon Button */}
          <div className="relative" data-filter-popover>
            <button
              onClick={() => setFilterPopoverOpen(!filterPopoverOpen)}
              title="Filter models"
              className={makeClassName(
                'p-1.5 rounded-md transition-all flex items-center gap-1',
                filterPopoverOpen ||
                  modelLayerFilter !== 'all' ||
                  structureTypeFilter !== 'all'
                  ? 'bg-primary text-primary-contrast'
                  : 'bg-surface/50 text-surface-contrast/50 hover:bg-surface hover:text-surface-contrast/70',
              )}
            >
              <FunnelIcon className="w-3.5 h-3.5" />
              {(modelLayerFilter !== 'all' ||
                structureTypeFilter !== 'all') && (
                <span className="text-[9px] font-bold">
                  {[
                    modelLayerFilter !== 'all' &&
                      modelLayerFilter.toUpperCase(),
                    structureTypeFilter !== 'all' && structureTypeFilter,
                  ]
                    .filter(Boolean)
                    .join(' · ')}
                </span>
              )}
            </button>
            {filterPopoverOpen && (
              <div className="absolute right-0 top-full mt-1 bg-card border border-surface rounded-lg shadow-lg z-50 overflow-hidden min-w-[140px]">
                {/* Layer Section */}
                <div className="px-3 py-1.5 border-b border-surface bg-surface/30">
                  <span className="text-[9px] font-bold text-surface-contrast/50 uppercase">
                    Layer
                  </span>
                </div>
                <div className="py-1">
                  {(['all', 'int', 'stg'] as const).map((layer) => (
                    <button
                      key={layer}
                      onClick={() => setModelLayerFilter(layer)}
                      className={makeClassName(
                        'w-full px-3 py-1.5 text-left text-[11px] font-medium transition-colors flex items-center justify-between',
                        modelLayerFilter === layer
                          ? 'bg-primary/10 text-primary'
                          : 'hover:bg-surface/50 text-surface-contrast/70',
                      )}
                    >
                      <span>
                        {layer === 'all' ? 'All' : layer.toUpperCase()}
                      </span>
                      {modelLayerFilter === layer && (
                        <span className="text-primary">✓</span>
                      )}
                    </button>
                  ))}
                </div>

                {/* Structure Type Section */}
                <div className="px-3 py-1.5 border-y border-surface bg-surface/30">
                  <span className="text-[9px] font-bold text-surface-contrast/50 uppercase">
                    Type
                  </span>
                </div>
                <div className="py-1">
                  {(
                    [
                      'all',
                      'join',
                      'rollup',
                      'select',
                      'union',
                      'lookback',
                      'join_column',
                    ] as const
                  ).map((type) => (
                    <button
                      key={type}
                      onClick={() => setStructureTypeFilter(type)}
                      className={makeClassName(
                        'w-full px-3 py-1.5 text-left text-[11px] font-medium transition-colors flex items-center justify-between',
                        structureTypeFilter === type
                          ? 'bg-primary/10 text-primary'
                          : 'hover:bg-surface/50 text-surface-contrast/70',
                      )}
                    >
                      <span>
                        {type === 'all'
                          ? 'All'
                          : type === 'join_column'
                            ? 'Join Column'
                            : type.charAt(0).toUpperCase() + type.slice(1)}
                      </span>
                      {structureTypeFilter === type && (
                        <span className="text-primary">✓</span>
                      )}
                    </button>
                  ))}
                </div>

                {/* Clear All */}
                {(modelLayerFilter !== 'all' ||
                  structureTypeFilter !== 'all') && (
                  <div className="px-3 py-2 border-t border-surface">
                    <button
                      onClick={() => {
                        setModelLayerFilter('all');
                        setStructureTypeFilter('all');
                      }}
                      className="text-[10px] text-surface-contrast/50 hover:text-surface-contrast/70 underline"
                    >
                      Clear all filters
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        </SectionHeader>

        {/* Active Filters Display */}
        {(modelLayerFilter !== 'all' || structureTypeFilter !== 'all') && (
          <div className="px-4 pb-2 flex items-center gap-2 flex-wrap">
            <span className="text-[9px] text-surface-contrast/40 uppercase">
              Filters:
            </span>
            {modelLayerFilter !== 'all' && (
              <span className="text-[9px] px-2 py-0.5 bg-primary/20 text-primary rounded-full font-medium flex items-center gap-1">
                {modelLayerFilter}
                <button
                  onClick={() => setModelLayerFilter('all')}
                  className="hover:text-primary-contrast ml-0.5"
                >
                  ×
                </button>
              </span>
            )}
            {structureTypeFilter !== 'all' && (
              <span className="text-[9px] px-2 py-0.5 bg-primary/20 text-primary rounded-full font-medium flex items-center gap-1 capitalize">
                {structureTypeFilter}
                <button
                  onClick={() => setStructureTypeFilter('all')}
                  className="hover:text-primary-contrast ml-0.5"
                >
                  ×
                </button>
              </span>
            )}
            <button
              onClick={() => {
                setModelLayerFilter('all');
                setStructureTypeFilter('all');
              }}
              className="text-[9px] text-surface-contrast/40 hover:text-surface-contrast/70 underline"
            >
              Clear all
            </button>
          </div>
        )}

        {/* Available Models List */}
        <div className="space-y-0.5">
          {filteredAvailable.length === 0 ? (
            <div className="p-4 text-center text-sm text-surface-contrast/50">
              {availableModels.length === 0
                ? 'No other models available'
                : 'No models match your search'}
            </div>
          ) : (
            filteredAvailable.map((item) => (
              <div
                key={item.name}
                className="group flex items-center px-4 py-2.5 hover:bg-surface/30 transition-colors cursor-pointer"
                onClick={() => void addToActiveChanges(item)}
              >
                <Checkbox
                  checked={false}
                  onChange={() => void addToActiveChanges(item)}
                />
                <div className="ml-3 flex-1 flex items-center justify-between min-w-0">
                  <p
                    title={item.name}
                    className="text-[14px] font-mono text-surface-contrast/70 truncate group-hover:text-surface-contrast font-medium"
                  >
                    {item.name}
                  </p>
                  <TestCountPopover
                    testDetails={item.testDetails || []}
                    testCount={item.testCount}
                  />
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Revert All Confirmation Dialog */}
      <DialogBox
        open={showRevertDialog}
        variant="warning"
        title="Revert All Changes"
        description="Are you sure you want to revert all changes? This will remove all data tests from the model JSON files and move models back to Available Models."
        confirmCTALabel="Revert"
        discardCTALabel="Cancel"
        onConfirm={() => {
          void (async () => {
            if (projectName) {
              await revertAllWithBackend(api.post, projectName);
            } else {
              revertAll();
            }
            setShowRevertDialog(false);
          })();
        }}
        onDiscard={() => setShowRevertDialog(false)}
      />

      {/* Revert Individual Model Confirmation Dialog */}
      <DialogBox
        open={!!modelToRevert}
        variant="warning"
        title="Revert Model"
        description={`Are you sure you want to revert "${modelToRevert}"? This will remove all data tests from this model's JSON file.`}
        confirmCTALabel="Revert"
        discardCTALabel="Cancel"
        onConfirm={() => void confirmRevertModel()}
        onDiscard={() => setModelToRevert(null)}
      />
    </div>
  );
}
