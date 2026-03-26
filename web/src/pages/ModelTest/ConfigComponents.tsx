import {
  ArrowDownCircleIcon,
  ArrowUpCircleIcon,
  Cog6ToothIcon,
  MinusIcon,
  PlusIcon,
  ShieldCheckIcon,
  Square3Stack3DIcon,
} from '@heroicons/react/24/outline';
import { makeClassName } from '@web';
import { Button, Checkbox } from '@web/elements';
import { useRef, useState } from 'react';

// Available test options for configuration
// These match the generic tests in macros/tests/generic/
export const TEST_OPTIONS = [
  { id: 'equal_row_count', label: 'Equal Row Count', requiresModel: true },
  {
    id: 'equal_or_lower_row_count',
    label: 'Equal or Lower Row Count',
    requiresModel: true,
  },
  {
    id: 'no_null_aggregates',
    label: 'No Null Aggregates',
    requiresColumn: true,
  },
];

// Limit Spinner Component for upstream/downstream limits
export function LimitSpinner({
  value,
  onChange,
  disabled,
  label,
  icon: Icon,
  colorClass,
}: {
  value: number;
  onChange: (value: number) => void;
  disabled?: boolean;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  colorClass: string;
}) {
  return (
    <div
      className={makeClassName(
        'flex items-center justify-between px-2 py-1.5 rounded-lg transition-all',
        disabled ? 'opacity-30' : 'hover:bg-surface/30',
      )}
    >
      <div className="flex items-center gap-2.5">
        <Icon
          className={makeClassName(
            'w-4 h-4',
            disabled ? 'text-surface-contrast/30' : colorClass,
          )}
        />
        <span
          className={makeClassName(
            'text-[11px] font-bold uppercase tracking-widest',
            disabled ? 'text-surface-contrast/40' : 'text-surface-contrast/80',
          )}
        >
          {label}
        </span>
      </div>

      <div
        className={makeClassName(
          'flex items-center gap-1 bg-card border border-surface rounded px-1 ml-auto',
          disabled ? 'opacity-50 pointer-events-none' : '',
        )}
      >
        <button
          disabled={disabled || value <= 0}
          onClick={() => onChange(Math.max(0, value - 1))}
          className="p-0.5 hover:bg-surface/50 rounded disabled:opacity-50"
        >
          <MinusIcon className="w-2.5 h-2.5 text-surface-contrast/70" />
        </button>
        <input
          type="number"
          value={value}
          disabled={disabled}
          readOnly
          className="w-6 text-center text-[11px] font-mono font-bold bg-transparent border-none outline-none text-surface-contrast"
        />
        <button
          disabled={disabled}
          onClick={() => onChange(value + 1)}
          className="p-0.5 hover:bg-surface/50 rounded disabled:opacity-50"
        >
          <PlusIcon className="w-2.5 h-2.5 text-surface-contrast/70" />
        </button>
      </div>
    </div>
  );
}

// Lineage Toggle Buttons Component
export function LineageToggles({
  upstream,
  downstream,
  fullLineage,
  onToggleUpstream,
  onToggleDownstream,
  onToggleFullLineage,
}: {
  upstream: boolean;
  downstream: boolean;
  fullLineage: boolean;
  onToggleUpstream: () => void;
  onToggleDownstream: () => void;
  onToggleFullLineage: () => void;
}) {
  return (
    <div className="flex items-center gap-1 bg-surface/30 p-1 rounded-lg border border-surface/50">
      <button
        onClick={onToggleUpstream}
        disabled={fullLineage}
        className={makeClassName(
          'p-1.5 rounded-md transition-all',
          upstream && !fullLineage
            ? 'bg-primary text-primary-contrast shadow-sm'
            : 'text-surface-contrast/50 hover:bg-surface/50',
          fullLineage && 'opacity-20',
        )}
        title="Upstream"
      >
        <ArrowUpCircleIcon className="w-4 h-4" />
      </button>
      <button
        onClick={onToggleDownstream}
        disabled={fullLineage}
        className={makeClassName(
          'p-1.5 rounded-md transition-all',
          downstream && !fullLineage
            ? 'bg-green-600 text-white shadow-sm'
            : 'text-surface-contrast/50 hover:bg-surface/50',
          fullLineage && 'opacity-20',
        )}
        title="Downstream"
      >
        <ArrowDownCircleIcon className="w-4 h-4" />
      </button>
      <button
        onClick={onToggleFullLineage}
        className={makeClassName(
          'p-1.5 rounded-md transition-all',
          fullLineage
            ? 'bg-indigo-600 text-white shadow-sm'
            : 'text-surface-contrast/50 hover:bg-surface/50',
        )}
        title="Full Lineage"
      >
        <Square3Stack3DIcon className="w-4 h-4" />
      </button>
    </div>
  );
}

// Tests Tab Content
export function TestsTabContent({
  selectedTests,
  onToggleTest,
  hasJoins = true,
  hasAggregates = true,
  hasPortalPartitionDaily = true,
  existingDataTests = [],
}: {
  selectedTests: string[];
  onToggleTest: (testId: string) => void;
  hasJoins?: boolean;
  hasAggregates?: boolean;
  hasPortalPartitionDaily?: boolean;
  existingDataTests?: Array<{ type: string; [key: string]: any }>;
}) {
  // Check if a test type is already configured in the model JSON
  const isTestAlreadyConfigured = (testType: string): boolean => {
    return existingDataTests.some((test) => test.type === testType);
  };

  // Determine if a test option should be disabled based on model capabilities
  const isTestDisabled = (testOption: (typeof TEST_OPTIONS)[0]): boolean => {
    // Already configured tests should be disabled
    if (isTestAlreadyConfigured(testOption.id)) {
      return true;
    }
    if (testOption.requiresModel && !hasJoins) {
      return true;
    }
    // Row count tests require portal_partition_daily column
    if (testOption.requiresModel && hasJoins && !hasPortalPartitionDaily) {
      return true;
    }
    if (testOption.requiresColumn && !hasAggregates) {
      return true;
    }
    return false;
  };

  const getDisabledReason = (
    testOption: (typeof TEST_OPTIONS)[0],
  ): string | undefined => {
    if (isTestAlreadyConfigured(testOption.id)) {
      return 'This test is already configured in the model JSON';
    }
    if (testOption.requiresModel && !hasJoins) {
      return 'This test requires a model with joins';
    }
    if (testOption.requiresModel && hasJoins && !hasPortalPartitionDaily) {
      return 'Row count tests require portal_partition_daily column';
    }
    if (testOption.requiresColumn && !hasAggregates) {
      return 'This test requires a model with aggregate columns';
    }
    return undefined;
  };

  return (
    <div className="space-y-0.5 max-h-60 overflow-y-auto">
      {TEST_OPTIONS.map((testOption) => {
        const alreadyConfigured = isTestAlreadyConfigured(testOption.id);
        const disabled = isTestDisabled(testOption);
        const disabledReason = getDisabledReason(testOption);

        return (
          <button
            key={testOption.id}
            onClick={disabled ? undefined : () => onToggleTest(testOption.id)}
            disabled={disabled}
            title={disabledReason}
            className={`w-full flex items-center justify-between px-3 py-1.5 transition-colors text-[12px] font-medium rounded-lg ${
              disabled ? 'opacity-50 cursor-not-allowed' : 'hover:bg-surface/30'
            }`}
          >
            <span
              className={
                alreadyConfigured
                  ? 'text-green-600 font-medium'
                  : disabled
                    ? 'text-surface-contrast/40'
                    : selectedTests.includes(testOption.id)
                      ? 'text-primary font-medium'
                      : 'text-surface-contrast/60'
              }
            >
              {testOption.label}
              {alreadyConfigured && (
                <span className="ml-2 text-[9px] text-green-500">
                  (configured)
                </span>
              )}
            </span>
            <Checkbox
              checked={
                alreadyConfigured || selectedTests.includes(testOption.id)
              }
              onChange={disabled ? () => {} : () => onToggleTest(testOption.id)}
              disabled={disabled}
            />
          </button>
        );
      })}
    </div>
  );
}

// Run Config Tab Content
export function RunConfigTabContent({
  upstream,
  downstream,
  fullLineage,
  upLimit,
  downLimit,
  onToggleUpstream,
  onToggleDownstream,
  onToggleFullLineage,
  onUpLimitChange,
  onDownLimitChange,
}: {
  upstream: boolean;
  downstream: boolean;
  fullLineage: boolean;
  upLimit: number;
  downLimit: number;
  onToggleUpstream: () => void;
  onToggleDownstream: () => void;
  onToggleFullLineage: () => void;
  onUpLimitChange: (value: number) => void;
  onDownLimitChange: (value: number) => void;
}) {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-[10px] font-bold text-surface-contrast/50 uppercase tracking-widest">
          Lineage
        </p>
        <LineageToggles
          upstream={upstream}
          downstream={downstream}
          fullLineage={fullLineage}
          onToggleUpstream={onToggleUpstream}
          onToggleDownstream={onToggleDownstream}
          onToggleFullLineage={onToggleFullLineage}
        />
      </div>
      <div className="space-y-2 pt-2 border-t border-surface/50">
        <p className="text-[10px] font-bold text-surface-contrast/50 uppercase tracking-widest">
          Limit
        </p>
        <div className="space-y-1.5">
          <LimitSpinner
            label="Upstream"
            icon={ArrowUpCircleIcon}
            colorClass="text-primary"
            value={upLimit}
            onChange={onUpLimitChange}
            disabled={fullLineage || !upstream}
          />
          <LimitSpinner
            label="Downstream"
            icon={ArrowDownCircleIcon}
            colorClass="text-green-500"
            value={downLimit}
            onChange={onDownLimitChange}
            disabled={fullLineage || !downstream}
          />
        </div>
        {fullLineage && (
          <p className="text-[9px] text-center text-indigo-400 font-bold uppercase mt-2">
            Full lineage active - limits disabled
          </p>
        )}
      </div>
    </div>
  );
}

// Tab Button Component
export function TabButton({
  label,
  isActive,
  onClick,
}: {
  label: string;
  isActive: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={makeClassName(
        'flex-1 py-2.5 text-[10px] font-bold uppercase tracking-widest transition-colors',
        isActive
          ? 'text-primary border-b-2 border-primary bg-primary/5'
          : 'text-surface-contrast/50 hover:text-surface-contrast/70',
      )}
    >
      {label}
    </button>
  );
}

// Tabbed Config Popover
export function TabbedConfigPopover({
  isOpen,
  onClose,
  onCommit,
  position = 'center',
  activeTab,
  onTabChange,
  testsContent,
  runConfigContent,
}: {
  isOpen: boolean;
  onClose: () => void;
  onCommit: () => void;
  position?: 'left' | 'right' | 'center';
  activeTab: 'tests' | 'runConfig';
  onTabChange: (tab: 'tests' | 'runConfig') => void;
  testsContent: React.ReactNode;
  runConfigContent: React.ReactNode;
}) {
  if (!isOpen) return null;

  const positionClasses = {
    left: 'left-0',
    right: 'right-0',
    center: 'left-1/2 -translate-x-1/2',
  };

  return (
    <div
      onClick={(e) => e.stopPropagation()}
      className={makeClassName(
        'absolute top-full mt-2 w-72 bg-card border border-surface shadow-2xl rounded-xl z-[150] overflow-hidden',
        positionClasses[position],
      )}
    >
      {/* Tabs */}
      <div className="flex border-b border-surface/50">
        <TabButton
          label="Tests"
          isActive={activeTab === 'tests'}
          onClick={() => onTabChange('tests')}
        />
        <TabButton
          label="Run Config"
          isActive={activeTab === 'runConfig'}
          onClick={() => onTabChange('runConfig')}
        />
      </div>

      {/* Tab Content */}
      <div className="p-3">
        {activeTab === 'tests' ? testsContent : runConfigContent}
      </div>

      {/* Footer */}
      <div className="flex items-center gap-2 p-2 border-t border-surface/50 bg-surface/30">
        <Button
          label="Done"
          onClick={onCommit}
          className="flex-1 text-[10px] py-2"
        />
        <Button
          label="Discard"
          variant="secondary"
          onClick={onClose}
          className="flex-1 text-[10px] py-2"
        />
      </div>
    </div>
  );
}

// Inline Lineage Toggle Icons (for model row)
export function InlineLineageToggles({
  upstream,
  downstream,
  fullLineage,
  onToggleUpstream,
  onToggleDownstream,
  onToggleFullLineage,
}: {
  upstream: boolean;
  downstream: boolean;
  fullLineage: boolean;
  onToggleUpstream: () => void;
  onToggleDownstream: () => void;
  onToggleFullLineage: () => void;
}) {
  return (
    <div className="flex items-center gap-2 text-surface-contrast/50">
      <button
        onClick={(e) => {
          e.stopPropagation();
          onToggleUpstream();
        }}
        className={makeClassName(
          'p-0.5 rounded transition-colors',
          upstream ? 'text-primary' : 'hover:text-primary/70',
        )}
        title="Upstream"
      >
        <ArrowUpCircleIcon className="w-4 h-4" />
      </button>
      <button
        onClick={(e) => {
          e.stopPropagation();
          onToggleDownstream();
        }}
        className={makeClassName(
          'p-0.5 rounded transition-colors',
          downstream ? 'text-green-500' : 'hover:text-green-500/70',
        )}
        title="Downstream"
      >
        <ArrowDownCircleIcon className="w-4 h-4" />
      </button>
      <button
        onClick={(e) => {
          e.stopPropagation();
          onToggleFullLineage();
        }}
        className={makeClassName(
          'p-0.5 rounded transition-colors',
          fullLineage ? 'text-indigo-500' : 'hover:text-indigo-500/70',
        )}
        title="Full Lineage"
      >
        <Square3Stack3DIcon className="w-4 h-4" />
      </button>
    </div>
  );
}

// Config Button Component
export function ConfigButton({
  isActive,
  onClick,
  label = 'CONFIG',
}: {
  isActive: boolean;
  onClick: (e: React.MouseEvent) => void;
  label?: string;
}) {
  return (
    <button
      onClick={onClick}
      className={makeClassName(
        'flex items-center gap-1.5 px-2 h-6 border rounded-md transition-all font-bold uppercase tracking-tight text-[10px]',
        isActive
          ? 'bg-primary border-primary text-primary-contrast shadow-sm'
          : 'bg-primary/10 border-primary/20 text-primary/70 hover:text-primary hover:bg-primary/20',
      )}
    >
      <Cog6ToothIcon className="w-3 h-3" />
      <span>{label}</span>
    </button>
  );
}

// Add Tests Button Component
export function AddTestsButton({
  onClick,
  disabled = false,
  disabledTooltip,
}: {
  onClick: (e: React.MouseEvent) => void;
  disabled?: boolean;
  disabledTooltip?: string;
}) {
  return (
    <button
      onClick={disabled ? undefined : onClick}
      disabled={disabled}
      title={disabled ? disabledTooltip : undefined}
      className={`flex items-center gap-1 px-2 h-6 border rounded-md font-bold uppercase tracking-tight text-[10px] transition-all ${
        disabled
          ? 'border-surface/30 bg-surface/10 text-surface-contrast/30 cursor-not-allowed'
          : 'border-primary/20 bg-primary/10 hover:bg-primary/20 text-primary/70 hover:text-primary'
      }`}
    >
      <PlusIcon className="w-2.5 h-2.5" />
      <span>ADD TESTS</span>
    </button>
  );
}

// Test Detail interface for popover
export interface TestDetailItem {
  name: string;
  testType: string;
  columnName?: string;
}

// Test Count Popover - shows test details when hovering on test count badge
export function TestCountPopover({
  testDetails,
  testCount,
}: {
  testDetails: TestDetailItem[];
  testCount: number;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Handle mouse enter with delay for better UX
  const handleMouseEnter = () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    setIsOpen(true);
  };

  // Handle mouse leave with delay to prevent immediate dismissal
  const handleMouseLeave = () => {
    timeoutRef.current = setTimeout(() => {
      setIsOpen(false);
    }, 150);
  };

  // Group tests by type
  const testsByType: Record<string, TestDetailItem[]> = {};
  testDetails.forEach((test) => {
    const type = test.testType || 'other';
    if (!testsByType[type]) {
      testsByType[type] = [];
    }
    testsByType[type].push(test);
  });

  return (
    <div
      className="relative"
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      {/* Badge Trigger */}
      <div className="flex items-center gap-1 px-1.5 py-0.5 text-surface-contrast/50 bg-surface/50 rounded-md cursor-help shrink-0 ml-2">
        <ShieldCheckIcon className="w-3 h-3 text-green-500/80" />
        <span className="text-[10px] font-bold">{testCount}</span>
      </div>

      {/* Popover */}
      {isOpen && testDetails.length > 0 && (
        <div
          className="absolute right-0 top-full mt-1 w-80 bg-card border border-surface shadow-2xl rounded-xl z-[200] overflow-hidden"
          onClick={(e) => e.stopPropagation()}
          onMouseEnter={handleMouseEnter}
          onMouseLeave={handleMouseLeave}
        >
          {testDetails.length > 0 && (
            <>
              <div className="px-3 py-2 border-b border-surface/50 bg-surface/30">
                <p className="text-[10px] font-bold text-surface-contrast/60 uppercase tracking-wider">
                  Configured Tests ({testCount})
                </p>
              </div>
              <div className="max-h-40 overflow-y-auto p-2">
                {Object.entries(testsByType).map(([type, tests]) => (
                  <div key={type} className="mb-3 last:mb-0">
                    <p className="text-[9px] font-bold text-primary uppercase tracking-wider mb-1 px-1">
                      {type.replace(/_/g, ' ')}
                    </p>
                    <div className="space-y-0.5">
                      {tests.map((test, idx) => (
                        <div
                          key={idx}
                          title={test.name}
                          className="flex items-start gap-2 px-2 py-1.5 rounded-md hover:bg-surface/30 transition-colors"
                        >
                          <ShieldCheckIcon className="w-3 h-3 text-green-500/60 shrink-0 mt-0.5" />
                          <div className="min-w-0 flex-1">
                            <p className="text-[11px] font-mono text-surface-contrast/80 truncate">
                              {test.name}
                            </p>
                            {test.columnName && (
                              <p className="text-[9px] text-surface-contrast/50">
                                Column: {test.columnName}
                              </p>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
          {testDetails.length === 0 && (
            <div className="px-3 py-4 text-center">
              <p className="text-[11px] text-surface-contrast/50">
                No tests configured
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
