import { CalendarIcon } from '@heroicons/react/24/outline';
import { ButtonGroup } from '@web/elements';
import { useModelStore } from '@web/stores/useModelStore';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import React, { useCallback, useState } from 'react';

import ErrorMessage from '../components/ErrorMessage';

export const RollupNode: React.FC<NodeProps> = () => {
  // No props needed - everything comes from ModelStore

  // Direct ModelStore integration
  const { modelingState, updateRollupState } = useModelStore();

  const [selectedInterval, setSelectedInterval] = useState<string>(() => {
    // Initialize from ModelStore only
    const storeInterval = modelingState.rollup?.interval;

    if (storeInterval) {
      return storeInterval.charAt(0).toUpperCase() + storeInterval.slice(1);
    }
    return '';
  });
  const [dateExpression, setDateExpression] = useState<string>(
    modelingState.rollup?.dateExpression || '',
  );

  const intervalOptions = ['Day', 'Hour', 'Month', 'Year'];

  const handleIntervalChange = useCallback(
    (value: string) => {
      setSelectedInterval(value);
      // Update ModelStore directly
      updateRollupState({
        interval: value
          ? (value.toLowerCase() as 'day' | 'hour' | 'month' | 'year' | '')
          : '',
        dateExpression,
      });
    },
    [dateExpression, updateRollupState],
  );

  const handleDateExpressionChange = useCallback(
    (value: string) => {
      setDateExpression(value);
      // Update ModelStore directly
      updateRollupState({
        interval: selectedInterval
          ? (selectedInterval.toLowerCase() as
              | 'day'
              | 'hour'
              | 'month'
              | 'year'
              | '')
          : '',
        dateExpression: value.trim() || '',
      });
    },
    [selectedInterval, updateRollupState],
  );

  // Sync local state when modelingState changes
  React.useEffect(() => {
    const storeInterval = modelingState.rollup?.interval;
    const currentInterval = storeInterval
      ? storeInterval.charAt(0).toUpperCase() + storeInterval.slice(1)
      : '';
    const currentDateExpression = modelingState.rollup?.dateExpression || '';

    if (currentInterval !== selectedInterval) {
      setSelectedInterval(currentInterval);
    }
    if (currentDateExpression !== dateExpression) {
      setDateExpression(currentDateExpression);
    }
  }, [
    modelingState.rollup?.interval,
    modelingState.rollup?.dateExpression,
    selectedInterval,
    dateExpression,
  ]);

  return (
    <div
      className={`px-2 py-4 shadow-lg rounded-lg bg-background border-2 min-w-[400px] border-neutral`}
      data-tutorial-id="rollup-node"
    >
      <div className="flex items-center mb-2">
        <CalendarIcon className="w-6 h-6 text-foreground mr-2" />
        <div className="flex-1">
          <div className="text-sm font-bold text-muted-foreground">ROLLUP</div>
        </div>
      </div>

      <div
        className="mb-4"
        data-tutorial-id="rollup-dimensions"
        onMouseDown={(e) => {
          e.stopPropagation();
        }}
        onClick={(e) => {
          e.stopPropagation();
        }}
      >
        <ButtonGroup
          label="SELECT INTERVAL"
          tooltipText="Choose the time interval for data aggregation"
          initialValue={selectedInterval}
          options={intervalOptions}
          onSelect={handleIntervalChange}
        />
      </div>

      {!modelingState.rollup?.interval && (
        <div className="mb-4">
          <ErrorMessage type="rollup_interval" />
        </div>
      )}
      <hr className="border-neutral mb-4" />

      <div className="mb-4">
        <label className="block text-sm font-medium text-foreground mb-2">
          Date-time Expression
        </label>
        <textarea
          value={dateExpression}
          onChange={(e) => handleDateExpressionChange(e.target.value)}
          placeholder="Enter your date-time expression..."
          className="w-full h-24 px-3 py-2 text-sm border border-neutral rounded-md bg-background text-foreground placeholder-muted-foreground resize-none focus:outline-none"
          onMouseDown={(e) => {
            e.stopPropagation();
          }}
          onWheel={(e) => {
            e.stopPropagation();
          }}
        />
      </div>

      <Handle
        type="target"
        position={Position.Top}
        id="input"
        style={{
          background: '#757575',
          border: '1px solid #757575',
          width: '2px',
          height: '2px',
        }}
        className="bg-muted"
      />

      <Handle
        type="source"
        position={Position.Bottom}
        id="output"
        style={{
          background: '#757575',
          border: '1px solid #757575',
          width: '2px',
          height: '2px',
        }}
        className="bg-muted"
      />
    </div>
  );
};
