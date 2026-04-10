import { ClockIcon } from '@heroicons/react/24/outline';
import { InputText } from '@web/elements';
import { useModelStore } from '@web/stores/useModelStore';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import React, { useCallback, useEffect, useState } from 'react';

import ErrorMessage from '../components/ErrorMessage';

export interface AvailableModel {
  label: string;
  value: string;
}

export const LookbackNode: React.FC<NodeProps> = () => {
  // Direct ModelStore integration
  const { modelingState, updateLookbackState } = useModelStore();

  const [days, setDays] = useState<string>(() => {
    // Initialize from ModelStore only
    return modelingState?.lookback?.days > 0
      ? modelingState?.lookback?.days?.toString()
      : '';
  });

  const handleDaysChange = useCallback(
    (value: string) => {
      setDays(value);

      // Update ModelStore directly
      const daysNumber = value.trim() === '' ? 0 : parseInt(value) || 0;
      updateLookbackState({
        days: daysNumber,
        exclude_event_date:
          modelingState?.lookback?.exclude_event_date || false,
      });
    },
    [updateLookbackState, modelingState?.lookback?.exclude_event_date],
  );

  // Sync local state when modelingState changes
  useEffect(() => {
    const currentDays =
      modelingState?.lookback?.days > 0
        ? modelingState.lookback?.days.toString()
        : '';
    if (currentDays !== days) {
      setDays(currentDays);
    }
  }, [modelingState?.lookback?.days, days]);

  return (
    <div
      className="px-4 py-6 rounded-lg border-2 min-w-[400px] border-neutral bg-background shadow-lg"
      data-tutorial-id="lookback-node"
    >
      <div className="flex items-center mb-4">
        <ClockIcon className="w-6 h-6 text-foreground mr-2" />
        <div className="flex-1">
          <div className="text-sm font-medium text-foreground">LOOKBACK</div>
        </div>
      </div>

      <div className="flex items-center justify-between">
        <div className="text-sm font-medium text-muted-foreground">Days</div>
        <div className="w-20" data-tutorial-id="lookback-days-input">
          <InputText
            value={days}
            onChange={(e) => handleDaysChange(e.target.value)}
            placeholder="7"
            className="text-right border-0 bg-transparent p-0 text-sm font-medium text-foreground focus:ring-0 h-auto mt-0"
          />
        </div>
      </div>

      {!modelingState.lookback?.days && <ErrorMessage type="lookback_days" />}

      <Handle
        type="target"
        position={Position.Top}
        id="input"
        style={{
          background: '#757575',
          border: '1px solid #757575',
          width: '8px',
          height: '8px',
        }}
      />

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
      />
    </div>
  );
};
