import { ChartBarIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { Button, Checkbox, Tooltip } from '@web/elements';
import { useModelStore } from '@web/stores/useModelStore';
import type { NodeProps } from '@xyflow/react';
import { Handle, Position } from '@xyflow/react';
import React, { useCallback, useMemo } from 'react';

import { LightdashMetrics } from '../components/LightdashMetrics';
import { LightdashMetricsIncludeExclude } from '../components/LightdashMetricsIncludeExclude';
import { LightdashTableProperties } from '../components/LightdashTableProperties';
import { ActionType } from '../types';

export const LightdashNode: React.FC<NodeProps> = () => {
  // Direct ModelStore integration
  const { modelingState, updateLightdashState, setPendingRemovalAction } =
    useModelStore();

  // Get current lightdash config from modelingState
  const lightdashConfig = useMemo(() => {
    return (
      modelingState.lightdash || {
        table: { group_label: '', label: '' },
        metrics: [],
        metrics_exclude: [],
        metrics_include: [],
      }
    );
  }, [modelingState.lightdash]);

  const handleRemoveLightdash = useCallback(() => {
    setPendingRemovalAction(ActionType.LIGHTDASH);
  }, [setPendingRemovalAction]);

  return (
    <div
      className="nopan px-4 py-4 shadow-lg rounded-md bg-background border-2 min-w-[750px] border-neutral relative"
      data-tutorial-id="lightdash-node"
    >
      {/* Handles */}
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
        className="bg-muted"
      />

      {/* Close button */}
      <Button
        onClick={handleRemoveLightdash}
        className="absolute top-2 right-2 p-1 rounded-full hover:bg-muted transition-colors"
        onMouseDown={(e) => e.stopPropagation()}
        label=""
        variant="iconButton"
        icon={
          <XMarkIcon className="w-4 h-4 text-muted-foreground hover:text-foreground" />
        }
      />

      {/* Header */}
      <div className="flex items-center mb-4">
        <ChartBarIcon className="w-6 h-6 text-foreground mr-2" />
        <div className="flex-1 flex items-center gap-2">
          <div className="text-sm font-bold text-muted-foreground">
            LIGHTDASH
          </div>
          <Tooltip
            content="Configure Lightdash visualization and BI tool settings for your model, including table properties, metrics, and metric filters"
            variant="outline"
          />
        </div>
      </div>

      {/* Case Sensitivity */}
      <div className="mb-4 flex items-center gap-2">
        <Checkbox
          checked={lightdashConfig.case_sensitive ?? false}
          onChange={(checked) =>
            updateLightdashState({
              case_sensitive: checked as boolean,
            })
          }
          label="Case sensitive"
        />
        <Tooltip
          content="When enabled, string comparisons and filters in Lightdash will be case-sensitive"
          variant="outline"
        />
      </div>

      <hr className="border-neutral mb-4" />

      {/* Metrics Section */}
      <LightdashMetrics
        lightdashConfig={lightdashConfig}
        updateLightdashState={updateLightdashState}
      />

      <hr className="border-neutral mb-4" />

      {/* Table Properties Section */}
      <LightdashTableProperties
        lightdashConfig={lightdashConfig}
        updateLightdashState={updateLightdashState}
      />

      <hr className="border-neutral mb-4" />

      {/* Metrics to Include Section */}
      <LightdashMetricsIncludeExclude
        type="include"
        lightdashConfig={lightdashConfig}
        updateLightdashState={updateLightdashState}
      />

      <hr className="border-neutral mb-4" />

      {/* Metrics to Exclude Section */}
      <LightdashMetricsIncludeExclude
        type="exclude"
        lightdashConfig={lightdashConfig}
        updateLightdashState={updateLightdashState}
      />

      {/* Bottom Handle */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="output"
        className="w-2 h-2"
      />
    </div>
  );
};
