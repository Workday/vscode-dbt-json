import { PencilIcon, TrashIcon } from '@heroicons/react/24/outline';
import type { SchemaModelLightdash } from '@shared/schema/types/model.schema';
import { Button, Tooltip } from '@web/elements';
import type { LightdashMetricWithId } from '@web/stores/useModelStore';
import React, { useCallback, useState } from 'react';

import { AddLightdashMetric } from './AddLightdashMetric';

interface LightdashMetricsProps {
  lightdashConfig: SchemaModelLightdash;
  updateLightdashState: (state: SchemaModelLightdash) => void;
}

export const LightdashMetrics: React.FC<LightdashMetricsProps> = ({
  lightdashConfig,
  updateLightdashState,
}) => {
  // Modal state
  const [isModalOpen, setIsModalOpen] = useState<boolean>(false);
  const [editingMetric, setEditingMetric] =
    useState<LightdashMetricWithId | null>(null);

  // Metrics management functions
  const handleAddMetric = useCallback(() => {
    setEditingMetric(null);
    setIsModalOpen(true);
  }, []);

  const handleEditMetric = useCallback((metric: LightdashMetricWithId) => {
    setEditingMetric(metric);
    setIsModalOpen(true);
  }, []);

  const handleDeleteMetric = useCallback(
    (metricId: string) => {
      const updatedMetrics =
        (lightdashConfig.metrics as LightdashMetricWithId[])?.filter(
          (m) => m.id !== metricId,
        ) || [];

      updateLightdashState({
        ...lightdashConfig,
        metrics: updatedMetrics,
      });
    },
    [lightdashConfig, updateLightdashState],
  );

  const handleSaveMetric = useCallback(
    (metric: LightdashMetricWithId) => {
      const existingMetrics =
        (lightdashConfig.metrics as LightdashMetricWithId[]) || [];
      let updatedMetrics;

      if (editingMetric) {
        // Update existing metric
        updatedMetrics = existingMetrics.map((m) =>
          m.id === editingMetric.id ? metric : m,
        );
      } else {
        // Add new metric
        updatedMetrics = [...existingMetrics, metric];
      }

      updateLightdashState({
        ...lightdashConfig,
        metrics: updatedMetrics,
      });

      setIsModalOpen(false);
      setEditingMetric(null);
    },
    [lightdashConfig, editingMetric, updateLightdashState],
  );

  const handleCloseModal = useCallback(() => {
    setIsModalOpen(false);
    setEditingMetric(null);
  }, []);

  return (
    <div className="mb-4" data-tutorial-id="lightdash-metrics">
      <div className="text-sm font-medium text-foreground mb-2 flex items-center gap-2">
        Metrics
        <Tooltip
          content="Define custom metrics that will be available across all columns in your Lightdash model"
          variant="outline"
        />
      </div>

      {(lightdashConfig.metrics?.length || 0) === 0 ? (
        <div className="text-center text-sm text-muted-foreground py-4 mb-2">
          No metrics added yet
        </div>
      ) : (
        <div className="space-y-2 mb-2">
          {(lightdashConfig.metrics as LightdashMetricWithId[])?.map(
            (metric: LightdashMetricWithId) => (
              <div
                key={metric.id}
                className="flex items-center justify-between p-3 border border-border rounded-md bg-muted/20"
              >
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-medium text-foreground">
                    Name: {metric.name}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    Type: {metric.type}
                  </div>
                  {metric.description && (
                    <div className="text-xs text-muted-foreground truncate">
                      Description: {metric.description}
                    </div>
                  )}
                  {metric.sql && (
                    <div className="text-xs text-muted-foreground truncate font-mono">
                      SQL: {metric.sql}
                    </div>
                  )}
                </div>
                <div className="flex items-center gap-1">
                  <Button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleEditMetric(metric);
                    }}
                    onMouseDown={(e) => e.stopPropagation()}
                    className="p-1 hover:bg-muted rounded transition-colors"
                    title="Edit metric"
                    variant="iconButton"
                    label=""
                    icon={
                      <PencilIcon className="w-4 h-4 text-muted-foreground hover:text-foreground" />
                    }
                  />
                  <Button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteMetric(metric.id);
                    }}
                    onMouseDown={(e) => e.stopPropagation()}
                    className="p-1 hover:bg-muted rounded transition-colors"
                    title="Delete metric"
                    icon={
                      <TrashIcon className="w-4 h-4 text-muted-foreground hover:text-red-500" />
                    }
                    variant="iconButton"
                    label=""
                  />
                </div>
              </div>
            ),
          )}
        </div>
      )}

      <Button
        className="w-full py-1 px-4 border border-primary font-bold text-primary text-sm rounded-md bg-transparent hover:bg-primary/5 transition-colors flex items-center justify-center gap-2"
        onMouseDown={(e) => e.stopPropagation()}
        onClick={(e) => {
          e.stopPropagation();
          handleAddMetric();
        }}
        variant="link"
        label="+ Add metric"
      />

      {/* Add/Edit Metric Modal */}
      {isModalOpen && (
        <AddLightdashMetric
          isOpen={isModalOpen}
          onClose={handleCloseModal}
          onSave={handleSaveMetric}
          editingMetric={editingMetric}
        />
      )}
    </div>
  );
};
