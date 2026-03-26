import { CircleStackIcon } from '@heroicons/react/24/outline';
import DataSearchIcon from '@web/assets/icons/data-search.svg?react';
import { Button, Tooltip } from '@web/elements';
import { SelectSingle } from '@web/elements/SelectSingle';
import React from 'react';

import type { AvailableModel } from '../nodes/SelectNode';
import ErrorMessage from './ErrorMessage';

interface JoinHeaderProps {
  modelOptions: AvailableModel[];
  selectedModel: AvailableModel | null;
  modelsLoading: boolean;
  error: string | null;
  currentJoin: { model?: string; cte?: string } | null;
  onModelChange: (option: AvailableModel | null) => void;
  joinTypeOptions: AvailableModel[];
  selectedJoinType: AvailableModel | null;
  onJoinTypeChange: (option: AvailableModel | null) => void;
  onOpenDataExplorer?: () => void;
}

export const JoinHeader: React.FC<JoinHeaderProps> = ({
  modelOptions,
  selectedModel,
  modelsLoading,
  error,
  currentJoin,
  onModelChange,
  joinTypeOptions,
  selectedJoinType,
  onJoinTypeChange,
  onOpenDataExplorer,
}) => {
  return (
    <>
      <div className="flex-1 mb-4">
        <div className="flex items-center justify-between mb-2 pr-8">
          <div className="text-xs text-muted-foreground pl-1 flex items-center gap-1">
            JOIN SELECT FROM
            <Tooltip
              content="Choose a model to join with your base model. Define how tables connect using join conditions."
              variant="outline"
            />
          </div>

          <Button
            onClick={(e) => {
              e.stopPropagation();
              onOpenDataExplorer?.();
            }}
            disabled={!selectedModel}
            variant="iconButton"
            label="DATA EXPLORER"
            icon={<DataSearchIcon className="w-3 h-3" />}
            iconLabelClassName="text-xs"
            className="text-tiny bg-primary text-white hover:text-white font-bold p-0.5 px-2"
          />
        </div>

        <div
          className="flex items-center gap-2 border border-border rounded-lg pl-2"
          data-tutorial-id="join-model-select"
          onClick={(e) => {
            e.stopPropagation();
          }}
          onMouseDown={(e) => {
            e.stopPropagation();
          }}
          onWheel={(e) => {
            e.stopPropagation();
          }}
        >
          <CircleStackIcon className="w-4 h-4 text-foreground flex-shrink-0" />
          {modelsLoading ? (
            <div className="text-sm text-muted-foreground py-1 pl-1 flex-1">
              Loading models...
            </div>
          ) : (
            <div className="flex-1">
              <SelectSingle
                label=""
                options={modelOptions}
                value={selectedModel}
                onChange={(option) => {
                  onModelChange(option);
                }}
                placeholder="Start typing the model name..."
                onBlur={() => {}}
                error={error || undefined}
                disabled={modelsLoading}
                className="w-full bg-transparent h-8 py-1 pl-1 text-background-contrast text-sm ring-0 border-0 shadow-none focus:ring-0 focus:border-0 focus:outline-none"
              />
            </div>
          )}
        </div>
        {(!currentJoin?.model ||
          currentJoin?.model === '' ||
          currentJoin?.model === null) && (
          <div className="my-2">
            <ErrorMessage type="join_model_name" />
          </div>
        )}
      </div>

      <div className="flex items-center justify-between bg-card border border-surface px-3 py-2 rounded-md mb-4">
        <span className="text-sm font-medium text-surface-contrast">
          Join Type
        </span>
        <div
          className="w-32"
          data-tutorial-id="join-type-select"
          onClick={(e) => e.stopPropagation()}
          onMouseDown={(e) => e.stopPropagation()}
          onWheel={(e) => e.stopPropagation()}
        >
          <SelectSingle
            label=""
            options={joinTypeOptions}
            value={selectedJoinType}
            onChange={onJoinTypeChange}
            placeholder="Type"
            onBlur={() => {}}
            className="w-full rounded-md border-0 bg-transparent h-6 py-1 pl-2 pr-6 text-sm focus:ring-2 focus:ring-inset focus:ring-primary"
          />
        </div>
      </div>
    </>
  );
};
