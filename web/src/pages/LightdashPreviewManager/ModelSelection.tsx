import { makeClassName } from '@web';
import { Button, Checkbox, InputText, Spinner } from '@web/elements';
import { useLightdashPreviewStore } from '@web/stores/useLightdashPreviewStore';
import { useMemo, useState } from 'react';

export function ModelSelection() {
  const [searchTerm, setSearchTerm] = useState('');
  const {
    models,
    selectedModels,
    isLoadingModels,
    toggleModel,
    selectAllModels,
    deselectAllModels,
  } = useLightdashPreviewStore();

  /**
   * Filtered models are models that match the search term in the name, tags, or description.
   * If no search term, returns all models.
   */
  const filteredModels = useMemo(() => {
    if (!searchTerm) return models;
    const search = searchTerm.toLowerCase();
    return models.filter(
      (model) =>
        model.name.toLowerCase().includes(search) ||
        model.tags.some((tag) => tag.toLowerCase().includes(search)) ||
        model.description?.toLowerCase().includes(search),
    );
  }, [models, searchTerm]);

  if (isLoadingModels) {
    return (
      <div className="flex flex-col items-center justify-center p-8">
        <Spinner size={32} label="Loading models..." />
      </div>
    );
  }

  return (
    <div className="flex flex-col h-[70%] max-h-[70%]">
      {/* Header */}
      <div className="p-4 border-b border-surface flex flex-col gap-2">
        <div className="flex justify-between items-center gap-2">
          <h3 className="text-lg font-semibold">
            Mart Models ({selectedModels.size} selected)
          </h3>
          <div className="flex gap-2">
            <Button
              onClick={selectAllModels}
              variant="link"
              label="Select All"
              disabled={models.length === 0}
            ></Button>
            <Button
              onClick={() => deselectAllModels()}
              variant="link"
              label="Deselect All"
              disabled={selectedModels.size === 0}
            />
          </div>
        </div>
        <InputText
          placeholder="Search models..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="mb-3"
        />
      </div>

      {/* Model List */}
      <div className="overflow-y-auto p-4 flex flex-col gap-2">
        {filteredModels.length === 0 ? (
          <div className="text-center text-gray-500 py-8">
            {models.length === 0
              ? 'No mart models found'
              : 'No models match your search'}
          </div>
        ) : (
          <>
            {filteredModels.map((model) => {
              const isSelected = selectedModels.has(model.name);
              const hasLightdashTag = model.tags.includes('lightdash');
              const hasLightdashExploreTag =
                model.tags.includes('lightdash-explore');

              return (
                <div
                  key={model.name}
                  className={makeClassName(
                    'p-2 border rounded-lg cursor-pointer transition-colors flex items-center gap-2',
                    isSelected
                      ? 'border-blue-500 bg-message-info'
                      : 'border-neutral bg-background',
                  )}
                >
                  <Checkbox
                    checked={isSelected}
                    onChange={() => toggleModel(model.name)}
                  />
                  <div className="flex flex-col gap-1 w-[calc(100%-32px)]">
                    {/* Model name and tags */}
                    <div className="font-medium text-sm flex items-center gap-2 flex-wrap w-full">
                      <span className="text-sm overflow-hidden break-words">
                        {model.name}
                      </span>
                      {(hasLightdashTag || hasLightdashExploreTag) && (
                        <div className="flex gap-1">
                          {hasLightdashTag && (
                            <span className="px-2 py-0.5 rounded text-xs bg-green-100 text-green-800">
                              lightdash
                            </span>
                          )}
                          {hasLightdashExploreTag && (
                            <span className="px-2 py-0.5 rounded text-xs bg-purple-100 text-purple-800">
                              lightdash-explore
                            </span>
                          )}
                        </div>
                      )}
                    </div>
                    {/* Model description */}
                    {model.description && (
                      <div className="text-sm text-surface-contrast line-clamp-2">
                        {model.description}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}
