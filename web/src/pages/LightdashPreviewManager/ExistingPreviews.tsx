import {
  ArrowTopRightOnSquareIcon,
  ChevronDownIcon,
  TrashIcon,
} from '@heroicons/react/24/outline';
import type { LightdashPreview } from '@shared/lightdash/types';
import { makeClassName } from '@web';
import { useApp } from '@web/context';
import { Button, DialogBox, Spinner } from '@web/elements';
import { useLightdashPreviewStore } from '@web/stores/useLightdashPreviewStore';
import { useCallback, useState } from 'react';

// Constants for date calculations
const MILLISECONDS_PER_DAY = 1000 * 60 * 60 * 24;

export function ExistingPreviews() {
  const { api } = useApp();
  const {
    previews,
    isLoadingPreviews,
    setPreviews,
    addLog,
    resetPreviewSection,
    currentPreviewName,
    isRunning,
  } = useLightdashPreviewStore();
  const [expandedPreviews, setExpandedPreviews] = useState<Set<string>>(
    new Set(),
  );
  const [deletingPreview, setDeletingPreview] = useState<string | null>(null);
  const [showDeleteAllDialog, setShowDeleteAllDialog] = useState(false);
  const [showPreviewUpdatingDialog, setShowPreviewUpdatingDialog] =
    useState(false);

  const toggleExpand = (previewName: string) => {
    const newExpanded = new Set(expandedPreviews);
    if (newExpanded.has(previewName)) {
      newExpanded.delete(previewName);
    } else {
      newExpanded.add(previewName);
    }
    setExpandedPreviews(newExpanded);
  };

  const handleOpenPreview = useCallback(
    async (url: string) => {
      try {
        await api.post({
          type: 'framework-open-external-url',
          request: { url },
        });
      } catch (error) {
        console.error('Error opening preview URL:', error);
        addLog({
          level: 'error',
          message: 'Failed to open preview URL',
          timestamp: new Date().toISOString(),
        });
      }
    },
    [api, addLog],
  );

  const handleDeletePreview = useCallback(
    async (previewName: string) => {
      // Check if this preview is currently being updated
      if (isRunning && currentPreviewName === previewName) {
        setShowPreviewUpdatingDialog(true);
        return;
      }

      try {
        setDeletingPreview(previewName);

        const response = await api.post({
          type: 'lightdash-stop-preview',
          request: { previewName },
        });

        if (response.success) {
          // Remove from local state
          setPreviews(previews.filter((p) => p.name !== previewName));
          addLog({
            level: 'success',
            message: `✔ Preview "${previewName}" deleted successfully`,
            timestamp: new Date().toISOString(),
          });

          // Reset the preview section only if this was the current preview
          if (currentPreviewName === previewName) {
            resetPreviewSection();
          }

          // Show success notification
          await api.post({
            type: 'framework-show-message',
            request: {
              message: `Preview "${previewName}" deleted successfully`,
              type: 'success',
            },
          });
        } else {
          throw new Error(response.error || 'Failed to delete preview');
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : 'Unknown error';
        addLog({
          level: 'error',
          message: `✖ Error deleting preview: ${errorMessage}`,
          timestamp: new Date().toISOString(),
        });
      } finally {
        setDeletingPreview(null);
      }
    },
    [
      api,
      previews,
      setPreviews,
      addLog,
      resetPreviewSection,
      currentPreviewName,
      isRunning,
    ],
  );

  const handleDeleteAllStale = useCallback(() => {
    const stalePreviews = previews.filter((p) => p.status === 'inactive');

    if (stalePreviews.length === 0) {
      addLog({
        level: 'info',
        message: 'No stale previews to delete',
        timestamp: new Date().toISOString(),
      });
      return;
    }

    setShowDeleteAllDialog(true);
  }, [previews, addLog]);

  const confirmDeleteAllStale = useCallback(async () => {
    const stalePreviews = previews.filter((p) => p.status === 'inactive');

    setShowDeleteAllDialog(false);

    for (const preview of stalePreviews) {
      await handleDeletePreview(preview.name);
    }

    // Show success notification for bulk delete
    if (stalePreviews.length > 0) {
      await api.post({
        type: 'framework-show-message',
        request: {
          message: `Successfully deleted ${stalePreviews.length} stale preview${stalePreviews.length > 1 ? 's' : ''}`,
          type: 'success',
        },
      });
    }
  }, [previews, handleDeletePreview, api]);

  if (isLoadingPreviews) {
    return (
      <div className="flex flex-col items-center justify-center p-8">
        <Spinner size={32} label="Loading previews..." />
      </div>
    );
  }

  const stalePreviewsCount = previews.filter(
    (p) => p.status === 'inactive',
  ).length;

  return (
    <>
      <div className="flex flex-col border-t border-surface max-h-[30%]">
        {/* Header */}
        <div className="p-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold">
            Existing Previews ({previews.length})
          </h3>
          {previews.some((p) => p.status === 'inactive') && (
            <Button
              onClick={handleDeleteAllStale}
              variant="iconButton"
              icon={<TrashIcon className="w-4 h-4" />}
              label="Delete stale previews"
            />
          )}
        </div>

        {/* Previews List */}
        <div className="overflow-y-auto p-4 flex flex-col gap-2">
          {previews.length === 0 ? (
            <div className="text-center text-gray-500 py-8">
              No existing previews found
            </div>
          ) : (
            <>
              {previews.map((preview) => {
                const isExpanded = expandedPreviews.has(preview.name);
                const isDeleting = deletingPreview === preview.name;
                return (
                  <PreviewItem
                    key={preview.name}
                    preview={preview}
                    toggleExpand={toggleExpand}
                    handleOpenPreview={(url) => void handleOpenPreview(url)}
                    handleDeletePreview={(name) =>
                      void handleDeletePreview(name)
                    }
                    isDeleting={isDeleting}
                    isExpanded={isExpanded}
                  />
                );
              })}
            </>
          )}
        </div>
      </div>

      {/* Confirmation Dialog */}
      <DialogBox
        open={showDeleteAllDialog}
        title="Delete Stale Previews"
        description={`Are you sure you want to delete ${stalePreviewsCount} stale preview(s)? This action cannot be undone.`}
        confirmCTALabel="Delete All"
        discardCTALabel="Cancel"
        onConfirm={() => void confirmDeleteAllStale()}
        onDiscard={() => setShowDeleteAllDialog(false)}
      />

      {/* Preview Updating Dialog */}
      <DialogBox
        open={showPreviewUpdatingDialog}
        title="Preview is being updated"
        description={`The preview "${currentPreviewName}" is currently being updated. Please wait until the process completes before attempting to delete it.`}
        confirmCTALabel="OK"
        onConfirm={() => setShowPreviewUpdatingDialog(false)}
      />
    </>
  );
}

function PreviewItem({
  preview,
  isExpanded,
  toggleExpand,
  handleOpenPreview,
  handleDeletePreview,
  isDeleting,
}: {
  preview: LightdashPreview;
  isExpanded: boolean;
  toggleExpand: (previewName: string) => void;
  handleOpenPreview: (url: string) => void;
  handleDeletePreview: (previewName: string) => void;
  isDeleting: boolean;
}) {
  // Calculate difference in calendar days (ignoring time of day)
  const createdDate = new Date(preview.createdAt);
  const today = new Date();
  const createdDateOnly = new Date(
    createdDate.getFullYear(),
    createdDate.getMonth(),
    createdDate.getDate(),
  );
  const todayOnly = new Date(
    today.getFullYear(),
    today.getMonth(),
    today.getDate(),
  );
  const daysAgo = Math.floor(
    (todayOnly.getTime() - createdDateOnly.getTime()) / MILLISECONDS_PER_DAY,
  );

  const getDaysAgoText =
    daysAgo === 0
      ? 'Created today'
      : daysAgo === 1
        ? 'Created 1 day ago'
        : `Created ${daysAgo} days ago`;

  return (
    <div
      key={preview.name}
      className="border border-surface rounded-lg bg-background"
    >
      {/* Preview Header */}
      <div
        className="p-3 cursor-pointer flex items-center justify-between"
        onClick={() => toggleExpand(preview.name)}
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-medium truncate">{preview.name}</span>
            <span
              className={makeClassName(
                'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium',
                preview.status === 'active'
                  ? 'bg-green-100 text-green-800'
                  : 'bg-red-100 text-red-800',
              )}
            >
              {preview.status.charAt(0).toUpperCase() + preview.status.slice(1)}
            </span>
          </div>
          <div className="text-xs text-background-contrast">
            {getDaysAgoText}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            title="Open preview"
            onClick={(e) => {
              e.stopPropagation();
              handleOpenPreview(preview.url);
            }}
            variant="iconButton"
            icon={<ArrowTopRightOnSquareIcon className="w-4 h-4" />}
            label=""
          />
          <Button
            title="Delete preview"
            onClick={(e) => {
              e.stopPropagation();
              handleDeletePreview(preview.name);
            }}
            disabled={isDeleting}
            variant="iconButton"
            icon={
              isDeleting ? (
                <Spinner size={16} inline={true} />
              ) : (
                <TrashIcon className="w-4 h-4" />
              )
            }
            label=""
          />
          <Button
            title={isExpanded ? 'Collapse' : 'Expand'}
            onClick={(e) => {
              e.stopPropagation();
              toggleExpand(preview.name);
            }}
            variant="iconButton"
            icon={<ChevronDownIcon className="w-4 h-4" />}
            label=""
            className={makeClassName(
              'p-2 hover:bg-gray-200 dark:hover:bg-gray-700 rounded',
              isExpanded ? 'rotate-180' : '',
            )}
          />
        </div>
      </div>

      {/* Preview Details (Expanded) */}
      {isExpanded && (
        <div className="p-3 border-t border-surface bg-surface">
          <div className="flex flex-col gap-2 text-sm">
            <div className="flex items-center gap-2">
              <span className="font-medium">URL:</span>
              <Button
                onClick={(e) => {
                  e.stopPropagation();
                  handleOpenPreview(preview.url);
                }}
                variant="link"
                label={preview.url}
                className="text-primary hover:underline p-[0px!important] font-normal"
              />
            </div>
            <div className="flex items-center gap-2">
              <span className="font-medium">
                Models ({preview.models.length}):
              </span>
              <div className="flex flex-wrap gap-1">
                {preview.models.map((model) => (
                  <span
                    key={model}
                    className="inline-flex items-center px-2 py-0.5 rounded text-xs font-mono bg-tag text-tag-contrast"
                  >
                    {model}
                  </span>
                ))}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <span className="font-medium">Created:</span>
              {createdDate.toLocaleString()}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
