import { makeClassName } from '@web';
import { useApp } from '@web/context';
import { useEnvironment } from '@web/context';
import { Button, InputText, Spinner } from '@web/elements';
import { Message } from '@web/elements/Message';
import { useLightdashPreviewStore } from '@web/stores/useLightdashPreviewStore';
import { useCallback, useEffect, useRef, useState } from 'react';

export function PreviewControl() {
  const { api } = useApp();
  const { vscode } = useEnvironment();
  const {
    defaultPreviewName,
    previewNameSuffix,
    setPreviewNameSuffix,
    selectedModels,
    isRunning,
    setIsRunning,
    logs,
    addLog,
    clearLogs,
    currentPreviewUrl,
    setCurrentPreviewUrl,
    setCurrentPreviewName,
    resetPreviewSection,
  } = useLightdashPreviewStore();

  const [showFullLogs, setShowFullLogs] = useState(false);

  const logsEndRef = useRef<HTMLDivElement>(null);

  // Filter logs for display
  const progressSteps = logs.filter((log) => log.isProgress);
  const regularLogs = logs.filter((log) => !log.isProgress);

  // Auto-scroll to bottom when logs update
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  const fullPreviewName = previewNameSuffix.trim()
    ? `${defaultPreviewName}-${previewNameSuffix.trim()}`
    : defaultPreviewName;

  const handleRunPreview = useCallback(async () => {
    if (selectedModels.size === 0) {
      addLog({
        level: 'error',
        message: 'Please select at least one model',
        timestamp: new Date().toISOString(),
      });
      return;
    }

    try {
      setIsRunning(true);
      clearLogs();
      setCurrentPreviewUrl(null);
      setCurrentPreviewName(fullPreviewName);

      addLog({
        level: 'info',
        message: `Step 1/5: Starting preview: ${fullPreviewName}`,
        timestamp: new Date().toISOString(),
        isProgress: true,
      });
      addLog({
        level: 'info',
        message: `Starting preview: ${fullPreviewName}`,
        timestamp: new Date().toISOString(),
      });
      addLog({
        level: 'info',
        message: `Selected models: ${Array.from(selectedModels).join(', ')}`,
        timestamp: new Date().toISOString(),
      });

      const response = await api.post({
        type: 'lightdash-start-preview',
        request: {
          previewName: fullPreviewName,
          selectedModels: Array.from(selectedModels),
        },
      });

      if (response.success && response.url) {
        setCurrentPreviewUrl(response.url);
      } else {
        // Error is already shown via webview logs from backend
        throw new Error(response.error || 'Failed to generate preview');
      }
    } catch (error) {
      // Error is already shown via webview logs from backend
      // Add a failure progress step for prominent error indication
      addLog({
        level: 'error',
        message: `✖ Preview generation failed`,
        timestamp: new Date().toISOString(),
        isProgress: true,
        isPreviewSuccess: false,
      });
      // Just log to console for debugging
      console.error('Preview generation error:', error);
    } finally {
      setIsRunning(false);
    }
  }, [
    selectedModels,
    fullPreviewName,
    api,
    setIsRunning,
    clearLogs,
    setCurrentPreviewUrl,
    setCurrentPreviewName,
    addLog,
  ]);

  const handleOpenPreview = useCallback(async () => {
    if (currentPreviewUrl) {
      try {
        await api.post({
          type: 'framework-open-external-url',
          request: { url: currentPreviewUrl },
        });
      } catch (error) {
        console.error('Error opening preview URL:', error);
        addLog({
          level: 'error',
          message: 'Failed to open preview URL',
          timestamp: new Date().toISOString(),
        });
      }
    }
  }, [currentPreviewUrl, api, addLog]);

  const handleCopyLink = useCallback(() => {
    if (currentPreviewUrl) {
      vscode?.postMessage({
        type: 'copy-to-clipboard',
        text: currentPreviewUrl,
      });
      addLog({
        level: 'success',
        message: '✔ Link copied to clipboard',
        timestamp: new Date().toISOString(),
      });
    }
  }, [currentPreviewUrl, addLog, vscode]);

  const handleRunAgain = useCallback(() => {
    resetPreviewSection();
  }, [resetPreviewSection]);

  const getLogColor = (level: string) => {
    switch (level) {
      case 'success':
        return 'text-green-600';
      case 'error':
        return 'text-red-600';
      case 'warning':
        return 'text-yellow-600';
      default:
        return 'text-surface-contrast';
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="p-4 border-b border-surface flex flex-col gap-2">
        <h3 className="text-lg font-semibold">Preview Control</h3>

        {/* Preview Name Input */}

        <p className="block text-sm font-medium">
          Preview Name:{' '}
          <span className="font-normal text-background-contrast">
            {fullPreviewName}
          </span>
        </p>
        <InputText
          value={previewNameSuffix}
          onChange={(e) => setPreviewNameSuffix(e.target.value)}
          placeholder="Optional suffix (e.g., 'my-test')"
          disabled={isRunning}
          className="w-full"
        />

        {/* Action Buttons */}
        {!currentPreviewUrl ? (
          <Button
            label={isRunning ? 'Running Preview...' : 'Run Lightdash Preview'}
            onClick={() => void handleRunPreview()}
            disabled={selectedModels.size === 0 || isRunning}
            className="w-full"
            variant="primary"
            icon={isRunning && <Spinner size={14} inline={true} />}
          />
        ) : (
          <div className="flex flex-col gap-2">
            <Button
              label="Open Lightdash Preview"
              onClick={() => void handleOpenPreview()}
              className="flex-1"
            />
            <div className="flex gap-2">
              <Button
                label="Copy Preview Link"
                onClick={handleCopyLink}
                variant="secondary"
                className="flex-1"
              />
              <Button
                label="Start New Preview"
                onClick={handleRunAgain}
                variant="link"
                className="flex-1"
              />
            </div>
          </div>
        )}
      </div>

      {/* Process Logs */}
      <div className="flex-1 flex flex-col gap-4 p-4 max-h-[calc(100%-225px)]">
        <div className="flex justify-between items-center">
          <h4 className="text-sm font-semibold">Process Logs</h4>
          {logs.length > 0 && (
            <Button
              onClick={() => setShowFullLogs(!showFullLogs)}
              variant="link"
              label={showFullLogs ? 'Show Progress Only' : 'View complete logs'}
              className="text-xs"
            />
          )}
        </div>

        {logs.length === 0 ? (
          <div className="flex-1 flex items-center justify-center text-gray-500 text-sm">
            No logs yet. Click "Run Lightdash Preview" to start.
          </div>
        ) : (
          <div
            className={makeClassName(
              'flex-1 font-mono overflow-y-auto text-xs',
              !showFullLogs && 'grid grid-cols-4 gap-2',
            )}
            style={{
              alignContent: 'start',
            }}
          >
            {!showFullLogs ? (
              <>
                {/* Show progress steps only */}
                {progressSteps.map((step, index) => (
                  <Message
                    key={index}
                    variant={step.level as 'success' | 'error' | 'info'}
                    className="grid grid-cols-subgrid items-center gap-2 col-span-4"
                  >
                    <span className="text-gray-500 col-span-1">
                      [{new Date(step.timestamp).toLocaleTimeString()}]
                    </span>
                    <span className="font-semibold col-span-3 flex items-center gap-2 overflow-hidden break-words">
                      {step.message}
                      {index === progressSteps.length - 1 && isRunning && (
                        <Spinner size={12} inline={true} />
                      )}
                    </span>
                  </Message>
                ))}
              </>
            ) : (
              <>
                {/* Show complete log */}
                {regularLogs.map((log, index) => (
                  <div key={index} className={getLogColor(log.level)}>
                    <span className="text-gray-500 pr-2">
                      [{new Date(log.timestamp).toLocaleTimeString()}]
                    </span>
                    {log.message}
                  </div>
                ))}
              </>
            )}
            <div ref={logsEndRef} />
          </div>
        )}
      </div>
    </div>
  );
}
