import { useApp } from '@web/context';
import { useEnvironment } from '@web/context';
import { ExistingPreviews } from '@web/pages/LightdashPreviewManager/ExistingPreviews';
import { ModelSelection } from '@web/pages/LightdashPreviewManager/ModelSelection';
import { PreviewControl } from '@web/pages/LightdashPreviewManager/PreviewControl';
import { useLightdashPreviewStore } from '@web/stores/useLightdashPreviewStore';
import { useCallback, useEffect } from 'react';

export function LightdashPreviewManager() {
  const { api } = useApp();
  const { vscode } = useEnvironment();
  const {
    setModels,
    setIsLoadingModels,
    setDefaultPreviewName,
    setPreviews,
    setIsLoadingPreviews,
    addLog,
  } = useLightdashPreviewStore();

  // Load initial data
  const loadData = useCallback(async () => {
    try {
      console.log('[LightdashPreviewManager] Starting to load data...');
      // Load all data in parallel for faster loading
      setIsLoadingModels(true);
      setIsLoadingPreviews(true);

      console.log('[LightdashPreviewManager] Making API calls...');
      const [defaultName, modelsResponse, previewsResponse] = await Promise.all(
        [
          api.post({
            type: 'lightdash-get-preview-name',
            request: null,
          }),
          api.post({
            type: 'lightdash-fetch-models',
            request: null,
          }),
          api.post({
            type: 'lightdash-fetch-previews',
            request: null,
          }),
        ],
      );

      console.log('[LightdashPreviewManager] API responses received:', {
        defaultName,
        modelsCount: modelsResponse.length,
        previewsCount: previewsResponse.length,
      });

      setDefaultPreviewName(defaultName);
      setModels(modelsResponse);
      setPreviews(previewsResponse);
      setIsLoadingModels(false);
      setIsLoadingPreviews(false);
      console.log('[LightdashPreviewManager] Data loaded successfully');
    } catch (error) {
      console.error('[LightdashPreviewManager] Error loading data:', error);
      setIsLoadingModels(false);
      setIsLoadingPreviews(false);
    }
  }, [
    api,
    setModels,
    setIsLoadingModels,
    setDefaultPreviewName,
    setPreviews,
    setIsLoadingPreviews,
  ]);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  // Listen for webview messages (logs, progress, completion)
  useEffect(() => {
    if (!vscode) return;

    const messageHandler = (event: MessageEvent) => {
      const message = event.data;

      if (message.type === 'log') {
        const log = message.log;
        addLog(log);

        // Reload previews when a preview is successfully created
        if (log.isPreviewSuccess) {
          api
            .post({ type: 'lightdash-fetch-previews', request: null })
            .then(setPreviews)
            .catch((err) => console.error('Error reloading previews:', err));
        }
      }
    };

    window.addEventListener('message', messageHandler);
    return () => window.removeEventListener('message', messageHandler);
  }, [vscode, addLog, api, setPreviews]);

  return (
    <div className="min-h-screen text-surface-contrast">
      {/* Header */}
      <header className="border-b border-surface p-4">
        <h1 className="text-2xl font-bold">Lightdash Preview Manager</h1>
        <p className="text-sm">
          Select models and manage your Lightdash previews
        </p>
      </header>

      {/* Main Content */}
      <main className="h-[calc(100vh-85px)] overflow-hidden">
        <div className="h-full grid grid-cols-1 md:grid-cols-2 gap-4 p-4">
          {/* Left Column: Model Selection */}
          <div className="bg-card rounded-lg overflow-hidden flex flex-col h-full">
            <ModelSelection />
            <ExistingPreviews />
          </div>

          {/* Right Column: Preview Control */}
          <div className="bg-card rounded-lg overflow-hidden h-full">
            <PreviewControl />
          </div>
        </div>
      </main>
    </div>
  );
}
