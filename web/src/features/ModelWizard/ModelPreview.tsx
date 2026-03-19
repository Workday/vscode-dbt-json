import { MoonIcon, SunIcon } from '@heroicons/react/24/outline';
import { makeClassName } from '@web';
import { useApp } from '@web/context';
import type { RadioOption } from '@web/elements';
import { CodeBlock, RadioGroup, Spinner, Tooltip } from '@web/elements';
import { useDebounce } from '@web/hooks';
import { useModelStore } from '@web/stores/useModelStore';
import { useTutorialStore } from '@web/stores/useTutorialStore';
import { useEffect, useState } from 'react';

enum PreviewType {
  SQL = 'sql',
  YAML = 'yaml',
  JSON = 'json',
}

const PREVIEW_TYPES: RadioOption[] = [
  { value: PreviewType.SQL, label: 'SQL' },
  { value: PreviewType.YAML, label: 'YML' },
  { value: PreviewType.JSON, label: 'JSON' },
];

export const ModelPreview: React.FC = () => {
  const [selectedPreviewType, setSelectedPreviewType] = useState<PreviewType>(
    PreviewType.SQL,
  );

  // Initialize theme based on document/OS settings
  const [previewTheme, setPreviewTheme] = useState<'light' | 'dark'>(() => {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    return currentTheme?.includes('dark') ? 'dark' : 'light';
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [previewData, setPreviewData] = useState({
    json: '',
    sql: '',
    yaml: '',
  });

  const { api } = useApp();
  const { basicFields, buildModelJson } = useModelStore();

  // Tutorial store integration
  const { isPlayTutorialActive, tutorialPreviewType } = useTutorialStore(
    (state) => ({
      isPlayTutorialActive: state.isPlayTutorialActive,
      tutorialPreviewType: state.tutorialPreviewType,
    }),
  );

  const projectName = basicFields.projectName;

  // Build the model and stringify it
  // The stringified version changes only when the actual content changes
  const model = buildModelJson();
  const modelString = JSON.stringify(model);

  // Debounce model changes to avoid excessive API calls
  const debouncedModelString = useDebounce(modelString, 1000);

  const previewCode = previewData[selectedPreviewType];

  // Listen for theme changes in the document
  useEffect(() => {
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        if (mutation.attributeName === 'data-theme') {
          const newTheme = document.documentElement.getAttribute('data-theme');
          setPreviewTheme(newTheme?.includes('dark') ? 'dark' : 'light');
        }
      });
    });

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme'],
    });

    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const fetchPreview = async () => {
      // Parse the debounced model string back to object
      const currentModel = JSON.parse(debouncedModelString);

      // Don't fetch if required fields are missing
      if (!currentModel.name || !currentModel.type) {
        setPreviewData({
          json: '# Add model name and type to see JSON preview',
          sql: '-- Add model name and type to see SQL preview',
          yaml: '# Add model name and type to see YAML preview',
        });
        return;
      }

      setIsLoading(true);
      setError(null);

      try {
        const response = await api.post({
          type: 'framework-model-preview' as const,
          request: {
            projectName,
            modelJson: currentModel,
          },
        } as const);

        setPreviewData({
          json: response.json,
          sql: response.sql,
          yaml: response.yaml,
        });
      } catch (err) {
        const error = (err || {}) as Error;
        console.error('Error fetching preview:', error);
        setError(error.message || 'Failed to generate preview');
        // Set fallback preview data
        setPreviewData({
          json: JSON.stringify(currentModel, null, 4),
          sql: `-- Error generating SQL:\n-- ${error.message || 'Unknown error'}`,
          yaml: `# Error generating YAML:\n# ${error.message || 'Unknown error'}`,
        });
      } finally {
        setIsLoading(false);
      }
    };

    void fetchPreview();
  }, [debouncedModelString, projectName, api]);

  // Tutorial: Listen for preview type changes and switch tabs
  useEffect(() => {
    if (isPlayTutorialActive && tutorialPreviewType) {
      switch (tutorialPreviewType) {
        case 'sql':
          setSelectedPreviewType(PreviewType.SQL);
          break;
        case 'yaml':
          setSelectedPreviewType(PreviewType.YAML);
          break;
        case 'json':
          setSelectedPreviewType(PreviewType.JSON);
          break;
        // 'meta' type doesn't exist in ModelPreview, skip it
      }
    }
  }, [isPlayTutorialActive, tutorialPreviewType]);

  return (
    <div
      className="flex flex-col gap-4 h-full max-h-full overflow-hidden"
      data-tutorial-id="model-preview"
    >
      <div className="flex justify-between items-center">
        <div className="flex items-center gap-2">
          <h2 className="text-lg font-bold">Model Preview</h2>
          <Tooltip
            content="Live preview of your model's SQL, YML configuration, and JSON structure as you build it."
            variant="outline"
          />
        </div>
        <button
          onClick={() => {
            setPreviewTheme((prev) => (prev === 'light' ? 'dark' : 'light'));
          }}
          type="button"
          className="px-3 py-2 rounded-md bg-surface hover:bg-surface/80 transition-colors"
          title={`Switch to ${previewTheme === 'light' ? 'dark' : 'light'} theme`}
          data-tutorial-id="model-preview-theme-toggle"
        >
          {previewTheme === 'light' ? (
            <MoonIcon className="h-5 w-5" />
          ) : (
            <SunIcon className="h-5 w-5" />
          )}
        </button>
      </div>
      <div
        className="flex gap-2 bg-background rounded-md p-2"
        data-tutorial-id="model-preview-tabs"
      >
        <RadioGroup
          value={selectedPreviewType}
          onChange={(value) => {
            setSelectedPreviewType(value as PreviewType);
          }}
          name="model-preview"
          options={PREVIEW_TYPES}
          className="flex-1"
          variant="button-group"
        />
      </div>
      {error && (
        <div className="text-sm text-red-600 bg-red-50 p-2 rounded">
          {error}
        </div>
      )}
      <div
        className="live-preview rounded-md flex-grow min-h-0 relative"
        data-tutorial-id="model-preview-content"
      >
        {isLoading && (
          <div
            className={makeClassName(
              'absolute inset-0 flex items-center  justify-center  bg-opacity-85 z-10',
              previewTheme === 'dark'
                ? 'bg-gray-500 text-white'
                : 'bg-gray-100 text-black',
            )}
          >
            <Spinner size={32} label="Generating preview..." />
          </div>
        )}
        <CodeBlock
          code={previewCode}
          language={selectedPreviewType}
          theme={previewTheme}
          className="h-full min-h-60"
        />
      </div>
    </div>
  );
};
