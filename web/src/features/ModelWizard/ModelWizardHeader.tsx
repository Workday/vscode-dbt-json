import {
  ChevronDownIcon,
  QuestionMarkCircleIcon,
} from '@heroicons/react/20/solid';
import {
  LinkIcon,
  PlayCircleIcon,
  SparklesIcon,
} from '@heroicons/react/24/outline';
import { EXTERNAL_LINKS } from '@shared/web/constants';
import { useApp } from '@web/context';
import { Button, Popover } from '@web/elements';
import { useModelStore } from '@web/stores/useModelStore';

interface ModelWizardHeaderProps {
  title: string;
  onDiscard: () => void;
  onSaveForLater: () => void;
  onStartTutorial?: () => void; // Play Tutorial
  onToggleAssistMode?: () => void; // Assist Mode
  assistModeEnabled?: boolean; // Whether Assist Mode is active
  hasFormData: boolean;
  isLoading?: boolean;
  showPreviewToggle?: boolean;
}

export function ModelWizardHeader({
  title,
  onDiscard,
  onSaveForLater,
  onStartTutorial,
  onToggleAssistMode,
  assistModeEnabled = false,
  hasFormData,
  isLoading = false,
  showPreviewToggle = true,
}: ModelWizardHeaderProps) {
  const { api } = useApp();
  const { isPreviewEnabled, togglePreview } = useModelStore();

  const onHelp = async () => {
    try {
      await api.post({
        type: 'framework-open-external-url',
        request: { url: EXTERNAL_LINKS.documentation },
      });
      close?.();
    } catch {
      window.open(EXTERNAL_LINKS.documentation, '_blank');
      close?.();
    }
  };

  return (
    <div className="px-4 py-2">
      <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-3 mb-1">
        <h1 className="text-xl font-semibold">
          {title}
          <sup className="text-xs bg-blue-200 font-normal text-blue-800 px-3 py-1 ml-2 rounded-full">
            alpha
          </sup>
        </h1>
        <div className="flex flex-wrap gap-2">
          <div className="bg-surface shadow-sm rounded-lg flex flex-col divide-y divide-background-contrast/20 w-full max-w-xs sm:flex-row sm:w-max sm:max-w-full sm:divide-y-0 sm:divide-x sm:items-center">
            <Button
              label="Save draft"
              variant="iconButton"
              onClick={() => {
                onSaveForLater();
                close();
              }}
              type="button"
              disabled={isLoading}
              //icon={<BookmarkSquareIcon className="h-4 w-4" />}
              className="w-full text-foreground font-medium px-3 py-2 text-sm hover:bg-muted transition-colors ring-0 sm:w-auto"
            />
            <Button
              label="Discard"
              variant="iconButton"
              onClick={() => {
                onDiscard();
                close();
              }}
              type="button"
              disabled={isLoading || !hasFormData}
              //icon={<TrashIcon className="h-4 w-4" />}
              className="w-full text-foreground font-medium px-3 py-2 text-sm hover:bg-muted transition-colors ring-0 sm:w-auto"
            />
            {showPreviewToggle && (
              <Button
                label={isPreviewEnabled ? 'Hide preview' : 'Show preview'}
                variant="iconButton"
                type="button"
                onClick={() => togglePreview(!isPreviewEnabled)}
                disabled={isLoading}
                className="w-full text-foreground font-medium px-3 py-2 text-sm hover:bg-muted transition-colors ring-0 sm:w-auto"
              />
            )}
          </div>
          <div className="flex flex-wrap items-center">
            <Popover
              trigger={
                <div className="flex items-center gap-2 sm:gap-3 px-2 py-2 bg-blue-100 text-primary rounded-lg hover:bg-primary/20 cursor-pointer transition-colors text-sm">
                  <QuestionMarkCircleIcon className="h-4 w-4" />
                  <span className="font-medium text-sm inline">Help</span>
                  <ChevronDownIcon className="h-5 w-5" />
                </div>
              }
              panelClassName="w-56 p-2"
              placement="right"
            >
              {(close) => (
                <div className="space-y-1">
                  {onToggleAssistMode && (
                    <Button
                      label="Assist Me"
                      variant="iconButton"
                      onClick={() => {
                        onToggleAssistMode();
                        close();
                      }}
                      type="button"
                      disabled={isLoading}
                      icon={<SparklesIcon className="h-4 w-4" />}
                      className={`ring-0 px-3 py-2 cursor-pointer font-medium text-sm transition-all ${
                        assistModeEnabled
                          ? 'bg-primary/15 text-primary ring-2 ring-primary font-medium rounded-md'
                          : ''
                      }`}
                    />
                  )}
                  {onStartTutorial && (
                    <Button
                      label="Play Tutorial"
                      variant="iconButton"
                      onClick={() => {
                        onStartTutorial();
                        close();
                      }}
                      type="button"
                      icon={<PlayCircleIcon className="h-4 w-4" />}
                      className="w-full justify-start px-3 py-2 text-sm hover:bg-muted rounded-md font-medium transition-colors ring-0"
                    />
                  )}
                  <Button
                    label="Docs"
                    variant="iconButton"
                    onClick={() => void onHelp()}
                    type="button"
                    icon={<LinkIcon className="h-4 w-4" />}
                    className="w-full justify-start px-3 py-2 text-sm hover:bg-muted rounded-md font-medium transition-colors ring-0"
                  />
                </div>
              )}
            </Popover>
          </div>
        </div>
      </div>
    </div>
  );
}
