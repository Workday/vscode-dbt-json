import { ClipboardDocumentIcon } from '@heroicons/react/24/solid';
import type { VSCodeApi } from '@shared/types/config';
import { makeClassName } from '@web';
import { Button, CodeBlock } from '@web/elements';
import React, { useCallback, useState } from 'react';

interface CommandPreviewProps {
  commandPreview: string | null;
  isFormValid: boolean;
  previewTheme: 'light' | 'dark';
  vscode: VSCodeApi | null;
}

/**
 * Memoized command preview component
 * Only re-renders when props change
 */
export const CommandPreview = React.memo<CommandPreviewProps>(
  ({ commandPreview, isFormValid, previewTheme, vscode }) => {
    const [copiedToClipboard, setCopiedToClipboard] = useState(false);

    const handleCopyToClipboard = useCallback(() => {
      if (!commandPreview) return;

      try {
        // Use VS Code API to copy to clipboard
        vscode?.postMessage({
          type: 'copy-to-clipboard',
          text: commandPreview,
        });
        setCopiedToClipboard(true);
        // Reset the copied state after 2 seconds
        setTimeout(() => setCopiedToClipboard(false), 2000);
      } catch (err) {
        console.error('Failed to copy to clipboard:', err);
      }
    }, [commandPreview, vscode]);

    return (
      <div className="dbt-run-preview flex flex-col gap-2">
        <div className="flex items-center justify-between">
          <p className="text-sm font-semibold">DBT Run Command Preview:</p>
          {isFormValid && commandPreview && (
            <Button
              variant="iconButton"
              type="button"
              label={copiedToClipboard ? 'Copied!' : 'Copy'}
              icon={<ClipboardDocumentIcon className="h-4 w-4" />}
              onClick={handleCopyToClipboard}
              className={makeClassName(
                'text-xs px-2 py-1',
                copiedToClipboard ? 'text-green-600' : 'text-primary',
              )}
              title="Copy command to clipboard"
            />
          )}
        </div>
        {!isFormValid ? (
          <div className="bg-[var(--color-message-warning)] border border-warning rounded-lg p-4">
            <p className="text-sm text-[var(--color-message-warning-contrast)]">
              Fix validation errors to see command preview
            </p>
          </div>
        ) : (
          <div className="max-h-64 overflow-y-auto rounded-lg border border-neutral">
            <CodeBlock
              code={commandPreview || ''}
              language="bash"
              theme={previewTheme}
              wrapLines={true}
              className="text-xs"
            />
          </div>
        )}
      </div>
    );
  },
);

CommandPreview.displayName = 'CommandPreview';
