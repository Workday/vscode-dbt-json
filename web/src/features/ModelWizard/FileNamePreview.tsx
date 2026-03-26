// import { Label } from '@headlessui/react';
import { ExclamationTriangleIcon } from '@heroicons/react/24/outline';
import { makeClassName } from '@web';
import { Spinner, Tooltip } from '@web/elements';
import { useModelFileValidation } from '@web/hooks';

interface FileNamePreviewProps {
  mode?: 'create' | 'edit';
  compact?: boolean;
}

/**
 * Component that displays the generated model file name and validates if it already exists
 * Shows in real-time as user fills in basic information fields
 */
export function FileNamePreview({ mode = 'create' }: FileNamePreviewProps) {
  const { fileName, isValidating, fileExists, isReady } =
    useModelFileValidation(mode);

  return (
    <div>
      <div className="flex items-center gap-2 mb-1 ">
        <div
          className={
            'text-sm/6 font-semibold leading-6 mt-2 text-foreground flex gap-1 items-center'
          }
        >
          Final File Name
          <Tooltip
            content={
              <div>
                <p className="font-medium mb-1">Auto-generated file name</p>
                <p>
                  The file name is automatically generated based on the model's
                  type, group, topic, and name fields.
                </p>
                <p className="mt-2 text-xs opacity-80">
                  Format: {`<source>__<group>__<topic>__<name>.model.json`}
                </p>
              </div>
            }
          />
        </div>
      </div>

      <div
        className={makeClassName(
          'flex items-center bg-background border rounded-lg px-3 h-11 text-sm text-background-contrast w-full focus:outline-none mt-3',
          fileExists && 'border-red-600 ',
          !fileExists && 'border-blue-500 ',
        )}
      >
        {!isReady ? (
          <p className="text-sm text-blue-800 italic">
            Please fill the basic information to see the actual file name to be
            generated
          </p>
        ) : isValidating ? (
          <div className="flex items-center gap-2">
            <Spinner size={16} />
            <span className="text-sm text-blue-800">
              Checking existing model files...
            </span>
          </div>
        ) : fileExists ? (
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <ExclamationTriangleIcon className="h-5 w-5 text-red-600 flex-shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-red-600 break-all">
                  {fileName}
                </p>
              </div>
            </div>
          </div>
        ) : (
          <div className="flex items-start gap-2">
            <div className="h-5 w-5 flex items-center justify-center flex-shrink-0">
              <div className="h-2 w-2 rounded-full bg-green-500" />
            </div>
            <p className="text-sm font-mono text-foreground break-all">
              {fileName}
            </p>
          </div>
        )}
      </div>
      {fileExists && (
        <div>
          <p className="inline-block text-error text-xs italic">
            Model name or topic exists, please update model name/topic to avoid
            file name conflicts.
          </p>
        </div>
      )}
    </div>
  );
}
