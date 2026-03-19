import {
  CheckCircleIcon,
  ClockIcon,
  ExclamationTriangleIcon,
  XCircleIcon,
} from '@heroicons/react/24/solid';
import { makeClassName } from '@web';
import { Spinner } from '@web/elements';
import type { TestStatus as TStatus } from '@web/stores/useModelTestStore';
import { useModelTestStore } from '@web/stores/useModelTestStore';

function getStatusIcon(status: TStatus) {
  switch (status) {
    case 'idle':
      return <ClockIcon className="w-4 h-4 text-surface-contrast/30" />;
    case 'progressing':
      return <Spinner size={16} inline />;
    case 'success':
      return <CheckCircleIcon className="w-4 h-4 text-green-500" />;
    case 'error':
      return <XCircleIcon className="w-4 h-4 text-red-500" />;
    case 'warning':
      return <ExclamationTriangleIcon className="w-4 h-4 text-amber-500" />;
    default:
      return null;
  }
}

function getStatusTextColor(status: TStatus) {
  switch (status) {
    case 'error':
      return 'text-red-600';
    case 'warning':
      return 'text-amber-600';
    default:
      return 'text-surface-contrast/80';
  }
}

export function TestStatus() {
  const { testQueue } = useModelTestStore();

  return (
    <div className="flex flex-col h-full bg-card overflow-hidden">
      {/* Header */}
      <div className="h-[52px] flex items-center px-6 border-b border-surface bg-card shrink-0">
        <h2 className="text-[13px] font-bold uppercase tracking-widest text-surface-contrast">
          Selected Models Test Status
        </h2>
      </div>

      {/* Status List */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3 min-h-0">
        {testQueue.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center text-center px-4 opacity-40">
            <div className="w-12 h-12 bg-surface/50 rounded-xl flex items-center justify-center mb-4">
              <ClockIcon className="w-6 h-6 text-surface-contrast/30" />
            </div>
            <p className="text-xs text-surface-contrast/50 font-medium leading-relaxed">
              No tests are currently running.
            </p>
          </div>
        ) : (
          testQueue.map((item) => (
            <div
              key={item.name}
              className="flex items-center gap-3 px-2 py-1 transition-all duration-300"
            >
              <div className="shrink-0">{getStatusIcon(item.status)}</div>
              <div className="min-w-0 flex-1">
                <p
                  title={item.name}
                  className={makeClassName(
                    'text-[13px] font-mono font-medium truncate leading-none',
                    getStatusTextColor(item.status),
                  )}
                >
                  {item.name}
                </p>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
