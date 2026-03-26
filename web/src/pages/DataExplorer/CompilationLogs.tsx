import {
  BeakerIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import { CodeBlock } from '@web/elements';
import { useEffect, useRef } from 'react';

export interface CompilationLog {
  level: 'info' | 'success' | 'error' | 'warning';
  message: string;
  timestamp: string;
  isProgress?: boolean;
}

/**
 * Detect if a message looks like SQL code
 * Typically the compiled node output starts with common SQL keywords
 */
function isSQL(message: string): boolean {
  const trimmed = message.trim();
  // Check if it starts with common SQL keywords (case insensitive)
  const sqlKeywords =
    /^(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|MERGE|TRUNCATE)\b/i;
  // Also check if it contains multiple SQL keywords and has multiple lines (multi-line SQL)
  const hasMultipleSqlKeywords =
    (
      trimmed.match(
        /\b(SELECT|FROM|WHERE|JOIN|GROUP BY|ORDER BY|UNION|WITH|AS)\b/gi,
      ) || []
    ).length >= 2;
  const isMultiLine = trimmed.includes('\n');

  return sqlKeywords.test(trimmed) || (hasMultipleSqlKeywords && isMultiLine);
}

interface CompilationLogsProps {
  logs: CompilationLog[];
  isCompiling: boolean;
  compilationSuccess: boolean | null;
  modelName: string;
  onClose: () => void;
  onRunQuery?: () => void;
  showRunButton: boolean;
}

export default function CompilationLogs({
  logs,
  isCompiling,
  compilationSuccess,
  modelName,
  onClose,
  onRunQuery,
  showRunButton,
}: CompilationLogsProps) {
  const logsEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom when logs update
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  const getLogColor = (level: CompilationLog['level']) => {
    switch (level) {
      case 'success':
        return 'text-green-400';
      case 'error':
        return 'text-red-400';
      case 'warning':
        return 'text-yellow-400';
      default:
        return 'text-gray-300';
    }
  };

  return (
    <div className="h-full flex flex-col bg-white border-l border-gray-200">
      {/* Header */}
      <div className="flex-shrink-0 px-4 py-3 border-b border-gray-200 flex items-center justify-between bg-gray-50">
        <div className="flex items-center gap-3">
          <BeakerIcon className="w-5 h-5 text-gray-700" />
          <h3 className="font-semibold text-gray-900">Compiling Model</h3>
          {isCompiling && (
            <div className="flex items-center gap-2 text-sm text-blue-600">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
              <span>Compiling...</span>
            </div>
          )}
          {!isCompiling && compilationSuccess === true && (
            <div className="flex items-center gap-1 text-sm text-green-600">
              <CheckCircleIcon className="w-4 h-4" />
              <span>Compiled successfully</span>
            </div>
          )}
          {!isCompiling && compilationSuccess === false && (
            <div className="flex items-center gap-1 text-sm text-red-600">
              <ExclamationCircleIcon className="w-4 h-4" />
              <span>Compilation failed</span>
            </div>
          )}
        </div>
        <button
          onClick={onClose}
          className="p-1.5 rounded hover:bg-gray-200 transition-colors"
          title="Close"
        >
          <XMarkIcon className="w-5 h-5 text-gray-600" />
        </button>
      </div>

      {/* Content - Logs */}
      <div className="flex-1 overflow-auto bg-gray-900 p-4">
        <div className="font-mono text-sm space-y-1">
          {logs.length === 0 && isCompiling && (
            <div className="text-gray-200">Starting compilation...</div>
          )}
          {logs.map((log, index) => (
            <div key={index} className={`${getLogColor(log.level)}`}>
              <div className="flex gap-2">
                <span className="text-gray-400 flex-shrink-0">
                  [{new Date(log.timestamp).toLocaleTimeString()}]
                </span>
                {isSQL(log.message) ? (
                  <span className="text-gray-200 text-xs">Compiled SQL:</span>
                ) : (
                  <span className="break-all text-gray-200">{log.message}</span>
                )}
              </div>
              {isSQL(log.message) && (
                <div className="mt-2 rounded-lg overflow-hidden border border-gray-700">
                  <CodeBlock
                    code={log.message}
                    language="sql"
                    theme="light"
                    showLineNumbers={true}
                    wrapLines={true}
                  />
                </div>
              )}
            </div>
          ))}
          <div ref={logsEndRef} />
        </div>
      </div>

      {/* Footer with actions */}
      <div className="flex-shrink-0 px-4 py-3 border-t border-gray-200 bg-gray-50 flex items-center justify-between">
        <p className="text-xs text-gray-600">
          Model: <span className="font-mono font-semibold">{modelName}</span>
        </p>
        {showRunButton && compilationSuccess && onRunQuery && !isCompiling && (
          <button
            onClick={onRunQuery}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors text-sm font-medium"
          >
            Run Query
          </button>
        )}
      </div>
    </div>
  );
}
