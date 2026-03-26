import {
  ChartBarIcon,
  CheckCircleIcon,
  CommandLineIcon,
  ExclamationTriangleIcon,
  XCircleIcon,
} from '@heroicons/react/24/outline';
import { makeClassName } from '@web';
import { useModelTestStore } from '@web/stores/useModelTestStore';
import { useEffect, useRef } from 'react';

// Strip ANSI escape codes from text
const stripAnsi = (text: string): string => {
  // Match ANSI escape sequences in multiple formats:
  // 1. ESC[ followed by params and ending char (actual escape codes)
  // 2. Literal [0m, [32m, [31m, [33m etc (when displayed as text)

  return (
    text
      // eslint-disable-next-line no-control-regex -- ANSI escape sequences use control chars
      .replace(/\x1b\[[0-9;]*m/g, '') // Remove actual ANSI escape sequences
      .replace(/\[0m/g, '') // Remove literal reset codes
      .replace(/\[\d+m/g, '')
  ); // Remove literal color codes like [32m, [31m, [33m
};

// Detect ANSI color codes and return appropriate style
// Handles both actual escape sequences and literal codes
const detectAnsiColor = (text: string): 'green' | 'red' | 'yellow' | null => {
  // Green: [32m (success)
  if (text.includes('[32m')) return 'green';
  // Red: [31m (error)
  if (text.includes('[31m')) return 'red';
  // Yellow/Warning: [33m
  if (text.includes('[33m')) return 'yellow';
  return null;
};

// Analytics Summary Component
function AnalyticsSummary() {
  const { runAnalytics, isTesting } = useModelTestStore();

  if (isTesting || !runAnalytics) return null;

  return (
    <div className="mt-8 p-4 bg-surface/30 rounded-2xl border border-surface">
      <div className="flex items-center gap-2 mb-4">
        <ChartBarIcon className="w-5 h-5 text-surface-contrast/50" />
        <h3 className="text-sm font-bold text-surface-contrast/70 uppercase tracking-widest">
          Test Summary
        </h3>
      </div>
      <div className="flex flex-wrap gap-3">
        {/* Total Models */}
        <div className="bg-card p-3 rounded-xl border border-surface shadow-sm min-w-[100px] flex-1">
          <p className="text-[10px] font-bold text-surface-contrast/50 uppercase tracking-wider mb-1">
            Total Models
          </p>
          <p className="text-xl font-extrabold text-surface-contrast leading-none">
            {runAnalytics.total}
          </p>
        </div>

        {/* Success Rate */}
        <div className="bg-card p-3 rounded-xl border border-surface shadow-sm min-w-[100px] flex-1">
          <p className="text-[10px] font-bold text-green-500 uppercase tracking-wider mb-1 flex items-center justify-between gap-2">
            Success Rate
            <CheckCircleIcon className="w-3 h-3" />
          </p>
          <div className="flex items-baseline gap-1">
            <p className="text-xl font-extrabold text-green-600 leading-none">
              {runAnalytics.successRate}%
            </p>
            <p className="text-[10px] font-bold text-green-500/60">
              ({runAnalytics.success})
            </p>
          </div>
        </div>

        {/* Failure Rate */}
        <div className="bg-card p-3 rounded-xl border border-surface shadow-sm min-w-[100px] flex-1">
          <p className="text-[10px] font-bold text-red-500 uppercase tracking-wider mb-1 flex items-center justify-between gap-2">
            Failure Rate
            <XCircleIcon className="w-3 h-3" />
          </p>
          <div className="flex items-baseline gap-1">
            <p className="text-xl font-extrabold text-red-600 leading-none">
              {runAnalytics.failureRate}%
            </p>
            <p className="text-[10px] font-bold text-red-500/60">
              ({runAnalytics.error})
            </p>
          </div>
        </div>

        {/* Warnings */}
        <div className="bg-card p-3 rounded-xl border border-surface shadow-sm min-w-[100px] flex-1">
          <p className="text-[10px] font-bold text-amber-500 uppercase tracking-wider mb-1 flex items-center justify-between gap-2">
            Warnings
            <ExclamationTriangleIcon className="w-3 h-3" />
          </p>
          <p className="text-xl font-extrabold text-amber-600 leading-none">
            {runAnalytics.warning}
          </p>
        </div>
      </div>
    </div>
  );
}

export function ConsoleOutput() {
  const { logs, runAnalytics } = useModelTestStore();
  const logEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom when logs update or analytics appear
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs, runAnalytics]);

  const getLogStyle = (log: string) => {
    // First check for ANSI color codes in original log
    const ansiColor = detectAnsiColor(log);
    const cleanLog = stripAnsi(log);

    // Progress messages (start with >)
    if (cleanLog.startsWith('>')) {
      return 'text-primary font-bold mt-6 first:mt-0 bg-primary/10 p-3 rounded-lg border-l-4 border-primary shadow-sm';
    }

    // Check for dbt completion summary line: "Done. PASS=X WARN=Y ERROR=Z"
    if (cleanLog.includes('Done.') && cleanLog.includes('PASS=')) {
      const errorMatch = cleanLog.match(/ERROR=(\d+)/);
      const warnMatch = cleanLog.match(/WARN=(\d+)/);
      const errorCount = errorMatch ? parseInt(errorMatch[1], 10) : 0;
      const warnCount = warnMatch ? parseInt(warnMatch[1], 10) : 0;

      if (errorCount > 0) {
        return 'text-red-600 font-bold pl-4 bg-red-50 dark:bg-red-900/20 p-2 rounded';
      } else if (warnCount > 0) {
        return 'text-amber-600 font-bold pl-4 bg-amber-50 dark:bg-amber-900/20 p-2 rounded';
      } else {
        return 'text-green-600 font-bold pl-4 bg-green-50 dark:bg-green-900/20 p-2 rounded';
      }
    }

    // Use ANSI color if detected
    if (ansiColor === 'green') {
      return 'text-green-600 pl-4';
    }
    if (ansiColor === 'red') {
      return 'text-red-600 pl-4';
    }
    if (ansiColor === 'yellow') {
      return 'text-amber-600 pl-4';
    }

    // Test result lines with PASS/FAIL in brackets
    if (cleanLog.includes('[PASS')) {
      return 'text-green-600 font-medium pl-4';
    }
    if (cleanLog.includes('[ERROR') || cleanLog.includes('[FAIL')) {
      return 'text-red-600 font-medium pl-4';
    }

    // "Completed successfully" message
    if (cleanLog.includes('Completed successfully')) {
      return 'text-green-600 font-semibold pl-4';
    }
    // "Completed with X errors" message
    if (cleanLog.includes('Completed with') && cleanLog.includes('error')) {
      return 'text-red-600 font-semibold pl-4';
    }

    // Warning messages (but not inside test names)
    if (cleanLog.includes('[WARNING]') || cleanLog.match(/^\s*WARNING:/)) {
      return 'text-amber-600 font-semibold pl-4';
    }

    // Internal test result lines: "  - Tests completed ... OK/ERROR"
    if (cleanLog.includes('Tests completed')) {
      if (cleanLog.includes('OK')) {
        return 'text-green-600 font-semibold pl-4';
      }
      if (cleanLog.includes('ERROR')) {
        return 'text-red-600 font-semibold pl-4';
      }
    }

    // Default log style
    return 'text-surface-contrast/70 pl-4 border-l border-surface ml-1';
  };

  return (
    <div className="flex flex-col h-full bg-card overflow-hidden">
      {/* Header */}
      <div className="h-[52px] flex items-center justify-between px-8 border-b border-surface shrink-0 bg-card">
        <div className="flex items-center gap-3">
          <CommandLineIcon className="w-5 h-5 text-primary" />
          <h2 className="text-[13px] font-bold uppercase tracking-widest text-surface-contrast">
            Console Output
          </h2>
        </div>
      </div>

      {/* Logs */}
      <div className="flex-1 p-6 text-[13px] overflow-y-auto bg-card">
        {logs.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center text-center opacity-30">
            <CommandLineIcon className="w-12 h-12 text-surface-contrast/30 mb-4" />
            <p className="text-sm font-medium text-surface-contrast/50">
              Waiting for execution logs...
            </p>
          </div>
        ) : (
          <div className="space-y-1 w-full">
            {logs.map((log, i) => (
              <div
                key={i}
                className={makeClassName(
                  'whitespace-pre-wrap break-words leading-relaxed font-mono',
                  getLogStyle(log),
                )}
              >
                {stripAnsi(log)}
              </div>
            ))}

            {/* Analytics Summary */}
            <AnalyticsSummary />

            <div ref={logEndRef} />
          </div>
        )}
      </div>
    </div>
  );
}
