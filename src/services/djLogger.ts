import { getDjConfig } from '@services/config';
import { OUTPUT_CHANNEL_NAME } from '@services/constants';
import type { LogLevel } from '@shared/types/common';
import { timestamp } from 'admin';
import * as vscode from 'vscode';

// Configuration key for log level setting
const LOG_LEVEL_CONFIG_KEY = 'dj.logLevel';

const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/**
 * Centralized logging service that handles both console output and VS Code output channel
 */
export class DJLogger {
  // Output channel name
  private channelName: string;

  private outputChannel: vscode.OutputChannel;

  // Cache the log level to avoid repeated config lookups
  private currentLogLevel: LogLevel = 'info';

  // Store the configuration change listener for proper disposal
  private configChangeListener: vscode.Disposable;

  // Flag to prevent recursion when logging configuration errors
  private isLoggingConfigError = false;

  constructor(channelName: string = OUTPUT_CHANNEL_NAME) {
    this.channelName = channelName;
    this.outputChannel = vscode.window.createOutputChannel(channelName);
    this.updateLogLevel();

    // Listen for configuration changes
    this.configChangeListener = vscode.workspace.onDidChangeConfiguration(
      (e) => {
        if (e.affectsConfiguration(LOG_LEVEL_CONFIG_KEY)) {
          this.updateLogLevel();
        }
      },
    );
  }

  /**
   * Get the current log level from VS Code configuration
   */
  private getConfiguredLogLevel(): LogLevel {
    try {
      const level = getDjConfig().logLevel;

      // Validate that the level is one of the allowed values
      if (level in LOG_LEVEL_PRIORITY) {
        return level;
      }

      // Log a warning if invalid level is configured
      // Use guard to prevent recursion if this is called during logging
      if (!this.isLoggingConfigError) {
        this.isLoggingConfigError = true;
        this.outputChannel.appendLine(
          `${timestamp()} [warn] Invalid log level "${level}" configured. Using default "info".`,
        );
        this.isLoggingConfigError = false;
      }
    } catch (error) {
      // If configuration reading fails, log to console as fallback
      console.warn('DJ Logger: Failed to read log level configuration:', error);
    }

    return 'info';
  }

  /**
   * Update the cached log level
   */
  private updateLogLevel() {
    this.currentLogLevel = this.getConfiguredLogLevel();
  }

  /**
   * Check if a log level should be written based on current configuration
   */
  private shouldLog(level: LogLevel): boolean {
    return (
      LOG_LEVEL_PRIORITY[level] >= LOG_LEVEL_PRIORITY[this.currentLogLevel]
    );
  }

  /**
   * Get the current log level (useful for debugging/testing)
   */
  public getCurrentLogLevel(): LogLevel {
    return this.currentLogLevel;
  }

  /**
   * Format logs for output channel display
   */
  private formatLogsForOutputChannel(
    level: LogLevel,
    logs: readonly unknown[],
  ): string {
    return [
      `${timestamp()} [${level}]`,
      ...logs.map((log) => {
        // Handle null and undefined
        if (log === null || log === undefined) {
          return log === null ? 'null' : 'undefined';
        }

        // Handle Error objects specially
        // Stack trace already contains name and message, so prefer it
        if (log instanceof Error) {
          return log.stack ?? `${log.name}: ${log.message}`;
        }

        // Handle objects (including arrays)
        if (typeof log === 'object') {
          try {
            return JSON.stringify(log);
          } catch {
            return '[object]';
          }
        }

        // Handle primitives (string, number, boolean, symbol, bigint)
        if (
          typeof log === 'string' ||
          typeof log === 'number' ||
          typeof log === 'boolean' ||
          typeof log === 'bigint'
        ) {
          return String(log);
        }
        // Handle symbols specially
        if (typeof log === 'symbol') {
          return log.toString();
        }
        // Fallback for any edge cases (should not happen given checks above)
        // eslint-disable-next-line @typescript-eslint/no-base-to-string
        return String(log);
      }),
    ].join(' ');
  }

  /**
   * Write to both console and output channel
   * Note: Debug and info logs are only written to the output channel,
   * not to the console, as only warn and error are allowed by the linter.
   */
  private writeLog(level: LogLevel, ...logs: readonly unknown[]) {
    // Check if this log level should be written
    if (!this.shouldLog(level)) {
      return;
    }

    const timestamp_prefix = `${timestamp()} [${level}]`;

    // Write to console (only warn and error are allowed by linter)
    if (level === 'error') {
      console.error(this.channelName, timestamp_prefix, ...logs);
    } else if (level === 'warn') {
      console.warn(this.channelName, timestamp_prefix, ...logs);
    }

    // Write to output channel
    this.outputChannel.appendLine(this.formatLogsForOutputChannel(level, logs));
  }

  /**
   * Log debug level messages
   */
  debug(...logs: readonly unknown[]) {
    this.writeLog('debug', ...logs);
  }

  /**
   * Log error level messages
   */
  error(...logs: readonly unknown[]) {
    this.writeLog('error', ...logs);
  }

  /**
   * Log info level messages
   */
  info(...logs: readonly unknown[]) {
    this.writeLog('info', ...logs);
  }

  /**
   * Required by AJV for schema validation logging
   * Acts as an alias for info()
   */
  log(...logs: readonly unknown[]) {
    this.info(...logs);
  }

  /**
   * Log warning level messages
   */
  warn(...logs: readonly unknown[]) {
    this.writeLog('warn', ...logs);
  }

  /**
   * Show the output channel to the user
   */
  show(preserveFocus?: boolean) {
    this.outputChannel.show(preserveFocus);
  }

  /**
   * Get the underlying VS Code output channel
   */
  getOutputChannel(): vscode.OutputChannel {
    return this.outputChannel;
  }

  /**
   * Dispose of resources
   */
  dispose() {
    this.outputChannel.dispose();
    this.configChangeListener.dispose();
  }
}
