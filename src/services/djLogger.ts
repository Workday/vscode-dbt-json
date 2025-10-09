import * as vscode from 'vscode';
import { timestamp } from 'admin';
import { OUTPUT_CHANNEL_NAME } from '@shared/constants';

type LogLevel = 'debug' | 'error' | 'info' | 'warn';

/**
 * Centralized logging service that handles both console output and VS Code output channel
 */
export class DJLogger {
  // Output channel name
  private channelName: string;

  private outputChannel: vscode.OutputChannel;

  constructor(channelName: string = OUTPUT_CHANNEL_NAME) {
    this.channelName = channelName;
    this.outputChannel = vscode.window.createOutputChannel(channelName);
  }

  /**
   * Format logs for output channel display
   */
  private formatLogsForOutputChannel(level: LogLevel, logs: any[]): string {
    return [
      `${timestamp()} [${level}]`,
      ...logs.map((log) => {
        if (typeof log === 'object') {
          try {
            return JSON.stringify(log);
          } catch {
            return String(log);
          }
        }
        return log;
      }),
    ].join(' ');
  }

  /**
   * Write to both console and output channel
   */
  private writeLog(level: LogLevel, ...logs: any[]) {
    const timestamp_prefix = `${timestamp()} [${level}]`;

    // Write to console
    console[level](this.channelName, timestamp_prefix, ...logs);

    // Write to output channel
    this.outputChannel.appendLine(this.formatLogsForOutputChannel(level, logs));
  }

  debug(...logs: any[]) {
    this.writeLog('debug', ...logs);
  }

  error(...logs: any[]) {
    this.writeLog('error', ...logs);
  }

  info(...logs: any[]) {
    this.writeLog('info', ...logs);
  }

  /**
   * Required by AJV for schema validation logging
   * Acts as an alias for info()
   */
  log(...logs: any[]) {
    this.info(...logs);
  }

  warn(...logs: any[]) {
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
  }
}
