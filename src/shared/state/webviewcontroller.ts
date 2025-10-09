import * as vscode from 'vscode';
import { StateMessage } from './types';
import { StateManager } from '@services/statemanager';

export class WebviewController {
  stateManager: StateManager;

  constructor(stateManager: StateManager) {
    this.stateManager = stateManager;
  }

  /**
   * Handle incoming messages from webview
   */
  async handleWebviewMessage(
    message: StateMessage,
    webview: vscode.Webview,
  ): Promise<void> {
    try {
      switch (message.type) {
        case 'state:load':
          await this.handleLoadState(message, webview);
          break;

        case 'state:save':
          await this.handleSaveState(message, webview);
          break;

        case 'state:clear':
          await this.handleClearState(message, webview);
          break;

        default:
          console.warn(
            '[WebviewStateController] Unknown state message type:',
            message,
          );
      }
    } catch (error) {
      console.error(
        '[WebviewStateController] Error handling webview state message:',
        error,
      );
      // Send error back to webview
      webview.postMessage({
        type: 'state:error',
        formType: 'formType' in message ? message.formType : 'unknown',
        error:
          error instanceof Error ? error.message : 'Unknown error occurred',
      });
    }
  }

  /**
   * Load state and send back to webview
   */
  private async handleLoadState(
    message: { type: 'state:load'; formType: string },
    webview: vscode.Webview,
  ): Promise<void> {
    try {
      const data = await this.stateManager.getFormState(message.formType);

      const response = {
        type: 'state:loaded',
        formType: message.formType,
        data,
      } as StateMessage;

      webview.postMessage(response);
    } catch (error) {
      console.error(
        `[WebviewStateController] Error loading state for ${message.formType}:`,
        error,
      );
    }
  }

  /**
   * Save state from webview
   */
  private async handleSaveState(
    message: {
      type: 'state:save';
      formType: string;
      data: Record<string, any>;
    },
    webview: vscode.Webview,
  ): Promise<void> {
    try {
      await this.stateManager.saveFormState(message.formType, message.data);

      webview.postMessage({
        type: 'state:saved',
        formType: message.formType,
        success: true,
      } as StateMessage);
    } catch (error) {
      console.error(
        `[WebviewStateController] Error saving state for ${message.formType}:`,
        error,
      );

      webview.postMessage({
        type: 'state:saved',
        formType: message.formType,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to save state',
      } as StateMessage);
    }
  }

  /**
   * Clear state for form type
   */
  private async handleClearState(
    message: { type: 'state:clear'; formType: string },
    webview: vscode.Webview,
  ): Promise<void> {
    try {
      await this.stateManager.clearFormState(message.formType);

      webview.postMessage({
        type: 'state:cleared',
        formType: message.formType,
        success: true,
      } as StateMessage);
    } catch (error) {
      console.error(
        `[WebviewStateController] Error clearing state for ${message.formType}:`,
        error,
      );

      webview.postMessage({
        type: 'state:cleared',
        formType: message.formType,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to clear state',
      } as StateMessage);
    }
  }
}
