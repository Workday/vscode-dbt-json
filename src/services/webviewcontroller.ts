import * as vscode from 'vscode';
import { StateManager } from './statemanager';

export class State {
  stateManager: StateManager;

  constructor(stateManager: StateManager) {
    this.stateManager = stateManager;
  }

  /**
   * Handle state API requests
   */
  async handleApi(payload: any): Promise<any> {
    try {
      switch (payload.type) {
        case 'state-load': {
          const data = await this.stateManager.getFormState(
            payload.request.formType,
          );
          return { data };
        }
        case 'state-save': {
          await this.stateManager.saveFormState(
            payload.request.formType,
            payload.request.data,
          );
          return { success: true };
        }
        case 'state-clear': {
          await this.stateManager.clearFormState(payload.request.formType);
          return { success: true };
        }
        default:
          throw new Error(`Unknown state API type: ${payload.type}`);
      }
    } catch (error) {
      console.error('[State] Error handling state API request:', error);
      throw error;
    }
  }

  /**
   * Inject initial state when webview is created
   * Call this right after webview HTML is set
   */
  async injectInitialState(
    webview: vscode.Webview,
    formType: string,
  ): Promise<void> {
    // Small delay to ensure webview is ready to receive messages
    setTimeout(async () => {
      try {
        const data = await this.stateManager.getFormState(formType);

        webview.postMessage({
          type: 'state:loaded',
          formType,
          data,
        });
      } catch (error) {
        console.error(
          `[State] Error injecting initial state for ${formType}:`,
          error,
        );
        // Send null data so form can start fresh
        webview.postMessage({
          type: 'state:loaded',
          formType,
          data: null,
        });
      }
    }, 10);
  }
}
