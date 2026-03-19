/**
 * UI Handlers - Simple UI-related operations
 *
 * Handles:
 * - Opening external URLs
 * - Closing webview panels
 * - Showing messages to users
 * - Getting/setting model settings
 */

import type { ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import * as vscode from 'vscode';

import type { FrameworkContext } from '../context';

export class UIHandlers {
  constructor(private readonly ctx: FrameworkContext) {}

  /**
   * Open an external URL in the user's default browser
   */
  handleOpenExternalUrl(
    payload: ApiPayload<'framework'> & { type: 'framework-open-external-url' },
  ): Promise<ApiResponse> {
    const { url } = payload.request;
    vscode.env.openExternal(vscode.Uri.parse(url));
    return Promise.resolve(apiResponse<typeof payload.type>({ success: true }));
  }

  /**
   * Close a webview panel (e.g., model-create panel)
   */
  handleClosePanel(
    payload: ApiPayload<'framework'> & { type: 'framework-close-panel' },
  ): Promise<ApiResponse> {
    const { panelType } = payload.request;

    // Close the appropriate webview panel
    if (panelType === 'model-create') {
      const panel = this.ctx.webviewPanelModelCreate;
      if (panel) {
        panel.dispose();
        this.ctx.webviewPanelModelCreate = undefined;
      }
    }

    return Promise.resolve(apiResponse<typeof payload.type>({ success: true }));
  }

  /**
   * Show a message to the user and optionally close panels
   */
  handleShowMessage(
    payload: ApiPayload<'framework'> & { type: 'framework-show-message' },
  ): Promise<ApiResponse> {
    const { message, type: messageType, closePanel } = payload.request;

    // Show the appropriate message type
    switch (messageType) {
      case 'info':
        vscode.window.showInformationMessage(message);
        break;
      case 'success':
        vscode.window.showInformationMessage(message);
        break;
      case 'warning':
        vscode.window.showWarningMessage(message);
        break;
      case 'error':
        vscode.window.showErrorMessage(message);
        break;
    }

    // Close panels if requested
    if (closePanel) {
      const panel = this.ctx.webviewPanelModelCreate;
      if (panel) {
        panel.dispose();
        this.ctx.webviewPanelModelCreate = undefined;
      }
    }

    return Promise.resolve(apiResponse<typeof payload.type>({ success: true }));
  }

  /**
   * Get model settings from persistent storage
   */
  async handleGetModelSettings(
    _payload: ApiPayload<'framework'> & {
      type: 'framework-get-model-settings';
    },
  ): Promise<ApiResponse> {
    try {
      const stateResponse = await this.ctx.api.handleApi({
        type: 'state-load',
        request: { formType: 'model-settings' },
      });
      // Type assertion: state-load returns { data: unknown }
      const data = (stateResponse as { data?: unknown })?.data ?? {};
      return apiResponse<'framework-get-model-settings'>(data);
    } catch (error: unknown) {
      this.ctx.log.warn('Failed to get model settings:', error);
      return apiResponse<'framework-get-model-settings'>({});
    }
  }

  /**
   * Save model settings to persistent storage
   */
  async handleSetModelSettings(
    payload: ApiPayload<'framework'> & { type: 'framework-set-model-settings' },
  ): Promise<ApiResponse> {
    try {
      await this.ctx.api.handleApi({
        type: 'state-save',
        request: {
          formType: 'model-settings',
          data: payload.request,
        },
      });
      return apiResponse<typeof payload.type>({ success: true });
    } catch (error: unknown) {
      this.ctx.log.error('Failed to save model settings:', error);
      return apiResponse<typeof payload.type>({ success: false });
    }
  }
}
