import type { VSCodeApi } from '@shared/coder/types';
import { v4 as uuid } from 'uuid';

type FormData = Record<string, unknown>;

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

interface StateResponse {
  success: boolean;
  data?: FormData | null;
}

class StateSyncManager {
  private vscode: VSCodeApi | null = null;
  private pendingRequests: Map<string, PendingRequest> = new Map();

  constructor() {
    // Initialize VS Code API connection
    try {
      const _vscode = (
        window as unknown as { acquireVsCodeApi?: () => VSCodeApi }
      ).acquireVsCodeApi?.();
      if (_vscode) {
        this.vscode = _vscode;
      }
    } catch (error) {
      console.warn(
        '[StateSyncManager] VS Code API not available - running in web mode:',
        error,
      );
    }

    // Listen for API response messages
    window.addEventListener('message', (event) => {
      const message = event.data;
      const _channelId = message?._channelId;

      if (!_channelId || typeof _channelId !== 'string') return;

      const pendingRequest = this.pendingRequests.get(_channelId);
      if (!pendingRequest) return;

      // Clean up
      this.pendingRequests.delete(_channelId);

      // Resolve or reject based on response
      if (message.err) {
        const errorMessage = message.err.message ?? 'Unknown error';
        pendingRequest.reject(new Error(errorMessage as string));
      } else if (message.response) {
        pendingRequest.resolve(message.response);
      } else {
        pendingRequest.reject(new Error('Invalid response format'));
      }
    });
  }

  // Expose VS Code API for other components to use
  getVSCodeApi(): VSCodeApi | null {
    return this.vscode;
  }

  // Send API message with _channelId pattern
  private async sendApiMessage(
    payload: Record<string, unknown>,
  ): Promise<unknown> {
    if (!this.vscode) {
      throw new Error('VS Code API not available');
    }

    return new Promise((resolve, reject) => {
      const _channelId = uuid();
      this.pendingRequests.set(_channelId, { resolve, reject });

      // Set timeout
      setTimeout(() => {
        if (this.pendingRequests.has(_channelId)) {
          this.pendingRequests.delete(_channelId);
          reject(new Error('Request timeout'));
        }
      }, 5000); // 5 second timeout

      try {
        this.vscode!.postMessage({ ...payload, _channelId });
      } catch (error) {
        this.pendingRequests.delete(_channelId);
        reject(new Error(String(error)));
      }
    });
  }

  // Save state using API pattern
  async saveState(formType: string, data: FormData): Promise<void> {
    try {
      const response = (await this.sendApiMessage({
        type: 'state-save',
        request: { formType, data },
      })) as StateResponse;

      if (!response.success) {
        throw new Error('Failed to save state');
      }
    } catch (error) {
      console.error(
        `[StateSyncManager] Error saving state for ${formType}:`,
        error,
      );
      throw error;
    }
  }

  // Load state using API pattern
  async loadState(formType: string): Promise<FormData | null> {
    try {
      const response = (await this.sendApiMessage({
        type: 'state-load',
        request: { formType },
      })) as StateResponse;

      return response.data ?? null;
    } catch (error) {
      console.error(
        `[StateSyncManager] Error loading state for ${formType}:`,
        error,
      );
      return null;
    }
  }

  // Clear state using API pattern
  async clearState(formType: string): Promise<void> {
    try {
      const response = (await this.sendApiMessage({
        type: 'state-clear',
        request: { formType },
      })) as StateResponse;

      if (!response.success) {
        throw new Error('Failed to clear state');
      }
    } catch (error) {
      console.error(
        `[StateSyncManager] Error clearing state for ${formType}:`,
        error,
      );
      throw error;
    }
  }
}

export const stateSync = new StateSyncManager();
