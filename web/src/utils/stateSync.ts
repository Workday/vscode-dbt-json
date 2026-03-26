import type { VSCodeApi } from '@shared/types/config';
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

      if (!_channelId) return;

      const pendingRequest = this.pendingRequests.get(_channelId);
      if (!pendingRequest) return;

      // Clean up
      this.pendingRequests.delete(_channelId);

      // Resolve or reject based on response
      if (message.err) {
        pendingRequest.reject(
          new Error(message.err.message || 'Unknown error'),
        );
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
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  // Save state using API pattern
  async saveState(formType: string, data: FormData): Promise<void> {
    // If VS Code API is not available, skip silently
    if (!this.vscode) {
      console.debug(
        `[StateSyncManager] VS Code API not available, skipping state save for ${formType}`,
      );
      return;
    }
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
    // If VS Code API is not available, return null immediately
    if (!this.vscode) {
      console.debug(
        `[StateSyncManager] VS Code API not available, skipping state load for ${formType}`,
      );
      return null;
    }

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
    // If VS Code API is not available, skip silently
    if (!this.vscode) {
      console.debug(
        `[StateSyncManager] VS Code API not available, skipping state clear for ${formType}`,
      );
      return;
    }

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
