import type { Coder } from '@services/coder';
import { assertExhaustive } from '@shared';
import type { ApiPayload, ApiResponse } from '@shared/api/types';
import type { FormStatesFile } from '@shared/state/types';
import { FORM_STATES_FILE_VERSION } from '@shared/state/types';
import { DJ_STATE_PATH } from 'admin';
import * as path from 'path';
import * as vscode from 'vscode';

import { FileSystemUtils } from './utils/fileSystem';

export class StateManager {
  private context: vscode.ExtensionContext;
  private readonly stateFileBasePath: string;
  private stateFileCache: Map<string, FormStatesFile> = new Map();

  coder: Coder;
  handleApi: (payload: ApiPayload<'state'>) => Promise<ApiResponse>;

  constructor(context: vscode.ExtensionContext, coder: Coder) {
    this.context = context;
    this.coder = coder;

    this.stateFileBasePath = DJ_STATE_PATH;

    // Set up API handler
    this.handleApi = async (payload) => {
      try {
        switch (payload.type) {
          case 'state-load': {
            const data = await this.getFormState(payload.request.formType);
            return { data };
          }
          case 'state-save': {
            await this.saveFormState(
              payload.request.formType,
              payload.request.data,
            );
            return { success: true };
          }
          case 'state-clear': {
            await this.clearFormState(payload.request.formType);
            return { success: true };
          }
          default:
            return assertExhaustive<ApiResponse>(payload);
        }
      } catch (error: unknown) {
        console.error(
          '[StateManager] Error handling state API request:',
          error,
        );
        throw error;
      }
    };

    // Ensure state directory exists
    this.initializeStateDirectory().catch((error) => {
      console.error(
        '[StateManager] Failed to initialize state directory:',
        error,
      );
    });
  }

  /**
   * Async initialization - call this from coder activation
   */
  async activate(): Promise<void> {
    try {
      await FileSystemUtils.ensureDirectory(this.stateFileBasePath);
    } catch (error: unknown) {
      console.error('[StateManager] Failed to create state directory:', error);
      throw new Error(
        `Failed to initialize state directory: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Get the state file path for a specific form type
   * Public method to support external callers
   */
  public getStateFilePath(formType: string): string {
    // Handle specific non-edit model formTypes
    if (formType === 'model-create') {
      return path.join(this.stateFileBasePath, `temp-current-${formType}.json`);
    }

    // Handle edit model formTypes: model-{modelName}
    if (formType.startsWith('model-')) {
      const modelName = formType.replace('model-', '');
      return path.join(
        this.stateFileBasePath,
        `temp-edit-model-${modelName}.json`,
      );
    }

    // Default behavior for other form types (framework-model-update, source-create, etc.)
    return path.join(this.stateFileBasePath, `temp-current-${formType}.json`);
  }

  /**
   * Ensure the state directory exists
   */
  private async initializeStateDirectory(): Promise<void> {
    try {
      await FileSystemUtils.ensureDirectory(this.stateFileBasePath);
    } catch (error: unknown) {
      console.error('[StateManager] Failed to create state directory:', error);
      throw new Error(
        `Failed to initialize state directory: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Load the state file from disk, with caching
   */
  private async loadStateFile(formType: string): Promise<FormStatesFile> {
    const cachedState = this.stateFileCache.get(formType);
    if (cachedState) {
      return cachedState;
    }

    const stateFilePath = this.getStateFilePath(formType);
    const stateFile =
      await FileSystemUtils.readJsonFile<FormStatesFile>(stateFilePath);

    if (stateFile?.version === FORM_STATES_FILE_VERSION) {
      this.stateFileCache.set(formType, stateFile);
      return stateFile;
    }

    // Create new state file if doesn't exist or version mismatch
    const newStateFile: FormStatesFile = {
      version: FORM_STATES_FILE_VERSION,
      lastUpdated: new Date().toISOString(),
      forms: {},
    };

    this.stateFileCache.set(formType, newStateFile);
    return newStateFile;
  }

  /**
   * Save the state file to disk and update cache
   */
  private async saveStateFile(
    formType: string,
    stateFile: FormStatesFile,
  ): Promise<void> {
    try {
      // Ensure state directory exists before saving
      await this.initializeStateDirectory();

      stateFile.lastUpdated = new Date().toISOString();
      const stateFilePath = this.getStateFilePath(formType);
      await FileSystemUtils.writeJsonFile(stateFilePath, stateFile);
      this.stateFileCache.set(formType, stateFile);
    } catch (error: unknown) {
      throw new Error(
        `Failed to save form state: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Save form state - overrides existing state
   */
  async saveFormState(
    formType: string,
    data: Record<string, any>,
  ): Promise<void> {
    try {
      // Ensure state directory exists
      await this.initializeStateDirectory();

      const stateFile = await this.loadStateFile(formType);

      stateFile.forms[formType] = {
        data,
        lastModified: new Date().toISOString(),
      };

      await this.saveStateFile(formType, stateFile);
    } catch (error: unknown) {
      console.error(
        `[StateManager] Error saving form state for ${formType}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Get form state from disk
   */
  async getFormState(formType: string): Promise<Record<string, any> | null> {
    try {
      // Ensure state directory exists before trying to load
      await this.initializeStateDirectory();

      const stateFile = await this.loadStateFile(formType);
      const formState = stateFile.forms[formType];

      if (!formState) {
        return null;
      }

      return formState.data;
    } catch (error: unknown) {
      console.error(
        `[StateManager] Error loading form state for ${formType}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Clear form state from memory and disk
   */
  async clearFormState(formType: string): Promise<void> {
    try {
      const stateFile = await this.loadStateFile(formType);
      delete stateFile.forms[formType];
      await this.saveStateFile(formType, stateFile);
    } catch (error: unknown) {
      console.error(
        `[StateManager] Error clearing form state for ${formType}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Delete state file from disk
   */
  async deleteStateFile(formType: string): Promise<void> {
    try {
      const stateFilePath = this.getStateFilePath(formType);

      try {
        await vscode.workspace.fs.stat(vscode.Uri.file(stateFilePath));
        await vscode.workspace.fs.delete(vscode.Uri.file(stateFilePath));
      } catch {
        // File doesn't exist, that's okay
      }

      this.stateFileCache.delete(formType);
    } catch (error: unknown) {
      console.error(
        `[StateManager] Error deleting state file for ${formType}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Get all active edit drafts
   * Searches for temp-edit-model-* files
   */
  async getEditDrafts(): Promise<
    Array<{ formType: string; modelName: string; lastModified: Date }>
  > {
    try {
      const drafts: Array<{
        formType: string;
        modelName: string;
        lastModified: Date;
      }> = [];

      // Ensure state directory exists
      try {
        await vscode.workspace.fs.createDirectory(
          vscode.Uri.file(this.stateFileBasePath),
        );
      } catch {
        // Directory may already exist, ignore error
      }

      const files = await vscode.workspace.fs.readDirectory(
        vscode.Uri.file(this.stateFileBasePath),
      );

      for (const [fileName, fileType] of files) {
        if (
          fileType === vscode.FileType.File &&
          fileName.startsWith('temp-edit-model-') &&
          fileName.endsWith('.json')
        ) {
          try {
            // Extract model name from filename: temp-edit-model-{modelName}.json
            const modelName = fileName
              .replace('temp-edit-model-', '')
              .replace('.json', '');

            const formType = `model-${modelName}`;
            const stateFile = await this.loadStateFile(formType);
            const formState = stateFile.forms[formType];

            if (formState?.data) {
              const lastModified = new Date(formState.lastModified);
              drafts.push({ formType, modelName, lastModified });
            }
          } catch (error: unknown) {
            console.error(
              `[StateManager] Error reading edit draft ${fileName}:`,
              error,
            );
          }
        }
      }

      return drafts.sort(
        (a, b) => b.lastModified.getTime() - a.lastModified.getTime(),
      );
    } catch (error: unknown) {
      console.error('[StateManager] Error getting edit drafts:', error);
      return [];
    }
  }

  /**
   * Create temp file for model edit
   */
  async createModelEditTempFile(modelName: string): Promise<string> {
    try {
      const formType = `model-${modelName}`;

      // Create initial empty state for the model
      await this.saveFormState(formType, {});

      return formType;
    } catch (error: unknown) {
      console.error(
        `[StateManager] Error creating temp file for model ${modelName}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Clear a specific model edit draft
   */
  async clearModelEditDraft(modelName: string): Promise<void> {
    try {
      const formType = `model-${modelName}`;
      await this.clearFormState(formType);
      await this.deleteStateFile(formType);
    } catch (error: unknown) {
      console.error(
        `[StateManager] Error clearing model edit draft ${modelName}:`,
        error,
      );
      throw error;
    }
  }
}
