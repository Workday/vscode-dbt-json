import * as vscode from 'vscode';
import * as path from 'path';
import { FileSystemUtils } from '../utils/fileSystem';
import { FormStatesFile, FORM_STATES_FILE_VERSION } from '@shared/state/types';

export class StateManager {
  private context: vscode.ExtensionContext;
  private readonly stateFileBasePath: string;

  private stateFileCache: Map<string, FormStatesFile> = new Map();

  constructor(context: vscode.ExtensionContext) {
    this.context = context;

    // Get workspace root path
    const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    if (!workspaceRoot) {
      console.error('[StateManager] No workspace folder found');
      throw new Error('No workspace folder found');
    }

    // Set base path for state files
    this.stateFileBasePath = path.join(workspaceRoot, '.dj');

    // Ensure .vscode directory exists
    this.initializeStateDirectory().catch((error) => {
      console.error(
        '[StateManager] Failed to initialize state directory:',
        error,
      );
    });
  }

  /**
   * Get the state file path for a specific form type
   */
  private getStateFilePath(formType: string): string {
    return path.join(this.stateFileBasePath, `temp-current-${formType}.json`);
  }

  /**
   * Ensure the .vscode directory exists
   */
  private async initializeStateDirectory(): Promise<void> {
    try {
      await FileSystemUtils.ensureDirectory(this.stateFileBasePath);
    } catch (error) {
      console.error(
        '[StateManager] Failed to create .vscode directory:',
        error,
      );
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

    if (stateFile && stateFile.version === FORM_STATES_FILE_VERSION) {
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
      stateFile.lastUpdated = new Date().toISOString();
      const stateFilePath = this.getStateFilePath(formType);
      await FileSystemUtils.writeJsonFile(stateFilePath, stateFile);
      this.stateFileCache.set(formType, stateFile);
    } catch (error) {
      throw new Error(
        `Failed to save form state: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  // Save form state - overrides existing state
  async saveFormState(
    formType: string,
    data: Record<string, any>,
  ): Promise<void> {
    try {
      const stateFile = await this.loadStateFile(formType);

      stateFile.forms[formType] = {
        data,
        lastModified: new Date().toISOString(),
      };

      await this.saveStateFile(formType, stateFile);
    } catch (error) {
      throw error;
    }
  }

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
    } catch (error) {
      console.error(
        `[StateManager] Error loading form state for ${formType}:`,
        error,
      );
      return null;
    }
  }

  async clearFormState(formType: string): Promise<void> {
    try {
      const stateFile = await this.loadStateFile(formType);
      delete stateFile.forms[formType];
      await this.saveStateFile(formType, stateFile);
    } catch (error) {
      console.error(
        `[StateManager] Error clearing form state for ${formType}:`,
        error,
      );
      throw error;
    }
  }
}
