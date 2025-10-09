import * as vscode from 'vscode';

/**
 * Interface for services that can register VS Code commands and providers
 */
export interface DJService {
  /**
   * Activate the service
   */
  activate(context: vscode.ExtensionContext): Promise<void>;

  /**
   * Register VS Code commands specific to this service
   */
  registerCommands?(context: vscode.ExtensionContext): void;

  /**
   * Register VS Code language providers (code lens, definitions, etc.)
   */
  registerProviders?(context: vscode.ExtensionContext): void;

  /**
   * Register VS Code event handlers
   */
  registerEventHandlers?(context: vscode.ExtensionContext): void;
}
