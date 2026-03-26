import type * as vscode from 'vscode';

import { getHtml } from './utils';

/**
 * Generic provider for bottom panel webview views.
 * Reusable for any panel tab (column lineage, model lineage, etc.)
 */
export class PanelViewProvider implements vscode.WebviewViewProvider {
  private _view?: vscode.WebviewView;

  constructor(
    private readonly extensionUri: vscode.Uri,
    private readonly route: string,
    private readonly options?: {
      enableScripts?: boolean;
    },
    private readonly messageHandler?: (
      message: any,
      webview: vscode.Webview,
    ) => void,
    private readonly onResolve?: (webviewView: vscode.WebviewView) => void,
  ) {}

  /**
   * Resolves the webview view when VS Code requests it
   */
  resolveWebviewView(
    webviewView: vscode.WebviewView,
    _context: vscode.WebviewViewResolveContext,
    _token: vscode.CancellationToken,
  ): void | Thenable<void> {
    this._view = webviewView;

    webviewView.webview.options = {
      enableScripts: this.options?.enableScripts ?? true,
      localResourceRoots: [this.extensionUri],
    };

    // Set HTML content using existing getHtml utility
    webviewView.webview.html = getHtml({
      extensionUri: this.extensionUri,
      route: this.route,
      webview: webviewView.webview,
    });

    // Set up message handler to forward API requests to extension
    if (this.messageHandler) {
      webviewView.webview.onDidReceiveMessage((message) => {
        this.messageHandler!(message, webviewView.webview);
      });
    }

    this.onResolve?.(webviewView);
  }

  /**
   * Post a message to the webview
   */
  postMessage(message: any): Thenable<boolean> | undefined {
    return this._view?.webview.postMessage(message);
  }

  /**
   * Register a message handler for messages from the webview
   */
  onDidReceiveMessage(
    listener: (message: any) => any,
    thisArgs?: any,
    disposables?: vscode.Disposable[],
  ): vscode.Disposable | undefined {
    return this._view?.webview.onDidReceiveMessage(
      listener,
      thisArgs,
      disposables,
    );
  }

  /**
   * Focus the panel view
   */
  focus(): void {
    if (this._view) {
      this._view.show?.(true);
    }
  }

  /**
   * Check if the view is currently visible
   */
  get visible(): boolean {
    return this._view?.visible ?? false;
  }

  /**
   * Get the webview instance (if resolved)
   */
  get webview(): vscode.Webview | undefined {
    return this._view?.webview;
  }

  get resolved(): boolean {
    return !!this._view;
  }
}
