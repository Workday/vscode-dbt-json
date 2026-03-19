import * as vscode from 'vscode';

/**
 * Open or show a file in VS Code editor.
 * If the file is already open in a visible editor, focuses it.
 * Otherwise, opens it in a new editor.
 *
 * @param pathOrUri - File path (string) or VS Code Uri
 * @param options - Optional editor options (e.g., viewColumn)
 *
 * @example
 * ```typescript
 * // Open file beside current editor
 * await showOrOpenFile('/path/to/file.ts', {
 *   viewColumn: vscode.ViewColumn.Beside
 * });
 *
 * // Open file with Uri
 * const uri = vscode.Uri.file('/path/to/file.ts');
 * await showOrOpenFile(uri);
 * ```
 */
export async function showOrOpenFile(
  pathOrUri: string | vscode.Uri,
  options?: {
    viewColumn?: vscode.ViewColumn;
  },
): Promise<void> {
  const uri =
    typeof pathOrUri === 'string' ? vscode.Uri.file(pathOrUri) : pathOrUri;
  const activeEditors = vscode.window.visibleTextEditors;
  const activeEditor = activeEditors.find(
    (e) => e.document.uri.fsPath === uri.fsPath,
  );
  if (activeEditor) {
    await vscode.window.showTextDocument(activeEditor.document);
  } else {
    await vscode.window.showTextDocument(uri, options);
  }
}
