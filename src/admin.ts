import { getDjConfig } from '@services/config';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

export { ThemeIcon } from 'vscode';

export const WORKSPACE_ROOT =
  vscode.workspace.workspaceFolders?.[0].uri.fsPath || '';
export const BASE_AIRFLOW_PATH = path.join(__dirname, '../../airflow');
export const BASE_MACROS_PATH = path.join(__dirname, '../../macros');
export const BASE_SCHEMAS_PATH = path.join(__dirname, '../../schemas');

export const DJ_SCHEMAS_PATH = path.join(WORKSPACE_ROOT, `.dj/schemas`);
export const DJ_SQL_PATH = path.join(WORKSPACE_ROOT, `.dj/sql`);

export const convertArgsForEnv = ({
  command,
  path,
  venv,
}: {
  command: string;
  path?: string;
  venv?: string;
}): { command: string; path: string } => {
  path =
    path?.replace(WORKSPACE_ROOT, '')?.replace(/^\//, '').replace(/\/$/, '') ||
    '';
  if (venv) {
    const pathLevels = path ? path.split('/').length : 0;
    const venvRoot = Array(pathLevels).fill('../').join('');
    command = `source ${venvRoot}${venv}/bin/activate; ${command}`;
    command = command.replace('trino-cli', 'trino');
  }
  return { command, path };
};

/**
 * Get Trino connection configuration with precedence: VS Code > Environment > Default
 */
export function getTrinoConfig() {
  let { trinoPath } = getDjConfig();

  if (trinoPath) {
    // Remove any trailing slashes
    trinoPath = trinoPath.replace(/\/+$/, '');
  }

  const path = trinoPath ? `${trinoPath}/trino-cli` : 'trino-cli';

  return {
    path,
  };
}

export function djSqlPath({ name }: { name: string }) {
  return path.join(DJ_SQL_PATH, name);
}
export function djSqlRead({ name }: { name: string }) {
  fs.readFileSync(djSqlPath({ name }), 'utf8');
}
export function djSqlWrite({ name, sql }: { name: string; sql: string }) {
  writeFile({
    filePath: djSqlPath({ name }),
    fileText: sql,
  });
}

function writeFile({
  filePath,
  fileText,
}: {
  filePath: string;
  fileText: string;
}) {
  const dirName = path.dirname(filePath);
  if (!fs.existsSync(dirName)) {
    fs.mkdirSync(dirName, { recursive: true });
  }
  fs.writeFileSync(filePath, Buffer.from(fileText, 'utf8'));
}

export function timestamp() {
  return new Date().toISOString().slice(0, 23);
}

export type SettingFileAssociations = Record<string, string>;

export type SettingJsonSchema = {
  fileMatch: string[];
  schema?: { allowTrailingCommas?: boolean; $ref?: string };
  url?: string;
};

export type TreeItem = vscode.TreeItem & { children?: TreeItem[] };
export type TreeData = TreeItem[];

export class TreeDataInstance implements vscode.TreeDataProvider<TreeItem> {
  private _onDidChangeTreeData: vscode.EventEmitter<
    vscode.TreeItem | undefined | null | void
  > = new vscode.EventEmitter<TreeItem | undefined | null | void>();
  readonly onDidChangeTreeData: vscode.Event<
    TreeItem | undefined | null | void
  > = this._onDidChangeTreeData.event;

  data: TreeItem[];

  constructor(_data?: TreeItem[]) {
    this.data = _data || [];
  }

  getTreeItem(element: TreeItem): TreeItem | Thenable<TreeItem> {
    return element;
  }

  getChildren(
    element?: (TreeItem & { children?: TreeItem[] }) | undefined,
  ): vscode.ProviderResult<TreeItem[]> {
    if (!element) return this.data;
    return element?.children || [];
  }

  setData(data: TreeItem[]): void {
    this.data = data;
    this._onDidChangeTreeData.fire();
  }
}

export class WebviewViewInstance implements vscode.WebviewViewProvider {
  onResolve?: () => Promise<void>;
  view?: vscode.WebviewView;

  constructor({ onResolve }: { onResolve?: () => Promise<void> }) {
    this.onResolve = onResolve;
  }

  resolveWebviewView(_view: vscode.WebviewView) {
    this.view = _view;
    this.onResolve?.();
  }
  setHtml(html: string) {
    if (!this.view) return;
    this.view.webview.html = html;
  }
}
