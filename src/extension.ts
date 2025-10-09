import { Coder } from '@services/coder';
import * as vscode from 'vscode';

let coder: Coder;

export async function activate(context: vscode.ExtensionContext) {
  coder = new Coder(context);
  coder.activate();
}

export function deactivate() {
  coder?.deactivate();
}
