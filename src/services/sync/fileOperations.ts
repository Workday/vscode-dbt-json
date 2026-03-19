/**
 * File Operations Module
 *
 * This module provides efficient, batched file I/O operations
 * for the sync engine. Key features:
 * - Parallel file reads using Promise.all
 * - Batched file writes to avoid overwhelming the file system
 * - Async operations throughout (no blocking sync calls)
 */

import type { FrameworkSyncOp } from '@shared/framework/types';
import * as fs from 'fs';
import * as vscode from 'vscode';

import { SYNC_BATCH_SIZES } from './constants';
import type { SyncLogger } from './types';

/**
 * Reads a single file asynchronously
 * Returns empty string if file doesn't exist
 */
async function readFileAsync(filePath: string): Promise<string> {
  try {
    return await fs.promises.readFile(filePath, 'utf8');
  } catch {
    return '';
  }
}

/**
 * Executes sync operations using VS Code's workspace.fs API
 * This is preferred when running in the extension context as it
 * integrates with VS Code's file system abstraction
 *
 * @param operations - Array of FrameworkSyncOp operations
 * @param batchSize - Maximum number of concurrent operations
 * @param logger - Optional logger for progress reporting
 */
export async function executeSyncOperationsVSCode(
  operations: FrameworkSyncOp[],
  batchSize: number = SYNC_BATCH_SIZES.FILE_OPERATIONS,
  logger?: SyncLogger,
): Promise<{
  writesCount: number;
  deletesCount: number;
  renamesCount: number;
}> {
  // Separate operations by type
  const renameOps: { oldPath: string; newPath: string }[] = [];
  const deleteOps: string[] = [];
  const writeOps: { path: string; content: string }[] = [];

  for (const op of operations) {
    if (op.type === 'delete') {
      deleteOps.push(op.path);
    } else if (op.type === 'rename') {
      renameOps.push({ oldPath: op.oldPath, newPath: op.newPath });
    } else if (op.type === 'write') {
      writeOps.push({ path: op.path, content: op.text });
    }
  }

  // Execute renames FIRST using WorkspaceEdit for atomicity.
  // WorkspaceEdit.renameFile() atomically renames the file AND updates
  // all open editor tabs to point to the new path, preventing the
  // race condition where auto-save re-creates the old file.
  let renameFailed = false;
  if (renameOps.length > 0) {
    logger?.info(`Renaming ${renameOps.length} files...`);
    const edit = new vscode.WorkspaceEdit();
    for (const { oldPath, newPath } of renameOps) {
      edit.renameFile(vscode.Uri.file(oldPath), vscode.Uri.file(newPath), {
        overwrite: true,
      });
    }
    try {
      const renameSuccess = await vscode.workspace.applyEdit(edit);
      if (!renameSuccess) {
        logger?.error('Failed to apply rename WorkspaceEdit');
        renameFailed = true;
      }

      // Close any editor tabs still pointing to old paths.
      // WorkspaceEdit.renameFile() should redirect tabs, but web-based
      // VS Code (Coder) doesn't always do this. Closing old tabs prevents
      // auto-save from re-creating the old file.
      if (renameSuccess) {
        for (const { oldPath } of renameOps) {
          const oldUri = vscode.Uri.file(oldPath);
          for (const group of vscode.window.tabGroups.all) {
            for (const tab of group.tabs) {
              if (
                tab.input instanceof vscode.TabInputText &&
                tab.input.uri.fsPath === oldUri.fsPath
              ) {
                try {
                  await vscode.window.tabGroups.close(tab);
                } catch {
                  // Tab may already be closed
                }
              }
            }
          }
        }
      }
    } catch {
      // Rename failed (e.g., old file already deleted).
      // We must clean up old files before writing new ones to avoid duplicates.
      renameFailed = true;
      logger?.info(
        'Rename WorkspaceEdit failed, cleaning up old files before writing',
      );
    }

    // If rename failed, delete old files to prevent duplicates.
    // The write operations below will create files at the new paths.
    if (renameFailed) {
      for (const { oldPath } of renameOps) {
        try {
          await vscode.workspace.fs.delete(vscode.Uri.file(oldPath));
        } catch {
          // Old file may not exist — that's fine
        }
      }
      // Also close old tabs to prevent auto-save from re-creating them
      for (const { oldPath } of renameOps) {
        const oldUri = vscode.Uri.file(oldPath);
        for (const group of vscode.window.tabGroups.all) {
          for (const tab of group.tabs) {
            if (
              tab.input instanceof vscode.TabInputText &&
              tab.input.uri.fsPath === oldUri.fsPath
            ) {
              try {
                await vscode.window.tabGroups.close(tab);
              } catch {
                // Tab may already be closed
              }
            }
          }
        }
      }
    }
  }

  // Execute deletes (in parallel batches)
  if (deleteOps.length > 0) {
    logger?.info(`Deleting ${deleteOps.length} files...`);
    for (let i = 0; i < deleteOps.length; i += batchSize) {
      const batch = deleteOps.slice(i, i + batchSize);
      await Promise.all(
        batch.map(async (filePath) => {
          try {
            await vscode.workspace.fs.delete(vscode.Uri.file(filePath));
          } catch {
            // Ignore errors for files that don't exist
          }
        }),
      );
    }
  }

  // Then execute writes (in parallel batches)
  if (writeOps.length > 0) {
    logger?.info(`Writing ${writeOps.length} files...`);
    for (let i = 0; i < writeOps.length; i += batchSize) {
      const batch = writeOps.slice(i, i + batchSize);
      await Promise.all(
        batch.map(async (op) => {
          await vscode.workspace.fs.writeFile(
            vscode.Uri.file(op.path),
            Buffer.from(op.content),
          );
        }),
      );
    }
  }

  return {
    writesCount: writeOps.length,
    deletesCount: deleteOps.length,
    renamesCount: renameOps.length,
  };
}

/**
 * Builds a Map of file paths to VS Code URIs for O(1) lookup
 *
 * @param uris - Array of VS Code URIs
 * @returns Map of fsPath -> Uri
 */
export function buildUriMap(uris: vscode.Uri[]): Map<string, vscode.Uri> {
  return new Map(uris.map((uri) => [uri.fsPath, uri]));
}

/**
 * Reads the old SQL and YML files for comparison
 * Uses async I/O for better performance
 *
 * @param prefix - File path prefix (without extension)
 * @returns Object with oldSql and oldYml content
 */
export async function readOldModelFiles(prefix: string): Promise<{
  oldSql: string;
  oldYml: string;
}> {
  const [oldSql, oldYml] = await Promise.all([
    readFileAsync(`${prefix}.sql`),
    readFileAsync(`${prefix}.yml`),
  ]);

  return { oldSql, oldYml };
}

/**
 * Reads the old YML file for a source
 *
 * @param prefix - File path prefix (without extension)
 * @returns Old YML content
 */
export async function readOldSourceFile(prefix: string): Promise<string> {
  return readFileAsync(`${prefix}.yml`);
}
