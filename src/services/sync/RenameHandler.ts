/**
 * Rename Handler Module
 *
 * Handles model rename detection and reference propagation.
 * Extracted from framework.ts for single responsibility and testability.
 *
 * Responsibilities:
 * - Detect when model name in JSON differs from file path
 * - Check for conflicts with existing models
 * - Propagate renames to all referencing model files
 */

import { frameworkGenerateModelOutput } from '@services/framework/utils';
import type { CoderConfig } from '@services/types/config';
import { jsonParse } from '@shared';
import type { DbtProject } from '@shared/dbt/types';
import type { FrameworkModel, FrameworkSyncOp } from '@shared/framework/types';
import * as path from 'path';
import * as vscode from 'vscode';

import { SYNC_BATCH_SIZES } from './constants';
import type {
  DetectedRename,
  RenameConflictResult,
  SyncLogger,
  SyncRoot,
} from './types';

/**
 * RenameHandler manages model rename detection and reference propagation.
 *
 * When a user renames a model (changes the name in the JSON file), this class:
 * 1. Detects the rename by comparing JSON content with file path
 * 2. Checks if the new name conflicts with an existing model
 * 3. Updates all other model files that reference the renamed model
 *
 * @example
 * ```typescript
 * const handler = new RenameHandler(logger);
 *
 * // Detect renames
 * const renames = handler.detectRenames({ roots, project });
 *
 * // Check for conflicts
 * for (const rename of renames) {
 *   const conflict = handler.checkConflict({ newName: rename.newName, project });
 *   if (conflict.hasConflict) {
 *     // Handle conflict
 *   }
 * }
 *
 * // Propagate renames
 * const ops = await handler.propagateRenames({ renames, jsonUris: _jsonUris, project, config });
 * ```
 */
export class RenameHandler {
  constructor(private readonly logger: SyncLogger) {}

  /**
   * Detect renames by comparing model IDs with file paths.
   *
   * A rename is detected when:
   * - The root is a model (starts with "model.")
   * - The model name in the ID differs from the file name
   *
   * @param params - Detection parameters
   * @param params.roots - Root resources being synced
   * @param params.project - Current dbt project (unused, but kept for consistency)
   * @returns Array of detected renames
   */
  detectRenames(params: {
    roots: SyncRoot[];
    project: DbtProject;
  }): DetectedRename[] {
    const { roots } = params;
    const renames: DetectedRename[] = [];

    for (const root of roots) {
      // Only check models with a path
      if (!root.id?.startsWith('model.') || !root.pathJson) {
        continue;
      }

      // Extract model name from ID (e.g., "model.project.stg__users" -> "stg__users")
      const modelIdName = root.id.split('.')[2];

      // Extract model name from file path
      const modelPathName = path
        .basename(root.pathJson)
        .replace(/\.model\.json$/, '');

      // If they differ, it's a rename
      if (modelIdName && modelPathName && modelIdName !== modelPathName) {
        renames.push({
          oldName: modelPathName,
          newName: modelIdName,
          pathJson: root.pathJson,
        });
      }
    }

    return renames;
  }

  /**
   * Check if a rename would conflict with an existing model.
   *
   * Uses a disk-based check: does a *different* file named
   * `{newName}.model.json` already exist on disk? This is deterministic
   * and immune to stale manifests.
   *
   * Every model managed by the framework has a `.model.json` file on disk,
   * so the `jsonUris` array (from `vscode.workspace.findFiles()` at the
   * start of each sync) is the authoritative source of truth.
   *
   * A manifest-based fallback was previously used but removed because it
   * caused false-positive conflicts when the manifest was stale (e.g.,
   * after a failed `dbt parse`).
   *
   * @param params - Conflict check parameters
   * @param params.newName - The new model name to check
   * @param params.oldName - The old model name (current file name) being renamed from
   * @param params.project - Current dbt project with manifest (unused, kept for interface stability)
   * @param params.jsonUris - Actual `.model.json` / `.source.json` files on disk
   * @returns Conflict result indicating if model exists
   */
  checkConflict(params: {
    newName: string;
    oldName: string;
    project: DbtProject;
    jsonUris: vscode.Uri[];
  }): RenameConflictResult {
    const { newName, oldName, jsonUris } = params;

    // Disk-based check: does a DIFFERENT file named {newName}.model.json
    // exist on disk? This is deterministic and immune to stale manifests.
    const newFileName = `${newName}.model.json`;
    const oldFileName = `${oldName}.model.json`;
    const conflictingFileOnDisk = jsonUris.find((uri) => {
      const basename = path.basename(uri.fsPath);
      // A conflict exists if a file with the new name exists AND
      // it's not the file being renamed (which still has the old name)
      return basename === newFileName && basename !== oldFileName;
    });

    if (conflictingFileOnDisk) {
      this.logger.info(
        `Rename conflict check: '${newName}' — conflicting file found on disk: ${conflictingFileOnDisk.fsPath}`,
      );
      return {
        hasConflict: true,
        existingPath: conflictingFileOnDisk.fsPath,
      };
    }

    this.logger.info(
      `Rename conflict check: '${newName}' — no conflicting file on disk, allowing rename`,
    );
    return { hasConflict: false };
  }

  /**
   * Propagate renames to all model files that reference the renamed model.
   *
   * This method uses the manifest's child_map to efficiently target only
   * the models that depend on the renamed model, then processes them in parallel.
   *
   * @param params - Propagation parameters
   * @param params.renames - Detected renames to propagate
   * @param params.jsonUris - All JSON file URIs in workspace (fallback only)
   * @param params.project - Current dbt project
   * @param params.extensionConfig - Extension config for SQL generation
   * @returns File operations to update referencing files
   */
  async propagateRenames(params: {
    renames: DetectedRename[];
    jsonUris: vscode.Uri[];
    project: DbtProject;
    extensionConfig: CoderConfig;
  }): Promise<FrameworkSyncOp[]> {
    const { renames, project, extensionConfig } = params;
    const operations: FrameworkSyncOp[] = [];

    if (renames.length === 0) {
      return operations;
    }

    this.logger.info('Propagating model renames to referencing files');

    // Collect all files that need updating using child_map
    const filesToProcess = new Set<string>();

    for (const rename of renames) {
      // Construct the old model ID
      const oldModelId = `model.${project.name}.${rename.oldName}`;

      // Get children from child_map
      const children = project.manifest.child_map?.[oldModelId] ?? [];

      this.logger.info(
        `Found ${children.length} children for renamed model ${rename.oldName}`,
      );

      // Resolve file paths for each child model
      for (const childId of children) {
        // Only process models (not tests, exposures, etc.)
        if (!childId.startsWith('model.')) {
          continue;
        }

        const childNode = project.manifest.nodes[childId];
        if (childNode?.original_file_path) {
          // Convert relative path to absolute system path
          const absolutePath = path.join(
            project.pathSystem,
            childNode.original_file_path,
          );

          // Replace .sql with .model.json to get the JSON file path
          const jsonPath = absolutePath.replace(/\.sql$/, '.model.json');

          filesToProcess.add(jsonPath);
        }
      }
    }

    this.logger.info(
      `Processing ${filesToProcess.size} files for rename propagation`,
    );

    // Pre-calculate replacement patterns for all renames.
    // Order matters: "model": "old_name" is replaced first, then the bare
    // "old_name" pattern catches remaining occurrences (e.g., inside
    // from.union.models arrays) without double-replacing.
    const renamePatterns = renames.map((rename) => ({
      pathJson: rename.pathJson,
      replacements: [
        {
          // Direct model reference: "model": "old_name"
          regex: new RegExp(`"model": "${rename.oldName}"`, 'g'),
          replacement: `"model": "${rename.newName}"`,
        },
        {
          // Model name as a bare JSON string value (e.g., in union.models arrays):
          //   "models": ["old_name", ...]
          // Applied after the "model": pattern so the old name only appears
          // in positions not yet replaced. The full model name (e.g.,
          // "int__group__topic__name") is unique enough to avoid false matches
          // with short fields like "name" which only contain the suffix.
          regex: new RegExp(`"${rename.oldName}"`, 'g'),
          replacement: `"${rename.newName}"`,
        },
        {
          // Column reference: old_name.column_name
          regex: new RegExp(`${rename.oldName}\\.`, 'g'),
          replacement: `${rename.newName}.`,
        },
      ],
    }));

    // Process files in parallel batches to optimize performance
    const fileArray = Array.from(filesToProcess);
    const batchSize = SYNC_BATCH_SIZES.RENAME_PROPAGATION;

    for (let i = 0; i < fileArray.length; i += batchSize) {
      const batch = fileArray.slice(i, i + batchSize);

      const batchOps = await Promise.all(
        batch.map(async (pathJson) => {
          try {
            const uriJson = vscode.Uri.file(pathJson);

            // Read the model file
            const modelJsonFileOld =
              await vscode.workspace.fs.readFile(uriJson);
            let modelJsonString = modelJsonFileOld.toString();
            let hasChanges = false;

            // Check each rename and apply replacements
            for (const {
              pathJson: renamePath,
              replacements,
            } of renamePatterns) {
              // Skip the file being renamed itself
              if (pathJson === renamePath) {
                continue;
              }

              // Apply all replacements for this rename
              for (const { regex, replacement } of replacements) {
                const updated = modelJsonString.replaceAll(regex, replacement);
                if (updated !== modelJsonString) {
                  hasChanges = true;
                  modelJsonString = updated;
                }
              }
            }

            // If changes were made, regenerate SQL and return operations
            if (hasChanges) {
              const modelJsonNew = jsonParse(modelJsonString) as FrameworkModel;
              const generated = frameworkGenerateModelOutput({
                dj: { config: extensionConfig },
                project,
                modelJson: modelJsonNew,
              });

              const pathSql = pathJson.replace(/\.model\.json$/, '.sql');

              this.logger.info(
                `Updated references in: ${path.basename(pathJson)}`,
              );

              return [
                {
                  type: 'write' as const,
                  path: pathJson,
                  text: modelJsonString,
                },
                {
                  type: 'write' as const,
                  path: pathSql,
                  text: generated.sql,
                },
              ];
            }

            return [];
          } catch (err: unknown) {
            this.logger.error(
              `Failed to process file during rename propagation: ${pathJson}`,
              err,
            );
            return [];
          }
        }),
      );

      // Flatten and collect operations
      for (const ops of batchOps) {
        operations.push(...ops);
      }
    }

    return operations;
  }

  /**
   * Check all renames for conflicts and return any that conflict.
   *
   * @param params - Check parameters
   * @param params.renames - Renames to check
   * @param params.project - Current dbt project
   * @param params.jsonUris - Actual `.model.json` / `.source.json` files on disk
   * @returns Array of renames that have conflicts
   */
  findConflicts(params: {
    renames: DetectedRename[];
    project: DbtProject;
    jsonUris: vscode.Uri[];
  }): DetectedRename[] {
    const { renames, project, jsonUris } = params;
    const conflicts: DetectedRename[] = [];

    for (const rename of renames) {
      const result = this.checkConflict({
        newName: rename.newName,
        oldName: rename.oldName,
        project,
        jsonUris,
      });
      if (result.hasConflict) {
        conflicts.push(rename);
      }
    }

    return conflicts;
  }
}
