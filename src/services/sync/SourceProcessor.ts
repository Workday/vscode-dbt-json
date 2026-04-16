/**
 * Source Processor Module
 *
 * Handles source-specific processing logic for the sync engine.
 * Extracted from framework.ts for single responsibility and testability.
 *
 * Responsibilities:
 * - Parse source JSON files
 * - Check cache for changes
 * - Generate YML output
 * - Build file operations
 * - Update cache after successful generation
 */

import {
  frameworkGenerateSourceOutput,
  frameworkMakeSourceName,
  frameworkMakeSourcePrefix,
} from '@services/framework/utils';
import { jsonParse } from '@shared';
import type { DbtProject } from '@shared/dbt/types';
import type { FrameworkSource, FrameworkSyncOp } from '@shared/framework/types';
import * as fs from 'fs';
import * as vscode from 'vscode';

import type { CacheManager } from './cacheManager';
import { ERROR_MESSAGES } from './constants';
import type {
  SourceProcessingResult,
  SyncCallbacks,
  SyncConfig,
  SyncResource,
} from './types';

/**
 * SourceProcessor handles the processing of individual source resources.
 *
 * This class encapsulates all source-specific logic:
 * - JSON parsing
 * - Change detection via cache
 * - YML generation
 * - File operation building
 *
 * Note: Sources only generate YML files, not SQL.
 *
 * @example
 * ```typescript
 * const processor = new SourceProcessor(config, cacheManager, callbacks);
 * const result = await processor.process({
 *   resource,
 *   jsonContent,
 *   project,
 * });
 * ```
 */
export class SourceProcessor {
  constructor(
    private readonly config: SyncConfig,
    private readonly cacheManager: CacheManager,
    private readonly callbacks: SyncCallbacks = {},
  ) {}

  /**
   * Process a single source resource.
   *
   * @param params - Processing parameters
   * @param params.resource - The source resource to process
   * @param params.jsonContent - Raw JSON file content
   * @param params.project - Current dbt project state
   * @returns Processing result with operations and updated project
   */
  async process(params: {
    resource: SyncResource;
    jsonContent: string;
    project: DbtProject;
  }): Promise<SourceProcessingResult> {
    const { resource, jsonContent, project } = params;
    const { pathJson } = resource;

    // 1. Parse the source JSON
    const sourceJson = jsonParse(jsonContent) as FrameworkSource;
    const sourceName = frameworkMakeSourceName(sourceJson);

    // 2. Calculate paths
    const oldPrefix = pathJson.replace(/\.source\.json$/, '');
    const newPrefix = frameworkMakeSourcePrefix({ ...sourceJson, project });

    if (!newPrefix) {
      this.config.logger.error(ERROR_MESSAGES.UNABLE_TO_GET_PREFIX(sourceName));
      return {
        sourceId: resource.id,
        sourceName,
        yml: '',
        updatedProject: null,
        operations: [],
        skipped: true,
      };
    }

    const newJson = JSON.stringify(sourceJson, null, '    ');
    const newJsonPath = `${newPrefix}.source.json`;
    const newYmlPath = `${newPrefix}.yml`;

    // 3. Read old YML for comparison (async)
    const oldYmlPath = `${oldPrefix}.yml`;
    const oldYml = await fs.promises
      .readFile(oldYmlPath, 'utf8')
      .catch(() => '');

    // 4. Generate YML
    let newYml = '';
    let updatedProject = project;

    try {
      const generated = frameworkGenerateSourceOutput({ project, sourceJson });
      updatedProject = generated.project;
      newYml = generated.yml;

      // Clear diagnostics on success
      const newJsonUri = vscode.Uri.file(newJsonPath);
      this.callbacks.onDiagnosticsClear?.(newJsonUri);
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.config.logger.error(
        `ERROR GENERATING YML FOR SOURCE: ${sourceName}`,
        error,
      );
      return {
        sourceId: resource.id,
        sourceName,
        yml: '',
        updatedProject: null,
        operations: [],
        skipped: false,
      };
    }

    // 5. Validate generated output
    if (!newYml) {
      this.config.logger.error(
        ERROR_MESSAGES.EMPTY_OUTPUT_GENERATED(sourceName),
      );
      return {
        sourceId: resource.id,
        sourceName,
        yml: '',
        updatedProject: null,
        operations: [],
        skipped: false,
      };
    }

    // 6. Build file operations
    const operations: FrameworkSyncOp[] = [];

    // 7. Check if we can skip file I/O by comparing actual file content.
    // This catches manual edits and ensures we don't overwrite unchanged files.
    //
    // CRITICAL: Even if we skip writing files, we must return the 'updatedProject'.
    // This ensures that the accumulated 'project' object in SyncEngine contains the full,
    // up-to-date metadata for all resources, which is required for downstream inheritance.
    //
    // Note: Cache will be updated AFTER file writes succeed (in SyncEngine.execute())
    const isContentUnchanged = newYml === oldYml;

    if (isContentUnchanged && pathJson === newJsonPath) {
      // Content matches and path is same - no need to write files, but we DO return updatedProject
      // This handles the "stale manifest but fresh files" case
      return {
        sourceId: resource.id,
        sourceName,
        yml: newYml,
        updatedProject,
        operations: [], // No file ops
        skipped: true, // Mark as skipped for stats, but we provide updatedProject
        cacheInfo: {
          pathJson,
          jsonContent,
          sql: '', // Sources don't have SQL
          yml: newYml,
          isSource: true,
        },
      };
    }

    if (pathJson !== newJsonPath) {
      // Path changed - delete old, write new
      operations.push({ type: 'delete', path: pathJson });
      operations.push({ type: 'delete', path: oldYmlPath });
      operations.push({ type: 'write', text: newJson, path: newJsonPath });
      operations.push({ type: 'write', text: newYml, path: newYmlPath });
    } else {
      // Same path - only write if content changed
      if (newYml !== oldYml) {
        operations.push({ type: 'write', text: newYml, path: newYmlPath });
      }
    }

    return {
      sourceId: resource.id,
      sourceName,
      yml: newYml,
      updatedProject,
      operations,
      skipped: false,
      cacheInfo: {
        pathJson,
        jsonContent,
        sql: '', // Sources don't have SQL
        yml: newYml,
        isSource: true,
      },
    };
  }

  /**
   * Process a source from raw file content (for new sources not in manifest).
   *
   * @param params - Processing parameters
   * @param params.pathJson - Path to the source.json file
   * @param params.project - Current dbt project state
   * @returns Processing result
   */
  async processFromFile(params: {
    pathJson: string;
    project: DbtProject;
  }): Promise<SourceProcessingResult> {
    const { pathJson, project } = params;

    // Read file content
    const file = await vscode.workspace.fs.readFile(vscode.Uri.file(pathJson));
    const jsonContent = file.toString();

    // Create a minimal resource
    const sourceJson = jsonParse(jsonContent) as FrameworkSource;
    const sourceName = frameworkMakeSourceName(sourceJson);

    const resource: SyncResource = {
      id: `source.${project.name}.${sourceName}`,
      pathJson,
      pathResource: pathJson.replace(/\.source\.json$/, '.yml'),
      type: 'source',
      parentIds: [],
      depth: 0,
    };

    return this.process({ resource, jsonContent, project });
  }
}
