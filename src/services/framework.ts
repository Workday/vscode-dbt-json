import { Coder } from '@services/coder';
import {
  getDbtProjectExcludePaths,
  getDjConfig,
  updateVSCodeJsonSchemas,
} from '@services/config';
import { Dbt } from '@services/dbt';
import { assertExhaustive, jsonParse } from '@shared';
import { ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import { CoderFileInfo } from '@shared/coder/types';
import { COMMAND_ID } from '@shared/constants';
import {
  DbtModel,
  DbtProject,
  DbtProjectManifestNode,
  DbtProjectManifestSource,
  DbtResourceType,
  DbtSeed,
  DbtSource,
} from '@shared/dbt/types';
import {
  FrameworkColumn,
  FrameworkDataType,
  FrameworkEtlSource,
  FrameworkModel,
  FrameworkSource,
  FrameworkSyncOp,
  FrameworkSyncPayload,
} from '@shared/framework/types';
import {
  frameworkGenerateModelOutput,
  frameworkGenerateSourceOutput,
  frameworkGetMacro,
  frameworkGetModelId,
  frameworkGetModelName,
  frameworkGetModelPrefix,
  frameworkGetNode,
  frameworkGetPathJson,
  frameworkGetSourceIds,
  frameworkMakeModelTemplate,
  frameworkMakeSourceId,
  frameworkMakeSourceName,
  frameworkMakeSourcePrefix,
} from '@shared/framework/utils';
import { BASE_SCHEMAS_PATH, DJ_SCHEMAS_PATH, TreeItem } from 'admin';
import { Ajv, ValidateFunction } from 'ajv';
import * as fs from 'fs';
import * as _ from 'lodash';
import * as path from 'path';
import * as vscode from 'vscode';
import { DJService } from './types';
import { dbtSourcePropertiesString } from '@shared/dbt/utils';

export class Framework implements DJService {
  ajv: Ajv;
  coder: Coder;
  diagnosticModelJson: vscode.DiagnosticCollection;
  diagnosticSourceJson: vscode.DiagnosticCollection;
  dbt: Dbt;
  etlSources = new Map<string, FrameworkEtlSource>();
  groupNames = new Set<string>();
  handleApi: (payload: ApiPayload<'framework'>) => Promise<ApiResponse>;
  intervalPendingSyncs: NodeJS.Timeout;
  isSyncing: () => boolean;
  isSyncingAll: () => boolean;
  modelTree = new Map<
    string,
    {
      name: string;
      children: string[];
      parents: string[];
      uris: {
        json: vscode.Uri | null;
        sql: vscode.Uri | null;
        yml: vscode.Uri | null;
      };
    }
  >();
  modelTreeRoots = new Map<string, TreeItem>();
  shouldSkipSync: (payload: { id: string; pathJson: string }) => boolean;
  sourceColumns = new Map<string, string[]>();
  sourceRefs = new Set<string>();
  syncsPending: { [timestamp: string]: FrameworkSyncPayload } = {};
  syncsRunning: { [timestamp: string]: FrameworkSyncPayload } = {};
  validateModelJson: ValidateFunction | undefined;
  validateSourceJson: ValidateFunction | undefined;
  webviewPanelQueryView: vscode.WebviewPanel | undefined;

  constructor({ coder }: { coder: Coder }) {
    this.ajv = new Ajv({
      allErrors: false,
      logger: coder.log,
      strictSchema: 'log',
    });
    this.coder = coder;
    this.dbt = new Dbt({
      coder: this.coder,
      framework: this,
    });
    this.diagnosticModelJson =
      vscode.languages.createDiagnosticCollection('modelJson');
    this.diagnosticSourceJson =
      vscode.languages.createDiagnosticCollection('sourceJson');
    this.handleApi = async (payload) => {
      switch (payload.type) {
        case 'framework-model-create': {
          const modelJson = frameworkMakeModelTemplate(payload.request);
          const { projectName } = payload.request;
          const project = this.dbt.projects.get(projectName);
          if (!project) {
            throw new Error('Project not found');
          }
          const modelPrefix = frameworkGetModelPrefix({
            modelJson,
            project,
          });
          const modelUri = vscode.Uri.file(`${modelPrefix}.model.json`);
          if (fs.existsSync(modelUri.fsPath)) {
            const modelName = frameworkGetModelName(modelJson);
            throw new Error(
              `Model ${modelName} already exists, please choose a different name or topic`,
            );
          }
          await vscode.workspace.fs.writeFile(
            modelUri,
            Buffer.from(JSON.stringify(modelJson, null, '    ')),
          );

          try {
            await this.coder.api.handleApi({
              type: 'state-clear',
              request: { formType: 'model-create' },
            });
          } catch (error) {
            this.coder.log.warn(
              'Failed to clear model create form state:',
              error,
            );
          }

          this.dbt.disposeWebviewPanelModelCreate();

          vscode.window.showTextDocument(modelUri);
          return apiResponse<typeof payload.type>('Model created');
        }
        case 'framework-source-create': {
          const { projectName, trinoCatalog, trinoSchema, trinoTable } =
            payload.request;
          const project = this.dbt.projects.get(projectName);
          if (!project) {
            throw new Error('Project not found');
          }

          const trinoColumns = await this.coder.api.handleApi({
            type: 'trino-fetch-columns',
            request: {
              catalog: trinoCatalog,
              schema: trinoSchema,
              table: trinoTable,
            },
          });
          const sourcePrefix = frameworkMakeSourcePrefix({
            database: trinoCatalog,
            project,
            schema: trinoSchema,
          });
          let newSourceJson: FrameworkSource = {
            database: trinoCatalog,
            schema: trinoSchema,
            tables: [],
          };
          const sourceJsonUri = vscode.Uri.file(`${sourcePrefix}.source.json`);
          let tables: FrameworkSource['tables'] = [];
          try {
            const existingSourceJson: FrameworkSource = jsonParse(
              (await vscode.workspace.fs.readFile(sourceJsonUri)).toString(),
            );
            if (existingSourceJson.tables) {
              // Keep properties on existing source json
              newSourceJson = existingSourceJson;
              tables = existingSourceJson.tables;
            }
          } catch {}
          if (tables.find((t) => t.name === trinoTable)) {
            // We're throwing this outside the try/catch to avoid catching the error
            throw new Error('Source table already exists');
          }
          tables.push({
            name: trinoTable,
            columns: trinoColumns.map((c) => ({
              name: c.column,
              data_type: c.type as FrameworkDataType,
              description: c.comment,
            })),
          });
          _.sortBy(tables, ['name']);
          await vscode.workspace.fs.writeFile(
            sourceJsonUri,
            Buffer.from(
              JSON.stringify({ ...newSourceJson, tables }, null, '    '),
            ),
          );

          try {
            await this.coder.api.handleApi({
              type: 'state-clear',
              request: { formType: 'source-create' },
            });
          } catch (error) {
            this.coder.log.warn(
              'Failed to clear source create form state:',
              error,
            );
          }

          this.dbt.disposeWebviewPanelSourceCreate();

          vscode.window.showTextDocument(sourceJsonUri);

          return apiResponse<typeof payload.type>('Source created');
        }
        default:
          return assertExhaustive<ApiResponse>(payload);
      }
    };
    this.intervalPendingSyncs = setInterval(() => {
      if (_.some(this.syncsPending, (s) => !s.roots)) {
        // If we have a full sync pending, run that one and clear the others
        vscode.commands.executeCommand(COMMAND_ID.JSON_SYNC);
        this.syncsPending = {};
      } else if (
        Object.keys(this.syncsPending).length > 0 &&
        !this.isSyncing()
      ) {
        const timestamps: string[] = [];
        const roots: { id: string; pathJson: string }[] = [];
        for (const [timestamp, sync] of Object.entries(this.syncsPending)) {
          if (sync.roots) {
            timestamps.push(timestamp);
            for (const root of sync.roots) {
              if (!roots.find((r) => r.id === root.id)) {
                roots.push(root);
              }
            }
          }
        }
        const timestamp = new Date().toISOString();
        this.syncsRunning[timestamp] = { timestamp };
        this.handleJsonSync({ timestamp, roots })
          .catch((err) => {
            this.coder.log.error('Error syncing files', err);
          })
          .then(() => {
            // Remove the syncs that were just completed
            for (const timestamp of timestamps) {
              delete this.syncsPending[timestamp];
            }
            // Remove the sync that was just started
            delete this.syncsRunning[timestamp];
          });
      }
    }, 10000);
    this.isSyncing = () => Object.keys(this.syncsRunning).length > 0;
    this.isSyncingAll = () => {
      for (const sync of Object.values(this.syncsRunning)) {
        if (sync.timestamp && !sync.roots) return true;
      }
      return false;
    };
    this.shouldSkipSync = ({ id, pathJson }) => {
      if (this.coder.gitPending) {
        if (!_.some(this.syncsPending, (s) => !s.roots)) {
          // If git operating is pending, don't sync now, but request a full sync
          const timestamp = new Date().toISOString();
          this.syncsPending[timestamp] = { timestamp };
        }
        return true;
      }
      if (this.isSyncing()) {
        // If we are already syncing, don't sync again
        if (
          !_.some(
            this.syncsPending,
            (s) => !s.roots || _.some(s.roots, (r) => r.pathJson === pathJson),
          )
        ) {
          // If this file isn't already in the pending syncs, add it
          const timestamp = new Date().toISOString();
          this.syncsPending[timestamp] = {
            roots: [{ id, pathJson }],
            timestamp,
          };
        }
        return true;
      }
      return false;
    };
  }

  async activate(context: vscode.ExtensionContext) {
    await vscode.workspace.fs.delete(vscode.Uri.file(DJ_SCHEMAS_PATH), {
      recursive: true,
    });

    const loadSchemasFiles = new Promise<void>((resolve, reject) => {
      fs.readdir(BASE_SCHEMAS_PATH, async (err, files) => {
        if (err) {
          reject(err);
        } else if (files) {
          try {
            // Write base schemas to local workspace
            for (const file of files) {
              const baseSchema = fs.readFileSync(
                path.join(BASE_SCHEMAS_PATH, file),
              );
              await vscode.workspace.fs.writeFile(
                vscode.Uri.file(path.join(DJ_SCHEMAS_PATH, file)),
                Buffer.from(baseSchema.toString()),
              );
              this.ajv.addSchema(
                require(path.join(DJ_SCHEMAS_PATH, file)),
                file,
              );
            }
            resolve();
          } catch (err) {
            reject(err);
          }
        }
      });
    });

    try {
      await loadSchemasFiles;
    } catch (err) {
      this.coder.log.error('Error loading schema files', err);
    }

    // Load main schemas to ajv
    try {
      this.validateModelJson = this.ajv.getSchema('model.schema.json');
    } catch (err) {
      this.coder.log.error('Error loading model.json schema', err);
    }

    try {
      this.validateSourceJson = this.ajv.getSchema('source.schema.json');
    } catch (err) {
      this.coder.log.error('Error loading source.json schema', err);
    }

    // Setting json schemas locally because we can't specify workspace paths from the extension
    this.coder.log.info('Updating Schemas');
    updateVSCodeJsonSchemas([
      {
        fileMatch: ['*.model.json'],
        url: '.dj/schemas/model.schema.json',
      },
      {
        fileMatch: ['*.source.json'],
        url: '.dj/schemas/source.schema.json',
      },
    ]);

    this.registerCommands(context);
    this.registerProviders(context);
    this.registerEventHandlers(context);
  }

  getManifestResources(project: DbtProject): {
    models: Record<string, DbtModel>;
    seeds: Record<string, DbtSeed>;
    sources: Record<string, DbtSource>;
  } {
    const { manifest } = project;
    const resourcesByType: {
      models: Record<string, DbtModel>;
      seeds: Record<string, DbtSeed>;
      sources: Record<string, DbtSource>;
    } = { models: {}, seeds: {}, sources: {} };
    for (const [key, node] of Object.entries({
      ...manifest.nodes,
      ...manifest.sources,
    })) {
      const pathRelativeFile = node?.original_file_path || '';
      const pathRelativeDirectory = path.dirname(pathRelativeFile);
      const pathSystemDirectory = path.join(
        project.pathSystem,
        pathRelativeDirectory,
      );
      const pathSystemFile = path.join(project.pathSystem, pathRelativeFile);
      const frameworkValues = {
        childMap: manifest.child_map[key] || [],
        parentMap: manifest.parent_map[key] || [],
        pathRelativeDirectory,
        pathSystemDirectory,
        pathSystemFile,
      };

      switch (node?.resource_type) {
        case 'model':
          resourcesByType.models[key] = {
            ...(node as DbtProjectManifestNode),
            ...frameworkValues,
          };
          break;
        case 'seed':
          resourcesByType.seeds[key] = {
            ...(node as DbtProjectManifestNode),
            ...frameworkValues,
          };
          break;
        case 'source':
          resourcesByType.sources[key] = {
            ...(node as DbtProjectManifestSource),
            ...frameworkValues,
          };
          break;
      }
    }
    return resourcesByType;
  }

  getOrderedResources({
    project,
    rootIds,
  }: {
    project: DbtProject;
    rootIds?: string[];
  }): {
    id: string;
    pathJson: string;
    pathResource: string;
    type: DbtResourceType;
  }[] {
    const orderedResources: {
      id: string;
      pathJson: string;
      pathResource: string;
      type: DbtResourceType;
    }[] = [];
    let waitingIds: string[] = [];
    rootIds = (rootIds || []) as string[];

    const { models, seeds, sources } = this.getManifestResources(project);

    if (!rootIds.length) {
      // If no roots provided, start with seeds and sources
      for (const [seedId, seed] of Object.entries(seeds)) {
        const pathResource = seed.pathSystemFile;
        // TODO: Implement a seed.json schema
        const pathJson = pathResource.replace(/\.csv$/, '.seed.json');
        rootIds.push(seedId);
        orderedResources.push({
          id: seedId,
          pathJson,
          pathResource,
          type: 'seed',
        });
      }
      for (const [sourceId, source] of Object.entries(sources)) {
        const pathResource = source.pathSystemFile;
        const pathJson = pathResource.replace(
          /(?:\.yml|\.yaml)$/,
          '.source.json',
        );
        rootIds.push(sourceId);
        orderedResources.push({
          id: sourceId,
          pathJson,
          pathResource,
          type: 'source',
        });
      }
    }

    // Sorting roots alphabetically
    rootIds.sort();
    orderedResources.sort((a, b) => (a.id > b.id ? 1 : -1));

    function getResource(resourceId: string) {
      let resource: DbtModel | DbtSeed | DbtSource | undefined;
      const resourceType = getResourceType(resourceId);
      switch (resourceType) {
        case 'model': {
          resource = models[resourceId];
          break;
        }
        case 'seed': {
          resource = seeds[resourceId];
          break;
        }
        case 'source': {
          resource = sources[resourceId];
          break;
        }
      }
      return resource;
    }

    function getResourceType(resourceId: string): DbtResourceType {
      return resourceId.split('.')[0] as DbtResourceType;
    }

    // Add to the ordered models
    function addResource(resourceId: string, skipWaiting = false): boolean {
      const alreadyAdded = !!orderedResources.find((m) => m.id === resourceId);
      // Don't add the same resource twice
      if (alreadyAdded) return true;

      const resource = getResource(resourceId);
      if (!resource) return false;

      // Wait until all parents of this resource have been added
      const ready =
        !resource.parentMap.length ||
        resource.parentMap.every(
          (id) => !!orderedResources.find((m) => m.id === id),
        );

      if (ready || skipWaiting) {
        const resourceType = getResourceType(resourceId);
        const pathResource = resource.pathSystemFile;
        const pathJson = frameworkGetPathJson({
          pathResource,
          type: resourceType,
        });
        orderedResources.push({
          id: resourceId,
          pathJson,
          pathResource,
          type: resourceType,
        });
        const waitingIndex = waitingIds.indexOf(resourceId);
        if (waitingIndex >= 0) {
          // Remove from waiting list if it was there before
          waitingIds = [
            ...waitingIds.slice(0, waitingIndex),
            ...waitingIds.slice(waitingIndex + 1),
          ];
        }
        return true;
      } else {
        if (!waitingIds.includes(resourceId)) {
          // Add to the waiting list if it wasn't there before
          waitingIds = [...waitingIds, resourceId];
        }
        return false;
      }
    }

    // Recursively traverse through the resource children
    function addChildren(resourceIds: string[], skipWaiting = false) {
      for (const resourceId of resourceIds) {
        const proceed = addResource(resourceId, skipWaiting);
        if (!proceed) continue;
        const resource = getResource(resourceId);
        if (!resource) continue;
        // We only process children for that are models
        const resourceChildren = resource.childMap
          .filter((id) => getResourceType(id) === 'model')
          .sort();
        addChildren(resourceChildren, skipWaiting);
      }
    }

    // Start with the root models
    addChildren(rootIds);

    // If we have waiting models that haven't been added yet, because a parent wasn't in the root chain, add them now without waiting on all parents
    if (waitingIds.length) {
      addChildren(waitingIds, true);
    }

    return orderedResources;
  }

  async fetchModelJson(uri: vscode.Uri): Promise<FrameworkModel | null> {
    try {
      const match = /((?:[0-9]|[A-z]|-|_)+)\.model\.json$/.exec(uri.fsPath);
      if (!match) return null;
      return jsonParse(
        (await vscode.workspace.fs.readFile(uri)).toString(),
      ) as FrameworkModel;
    } catch {
      return null;
    }
  }

  async fetchSourceJson(uri: vscode.Uri): Promise<FrameworkSource | null> {
    try {
      const match = /((?:[0-9]|[A-z]|-|_)+)\.source\.json$/.exec(uri.fsPath);
      if (!match) return null;
      return jsonParse(
        (await vscode.workspace.fs.readFile(uri)).toString(),
      ) as FrameworkSource;
    } catch {
      return null;
    }
  }

  async handleGenerateModelFiles(info: CoderFileInfo): Promise<void> {
    if (info?.type !== 'framework-model') return;
    const { filePath, modelJson, project } = info;
    const uri = vscode.Uri.file(filePath);

    if (!this.validateModelJson) {
      vscode.window.showErrorMessage('Model JSON Validation Not Active');
      return;
    }

    this.validateModelJson?.(modelJson); // Will return false if invalid
    const errors = this.validateModelJson?.errors;
    if (errors) {
      this.coder.log.error(this.validateModelJson?.errors);
      const message = errors.map((e) => e?.message || '').join('\n');
      this.diagnosticModelJson.set(uri, [
        new vscode.Diagnostic(new vscode.Range(0, 0, 0, 0), message),
      ]);
      vscode.window.showErrorMessage('Model JSON Invalid');
      this.coder.log.error('MODEL JSON INVALID', message);
      this.coder.log.show(true);
      return;
    }

    // No validation errors, so we'll clear any previous diagnostics on this file
    this.diagnosticModelJson.delete(uri);

    const modelId = frameworkGetModelId({ modelJson, project });
    if (!modelId) {
      vscode.window.showErrorMessage('Model Not Found');
      this.coder.log.error('MODEL NOT FOUND:', modelJson.name);
      this.coder.log.show(true);
      return;
    }

    try {
      const roots = [{ id: modelId, pathJson: uri.fsPath }];
      const timestamp = new Date().toISOString();
      this.syncsRunning[timestamp] = { roots, timestamp };
      await this.handleJsonSync({ roots, timestamp });
    } catch (err) {
      vscode.window.showErrorMessage('Error syncing files');
      this.coder.log.error('ERROR SYNCING FILES', err);
      this.coder.log.show(true);
    }
  }

  async handleGenerateSourceFiles(info: CoderFileInfo): Promise<void> {
    if (info?.type !== 'framework-source') return;
    const { filePath, project, sourceJson } = info;
    const uri = vscode.Uri.file(filePath);

    if (!this.validateSourceJson) {
      vscode.window.showErrorMessage('Source JSON Validation Not Active');
      return;
    }

    this.validateSourceJson?.(sourceJson); // Will return false if invalid
    const errors = this.validateSourceJson?.errors;
    if (errors) {
      this.coder.log.error(this.validateSourceJson?.errors);
      const message = errors.map((e) => e?.message || '').join('\n');
      this.diagnosticSourceJson.set(uri, [
        new vscode.Diagnostic(new vscode.Range(0, 0, 0, 0), message),
      ]);
      vscode.window.showErrorMessage('Source JSON Invalid');
      this.coder.log.error('SOURCE JSON INVALID', message);
      this.coder.log.show(true);
      return;
    }

    // No validation errors, so we'll clear any previous diagnostics on this file
    this.diagnosticSourceJson.delete(uri);

    const sourceIds = frameworkGetSourceIds({ project, sourceJson });
    if (!sourceIds?.length) {
      vscode.window.showWarningMessage('Source has no tables');
      this.coder.log.error('NO SOURCE TABLES');
      return;
    }

    try {
      const roots = sourceIds.map((id) => ({ id, pathJson: uri.fsPath }));
      const timestamp = new Date().toISOString();
      this.syncsRunning[timestamp], { roots, timestamp };
      await this.handleJsonSync({ roots, timestamp });
    } catch (err) {
      vscode.window.showErrorMessage('Error syncing files');
      this.coder.log.error('ERROR SYNCING FILES', err);
      this.coder.log.show(true);
    }
  }

  async handleJsonSync({
    roots,
    timestamp,
  }: {
    roots?: {
      id: string;
      pathJson?: string;
    }[];
    timestamp: string;
  }) {
    this.coder.log.info('--- Starting sync ----');
    const syncs: {
      name: string;
      ops: FrameworkSyncOp[];
    }[] = [];

    vscode.window.withProgress(
      {
        title: 'DJ Sync',
        location: vscode.ProgressLocation.Notification,
        cancellable: false,
      },
      async (progress, token) => {
        try {
          let progressMessage = '';
          let progressTotal = 0;

          progress.report({ increment: 0, message: progressMessage });

          const jsonUris = await vscode.workspace.findFiles(
            '**/*.{model,source}.json',
            getDbtProjectExcludePaths(),
          );
          // TODO: Determine a better way to estimate sync
          const estimatedSeconds = !roots ? jsonUris.length * 0.2 : 5;

          const progressInterval = setInterval(() => {
            const progressIncrement = 100 / estimatedSeconds;
            if (progressTotal <= 95) {
              progress.report({
                increment: progressIncrement,
                message: progressMessage,
              });
              progressTotal += progressIncrement;
            }
          }, 1000);

          const _projects = this.dbt.projects.values();
          for (const _project of _projects) {
            let manifest = await this.dbt.fetchManifest({ project: _project });

            const manifestGeneratedAt = manifest?.metadata.generated_at
              ? new Date(manifest.metadata.generated_at)
              : null;
            // If there were no file after the last manifest generation, and it has been at least 30 seconds since the last file change, we'll consider it current
            // When dbt is upgraded, we can replace this with a comparison to invocation_started_at
            const manifestCurrent = !!(
              manifestGeneratedAt &&
              (!this.coder.lastFileChange ||
                (manifestGeneratedAt.getTime() >
                  this.coder.lastFileChange.getTime() &&
                  new Date().getTime() - this.coder.lastFileChange.getTime() >
                    30000))
            );

            const shouldParse =
              !manifest || (!roots?.length && !manifestCurrent);

            if (shouldParse) {
              progressMessage = `Parsing ${_project.name} manifest`;
              manifest = await this.coder.api.handleApi({
                type: 'dbt-parse-project',
                request: {
                  logger: this.coder.log,
                  project: _project,
                },
              });
            } else {
              progressMessage = `Using existing ${_project.name} manifest`;
            }

            let project = { ..._project, ...(manifest && { manifest }) };

            const orderedResources = this.getOrderedResources({
              project,
              rootIds: roots?.map((r) => r.id),
            });

            const modelRenames: { old: string; new: string; path: string }[] =
              [];
            if (roots?.length) {
              for (const root of roots) {
                if (!root.id?.startsWith('model.') || !root.pathJson) continue;
                const modelIdName = root.id.split('.')[2];
                const modelPathName = path
                  .basename(root.pathJson)
                  .replace(/\.model\.json$/, '');
                if (
                  modelIdName &&
                  modelPathName &&
                  modelIdName !== modelPathName
                ) {
                  const existingModelNode = frameworkGetNode({
                    project,
                    model: modelIdName,
                  });
                  this.coder.log.info(
                    'HAS EXISTING MODEL NODE:',
                    !!existingModelNode,
                  );
                  if (existingModelNode) {
                    // If the model already exists, throw an error
                    const errorMessage = `A model named '${modelIdName}' already exists, please update name inputs.`;
                    const modelPathUri = vscode.Uri.file(root.pathJson);
                    this.diagnosticModelJson.set(modelPathUri, [
                      new vscode.Diagnostic(
                        new vscode.Range(0, 0, 0, 0),
                        errorMessage,
                        vscode.DiagnosticSeverity.Error,
                      ),
                    ]);
                    vscode.window.showErrorMessage(errorMessage);
                    this.coder.log.error(errorMessage);
                    delete this.syncsRunning[timestamp];
                    return;
                  }
                  // If the model ID name and path name don't match, we need to rename the model
                  modelRenames.push({
                    new: modelIdName,
                    old: modelPathName,
                    path: root.pathJson,
                  });
                }
              }
            }

            // These roots may have just been added
            if (!orderedResources.length) {
              if (roots) {
                for (const root of roots) {
                  const rootParts = root.id.split('.');
                  const [resourceType, projectName] = rootParts;
                  if (project.name === projectName) {
                    switch (resourceType) {
                      case 'model': {
                        const pathJson = root.pathJson;
                        if (pathJson) {
                          const modelUriNew = vscode.Uri.file(pathJson);
                          const modelFileNew = (
                            await vscode.workspace.fs.readFile(modelUriNew)
                          ).toString();
                          const modelJsonNew = jsonParse(
                            modelFileNew,
                          ) as FrameworkModel;

                          const modelNameNew =
                            frameworkGetModelName(modelJsonNew);

                          orderedResources.push({
                            id: root.id,
                            pathJson,
                            pathResource: pathJson.replace(
                              /\.model\.json$/,
                              '.sql',
                            ),
                            type: 'model',
                          });
                        }
                        break;
                      }
                      case 'source': {
                        const pathJson = root.pathJson;
                        if (pathJson) {
                          orderedResources.push({
                            id: root.id,
                            pathJson,
                            pathResource: pathJson.replace(
                              /\.source\.json$/,
                              '.yml',
                            ),
                            type: 'source',
                          });
                        }
                        break;
                      }
                    }
                  }
                }
              }
            }

            progressMessage = `Checking ${orderedResources.length} resources`;

            for (const resource of orderedResources) {
              try {
                const ops: FrameworkSyncOp[] = [];
                const oldJsonPath = resource.pathJson;
                const oldJsonUri = jsonUris.find(
                  (u) => u.fsPath === oldJsonPath,
                );
                if (!oldJsonUri) continue;

                switch (resource.type) {
                  case 'model': {
                    const oldPrefix = oldJsonPath.replace(/\.model\.json$/, '');
                    const file = await vscode.workspace.fs.readFile(
                      vscode.Uri.file(oldJsonPath),
                    );
                    const modelJson = jsonParse(
                      file.toString(),
                    ) as FrameworkModel;

                    const newPrefix = frameworkGetModelPrefix({
                      project,
                      modelJson,
                    });
                    if (!newPrefix) break;

                    const newJson = JSON.stringify(modelJson, null, '    ');
                    const newJsonPath = `${newPrefix}.model.json`;
                    const newJsonUri = vscode.Uri.file(newJsonPath);
                    const newSqlPath = `${newPrefix}.sql`;
                    const newYmlPath = `${newPrefix}.yml`;

                    let oldSql = '';
                    const oldSqlPath = `${oldPrefix}.sql`;
                    try {
                      oldSql = fs.readFileSync(oldSqlPath, 'utf8');
                    } catch {}

                    let oldYml = '';
                    const oldYmlPath = `${oldPrefix}.yml`;
                    try {
                      oldYml = fs.readFileSync(oldYmlPath, 'utf8');
                    } catch {}

                    let newSql = '';
                    let newYml = '';
                    try {
                      const generated = frameworkGenerateModelOutput({
                        project,
                        modelJson,
                      });
                      project = generated.project;
                      newSql = generated.sql;
                      newYml = generated.yml;
                      this.diagnosticModelJson.delete(newJsonUri);
                    } catch (err: any) {
                      const message = `Invalid model sql detected, please double check any "expr" \n\n${err?.message}`;
                      this.diagnosticModelJson.set(newJsonUri, [
                        new vscode.Diagnostic(
                          new vscode.Range(0, 0, newJson.split('\n').length, 0),
                          message,
                        ),
                      ]);
                      vscode.window.showErrorMessage(message);
                      break;
                    }
                    if (!newSql || !newYml) break;

                    if (oldJsonPath && newJsonPath !== oldJsonPath) {
                      ops.push({ type: 'delete', path: oldJsonPath });
                      ops.push({ type: 'delete', path: oldSqlPath });
                      ops.push({ type: 'delete', path: oldYmlPath });
                      ops.push({
                        type: 'write',
                        text: newJson,
                        path: newJsonPath,
                      });
                      ops.push({
                        type: 'write',
                        text: newSql,
                        path: newSqlPath,
                      });
                      ops.push({
                        type: 'write',
                        text: newYml,
                        path: newYmlPath,
                      });
                    } else {
                      if (newSql !== oldSql) {
                        ops.push({
                          type: 'write',
                          text: newSql,
                          path: newSqlPath,
                        });
                      }
                      if (newYml !== oldYml) {
                        ops.push({
                          type: 'write',
                          text: newYml,
                          path: newYmlPath,
                        });
                      }
                    }
                    if (ops.length) {
                      syncs.push({
                        name: frameworkGetModelName(modelJson),
                        ops,
                      });
                    }

                    break;
                  }
                  case 'source': {
                    const oldPrefix = oldJsonPath.replace(
                      /\.source\.json$/,
                      '',
                    );
                    const file = await vscode.workspace.fs.readFile(
                      vscode.Uri.file(oldJsonPath),
                    );
                    const sourceJson = jsonParse(
                      file.toString(),
                    ) as FrameworkSource;

                    const newPrefix = frameworkMakeSourcePrefix({
                      ...sourceJson,
                      project,
                    });
                    if (!newPrefix) break;

                    const newJson = JSON.stringify(sourceJson, null, '    ');
                    const newJsonPath = `${newPrefix}.source.json`;
                    const newYmlPath = `${newPrefix}.yml`;

                    let oldYml = '';
                    const oldYmlPath = `${oldPrefix}.yml`;
                    try {
                      oldYml = fs.readFileSync(oldYmlPath, 'utf8');
                    } catch {}

                    let newYml = '';
                    try {
                      const generated = frameworkGenerateSourceOutput({
                        project,
                        sourceJson,
                      });
                      project = generated.project;
                      newYml = generated.yml;
                    } catch (err) {
                      break;
                    }
                    if (!newYml) break;

                    if (oldJsonPath && newJsonPath !== oldJsonPath) {
                      ops.push({ type: 'delete', path: oldJsonPath });
                      ops.push({ type: 'delete', path: oldYmlPath });
                      ops.push({
                        type: 'write',
                        text: newJson,
                        path: newJsonPath,
                      });
                      ops.push({
                        type: 'write',
                        text: newYml,
                        path: newYmlPath,
                      });
                    } else {
                      if (newYml !== oldYml) {
                        ops.push({
                          type: 'write',
                          text: newYml,
                          path: newYmlPath,
                        });
                      }
                    }
                    if (ops.length) {
                      syncs.push({
                        name: frameworkMakeSourceName(sourceJson),
                        ops,
                      });
                    }

                    break;
                  }
                }
              } catch {}
            }
            this.coder.log.info('--- Checked all resources ----');

            // Handle model renames
            if (modelRenames.length) {
              this.coder.log.info('--- Renaming model references ----');
              progressMessage = `Renaming model references`;

              for (const uriJson of jsonUris) {
                const pathJson = uriJson.fsPath;
                if (/\.model\.json$/.test(pathJson)) {
                  const modelJsonFileOld =
                    await vscode.workspace.fs.readFile(uriJson);
                  let modelJsonString = modelJsonFileOld.toString();
                  for (const modelRename of modelRenames) {
                    if (pathJson === modelRename.path) continue; // Skip the file we're renaming
                    const replacements = [
                      {
                        regex: new RegExp(`"model": "${modelRename.old}"`, 'g'),
                        string: `"model": "${modelRename.new}"`,
                      },
                      {
                        regex: new RegExp(`${modelRename.old}\\.`, 'g'),
                        string: `${modelRename.new}.`,
                      },
                    ];
                    const shouldReplace = replacements.some((r) =>
                      r.regex.test(modelJsonString),
                    );
                    if (shouldReplace) {
                      for (const replacement of replacements) {
                        modelJsonString = modelJsonString.replaceAll(
                          replacement.regex,
                          replacement.string,
                        );
                      }
                      const modelJsonNew = jsonParse(
                        modelJsonString,
                      ) as FrameworkModel;
                      const generated = frameworkGenerateModelOutput({
                        project,
                        modelJson: modelJsonNew,
                      });
                      const modelSqlStringNew = generated.sql;
                      const pathSql = pathJson.replace(
                        /\.model\.json$/,
                        '.sql',
                      );
                      // Write both the new json and sql files
                      syncs.push({
                        name: modelRename.new,
                        ops: [
                          {
                            type: 'write',
                            path: pathJson,
                            text: modelJsonString,
                          },
                          {
                            type: 'write',
                            path: pathSql,
                            text: modelSqlStringNew,
                          },
                        ],
                      });
                    }
                  }
                }
              }
            }

            if (syncs.length) {
              this.coder.log.info(`--- Writing ${syncs.length} updates ----`);
              progressMessage = `Writing ${syncs.length} updates`;
              let count = 0;
              for (const sync of syncs) {
                for (const op of sync.ops) {
                  switch (op.type) {
                    case 'delete': {
                      await vscode.workspace.fs.delete(
                        vscode.Uri.file(op.path),
                      );
                      break;
                    }
                    case 'write': {
                      await vscode.workspace.fs.writeFile(
                        vscode.Uri.file(op.path),
                        Buffer.from(op.text),
                      );
                      break;
                    }
                  }
                }
                count++;
              }
              this.coder.log.info(`--- Wrote all updates ----`);

              if (modelRenames.length) {
                // Await the new manifest after renames
                progressMessage = `Reparsing ${project.name} manifest`;
                this.coder.log.info(
                  `--- Reparsing ${project.name} manifest after model renames ----`,
                );
                try {
                  const manifest = await this.coder.api.handleApi({
                    type: 'dbt-parse-project',
                    request: { project },
                  });
                  this.dbt.handleManifest({ manifest, project });
                } catch (err) {
                  this.coder.log.error('Error syncing manifest', err);
                }
              } else {
                // Otherwise just request a new one after
                this.coder.api
                  .handleApi({
                    type: 'dbt-parse-project',
                    request: { project },
                  })
                  .catch((err) =>
                    this.coder.log.error('Error syncing manifest', err),
                  );
              }
            }
          }

          this.coder.log.info('--- Sync complete ---');

          clearInterval(progressInterval);
        } catch (err) {
          this.coder.log.error('Error SYNCING JSON', err);
          this.coder.log.show(true);
        }
        delete this.syncsRunning[timestamp];
        // If this was a sync all, clear pending syncs
        if (!roots) {
          this.coder.log.info('Clearing pending syncs');
          this.syncsPending = {};
        }
        return;
      },
    );
  }

  async handleLoadEtlSources({ project }: { project: DbtProject }) {
    const { airflowGenerateDags } = getDjConfig();

    // Skip ETL sources loading if Airflow DAG generation is disabled
    if (!airflowGenerateDags) {
      this.coder.log.info(
        `Skipping ETL sources loading for project ${project.name} (airflowGenerateDags is disabled)`,
      );
      return;
    }

    try {
      const etlSources = await this.coder.api.handleApi({
        type: 'trino-fetch-etl-sources',
        request: { projectName: project.name },
      });
      for (const etlSource of etlSources) {
        this.etlSources.set(etlSource.source_id, etlSource);
      }
    } catch (error) {
      this.coder.log.warn(
        `Failed to load ETL sources for project ${project.name}:`,
        error,
      );
    }
  }

  /**
   * Register VS Code commands
   * @param context
   */
  registerCommands(context: vscode.ExtensionContext) {
    this.coder.log.info('Framework: Registering commands');
    this.registerNavigationCommands(context);
    this.registerFrameworkJumpCommands(context);
    this.registerUtilityCommands(context);
    this.coder.log.info('Framework: Commands registered successfully');
  }

  /**
   * Register navigation commands - Source Origin, Source Navigate, Model Navigate, Column Origin
   * @param context
   */
  registerNavigationCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(
        COMMAND_ID.SOURCE_ORIGIN,
        async ({
          filePath,
          projectName,
          tableName,
        }: {
          filePath: string;
          projectName: string;
          tableName: string;
        }) => {
          try {
            const project = this.coder.framework.dbt.projects.get(projectName);
            if (!project) throw new Error('No project found');

            const ext = path.extname(filePath);
            const pattern =
              ext === '.json' ? `"name": "${tableName}"` : `name: ${tableName}`;
            const fileUri = vscode.Uri.file(filePath);
            const editor = await vscode.window.showTextDocument(fileUri);
            const regex = new RegExp(pattern);
            const matches = regex.exec(editor.document.getText());
            if (!matches) return;

            const line = editor.document.lineAt(
              editor.document.positionAt(matches.index).line,
            );
            const indexOf = line.text.indexOf(matches[0]);
            const position = new vscode.Position(line.lineNumber, indexOf);
            const range = editor.document.getWordRangeAtPosition(
              position,
              new RegExp(regex),
            );
            if (range) {
              editor.revealRange(range);
              editor.selection = new vscode.Selection(range.start, range.end);
            }
          } catch (err) {
            this.coder.log.error('ERROR NAVIGATING TO SOURCE ORIGIN', err);
          }
        },
      ),

      vscode.commands.registerCommand(
        COMMAND_ID.SOURCE_NAVIGATE,
        async (source: DbtSource) => {
          try {
            const uriModelJson = vscode.Uri.file(
              source.pathSystemFile.replace('.yml', '.source.json'),
            );
            const uriModelSql = vscode.Uri.file(source.pathSystemFile);
            if (fs.existsSync(uriModelJson.fsPath)) {
              await vscode.window.showTextDocument(uriModelJson);
            } else {
              await vscode.window.showTextDocument(uriModelSql);
            }
          } catch (err) {
            this.coder.log.error('ERROR NAVIGATING TO SOURCE', err);
          }
        },
      ),

      vscode.commands.registerCommand(
        COMMAND_ID.MODEL_NAVIGATE,
        async (model: DbtModel) => {
          try {
            const uriModelJson = vscode.Uri.file(
              model.pathSystemFile.replace('.sql', '.model.json'),
            );
            const uriModelSql = vscode.Uri.file(model.pathSystemFile);
            if (fs.existsSync(uriModelJson.fsPath)) {
              await vscode.window.showTextDocument(uriModelJson);
            } else {
              await vscode.window.showTextDocument(uriModelSql);
            }
          } catch (err) {
            this.coder.log.error('ERROR NAVIGATING TO MODEL', err);
          }
        },
      ),

      vscode.commands.registerCommand(
        COMMAND_ID.COLUMN_ORIGIN,
        async (column: FrameworkColumn) => {
          try {
            const originId = column?.meta?.origin?.id;
            if (!originId) throw new Error('No id provided');

            const projectName = originId.split('.')[1];
            const project = this.coder.framework.dbt.projects.get(projectName);
            if (!project) throw new Error('No project found');

            const model = this.coder.framework.dbt.models.get(originId);
            if (!model) throw new Error('No model found');

            const uriModelJson = vscode.Uri.file(
              model.pathSystemFile.replace('.sql', '.model.json'),
            );
            const uriModelSql = vscode.Uri.file(model.pathSystemFile);
            if (fs.existsSync(uriModelJson.fsPath)) {
              const editor = await vscode.window.showTextDocument(uriModelJson);
              const regex = new RegExp(`"name": "${column.name}"`);
              const matches = regex.exec(editor.document.getText());
              if (!matches) return;
              const line = editor.document.lineAt(
                editor.document.positionAt(matches.index).line,
              );
              const indexOf = line.text.indexOf(matches[0]);
              const position = new vscode.Position(line.lineNumber, indexOf);
              const range = editor.document.getWordRangeAtPosition(
                position,
                new RegExp(regex),
              );
              if (range) {
                editor.revealRange(range);
                editor.selection = new vscode.Selection(range.start, range.end);
              }
            } else {
              await vscode.window.showTextDocument(uriModelSql);
            }
          } catch (err) {
            this.coder.log.error('ERROR NAVIGATING TO COLUMN ORIGIN', err);
          }
        },
      ),
    );
  }

  /**
   * Register VS Code commands - Framework Jump JSON, Framework Jump Model, Framework Jump YAML
   * @param context
   */
  registerFrameworkJumpCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(
        COMMAND_ID.FRAMEWORK_JUMP_JSON,
        async () => {
          try {
            const currentPath = this.coder.getCurrentPath();
            if (!currentPath) return;
            const info = await this.coder.fetchFileInfoFromPath(currentPath);
            switch (info?.type) {
              case 'model': {
                await this.coder.showOrOpenFile(
                  currentPath.replace(/\.sql$/, '.model.json'),
                  { viewColumn: vscode.ViewColumn.Beside },
                );
                break;
              }
              case 'yml': {
                if (info.properties?.sources) {
                  await this.coder.showOrOpenFile(
                    currentPath.replace(/\.yml$/, '.source.json'),
                    { viewColumn: vscode.ViewColumn.Beside },
                  );
                } else {
                  await this.coder.showOrOpenFile(
                    currentPath.replace(/\.yml$/, '.model.json'),
                    { viewColumn: vscode.ViewColumn.Beside },
                  );
                }
                break;
              }
            }
          } catch (err) {
            this.coder.log.error('ERROR JUMPING FRAMEWORK: ', err);
          }
        },
      ),

      vscode.commands.registerCommand(
        COMMAND_ID.FRAMEWORK_JUMP_MODEL,
        async () => {
          try {
            const currentPath = this.coder.getCurrentPath();
            if (!currentPath) return;
            await this.coder.showOrOpenFile(
              currentPath.replace(/\.(model\.json|yml)$/, '.sql'),
              { viewColumn: vscode.ViewColumn.Beside },
            );
          } catch (err) {
            this.coder.log.error('ERROR JUMPING FRAMEWORK: ', err);
          }
        },
      ),

      vscode.commands.registerCommand(
        COMMAND_ID.FRAMEWORK_JUMP_YAML,
        async () => {
          try {
            const currentPath = this.coder.getCurrentPath();
            if (!currentPath) return;
            await this.coder.showOrOpenFile(
              currentPath.replace(/\.(model\.json|source\.json|sql)$/, '.yml'),
              { viewColumn: vscode.ViewColumn.Beside },
            );
          } catch (err) {
            this.coder.log.error('ERROR JUMPING FRAMEWORK: ', err);
          }
        },
      ),
    );
  }

  /**
   * Register VS Code commands - DJ Sync
   */
  registerUtilityCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(COMMAND_ID.JSON_SYNC, async () => {
        this.coder.log.info('Starting new json sync');
        if (this.coder.framework.isSyncingAll()) {
          this.coder.log.info('Sync all already running');
          return;
        }
        const timestamp = new Date().toISOString();
        this.coder.framework.syncsRunning[timestamp] = { timestamp };
        try {
          await this.coder.framework.handleJsonSync({ timestamp });
        } catch (err) {
          this.coder.log.error('ERROR SYNCING MODEL JSON', err);
          this.coder.log.show(true);
        }
      }),
      vscode.commands.registerCommand(
        COMMAND_ID.SOURCE_REFRESH,
        ({ sourceId }: { sourceId: string }) => {
          vscode.window.withProgress(
            {
              title: 'DJ Loading',
              location: vscode.ProgressLocation.Notification,
              cancellable: false,
            },
            async (progress, token) => {
              try {
                progress.report({
                  increment: 20,
                  message: 'Checking source table columns',
                });

                const sourceJson = await this.coder.framework.fetchSourceJson(
                  vscode.Uri.file(
                    this.coder.getCurrentDocument()?.uri.fsPath || '',
                  ),
                );

                if (!sourceJson?.tables) return;

                const tableName = sourceId.split('.')[3];

                const trinoColumns = await this.coder.api.handleApi({
                  type: 'trino-fetch-columns',
                  request: {
                    catalog: sourceJson.database,
                    schema: sourceJson.schema,
                    table: tableName,
                  },
                });

                const updatedTables = sourceJson.tables.map((table) => {
                  if (table.name === tableName) {
                    return {
                      ...table,
                      columns: trinoColumns.map((value) => {
                        const existingColumn = table.columns.find(
                          (col) => col.name === value.column,
                        );
                        return {
                          name: value.column,
                          data_type: value.type as FrameworkDataType,
                          description:
                            existingColumn?.description || value.comment || '',
                        } as FrameworkColumn;
                      }),
                    };
                  }
                  _.sortBy(table, ['name']);
                  return table;
                });

                const updatedSourceJson = {
                  ...sourceJson,
                  tables: updatedTables,
                };

                progress.report({
                  increment: 40,
                  message: 'Writing updates',
                });

                await vscode.workspace.fs.writeFile(
                  vscode.Uri.file(
                    this.coder.getCurrentDocument()?.uri.fsPath || '',
                  ),
                  Buffer.from(JSON.stringify(updatedSourceJson, null, 4)),
                );

                progress.report({
                  increment: 40,
                  message: 'Finished updating',
                });
              } catch (error) {
                this.coder.log.error('ERROR Refreshing Source', error);
                vscode.window.showInformationMessage(
                  `Source ${sourceId} Failed to refresh source column.`,
                );
              }
            },
          );
        },
      ),
    );
  }

  /**
   * Register providers - Source Code Lens Provider, ModelDefinition Provider
   * @param context
   */
  registerProviders(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.languages.registerCodeLensProvider(
        { pattern: '**/*.source.json' },
        {
          provideCodeLenses: async (document, token) => {
            const project = this.coder.framework.dbt.getProjectFromPath(
              document.uri.fsPath,
            );
            if (!project) return [];

            const etlSources = this.coder.framework.etlSources;

            const documentText = document.getText();
            const sourceJson: FrameworkSource = jsonParse(documentText);
            const sourceDatabase = sourceJson.database;
            const sourceSchema = sourceJson.schema;

            const codeLenses: vscode.CodeLens[] = [];

            for (const sourceTable of sourceJson.tables || []) {
              const regex = new RegExp(`"name": "${sourceTable.name}"`);

              const matches = regex.exec(documentText);
              if (!matches) continue;
              const line = document.lineAt(
                document.positionAt(matches.index).line,
              );
              const indexOf = line.text.indexOf(matches[0]);
              const position = new vscode.Position(line.lineNumber, indexOf);
              const range = document.getWordRangeAtPosition(
                position,
                new RegExp(regex),
              );
              if (range) {
                const sourceId = frameworkMakeSourceId({
                  database: sourceDatabase,
                  schema: sourceSchema,
                  table: sourceTable.name,
                  project,
                });

                codeLenses.push(
                  new vscode.CodeLens(range, {
                    title: 'Refresh Source $(refresh)',
                    command: COMMAND_ID.SOURCE_REFRESH,
                    arguments: [{ sourceId }],
                  }),
                );

                const etlSource = etlSources.get(sourceId);
                if (etlSource) {
                  if (etlSource.etl_active) {
                    codeLenses.push(
                      new vscode.CodeLens(range, {
                        title: 'ETL Active $(pass-filled)',
                        command: '',
                      }),
                    );
                  } else {
                    const currentProperties = dbtSourcePropertiesString({
                      project,
                      sourceId,
                    });
                    const registeredProperties = etlSource.properties;
                    const propertiesEqual =
                      currentProperties === registeredProperties;
                    if (propertiesEqual) {
                      codeLenses.push(
                        new vscode.CodeLens(range, {
                          title: 'Source Registered $(pass-filled)',
                          command: '',
                        }),
                      );
                    }
                  }
                }
              }
            }
            return codeLenses;
          },
        },
      ),

      vscode.languages.registerDefinitionProvider(
        { pattern: '**/*.model.json' },
        {
          provideDefinition: async (document, position, token) => {
            const textLine = document.lineAt(position.line).text;

            const macroName = textLine.match(
              /{{ ((?:[A-z]|[0-9]|_|-|\.)+)\((?:.*)\) }}/,
            )?.[1];

            const modelName = textLine.match(
              /"((?:dim__|fct__|int__|mart__|src__|stg__)(?:[A-z]|[0-9]|_)+)"/,
            )?.[1];

            const sourceId = textLine.match(
              /"((?:[A-z]|[0-9]|_)+\.(?:[A-z]|[0-9]|_)+)"/,
            )?.[1];

            if (!(macroName || modelName || sourceId)) return;

            const project = this.coder.framework.dbt.getProjectFromPath(
              document.fileName,
            );
            if (!project) return;

            if (macroName) {
              const macro = frameworkGetMacro({ project, macro: macroName });
              if (!macro?.original_file_path) return;
              const macroPath = path.join(
                project.pathSystem,
                macro.original_file_path,
              );

              const fileLines = fs.readFileSync(macroPath, 'utf-8').split('\n');
              const macroLine = fileLines.findIndex((l) =>
                l.includes(`{% macro ${macroName}(`),
              );
              return new vscode.Location(
                vscode.Uri.file(macroPath),
                new vscode.Position(macroLine, 0),
              );
            } else if (modelName) {
              const model = frameworkGetNode({ project, model: modelName });
              if (!model?.original_file_path) return;
              const modelPath = path.join(
                project.pathSystem,
                model.original_file_path,
              );
              const jsonPath = modelPath.replace(/\.sql$/, '.model.json');
              if (fs.existsSync(jsonPath)) {
                return new vscode.Location(
                  vscode.Uri.file(jsonPath),
                  new vscode.Position(0, 0),
                );
              } else {
                return new vscode.Location(
                  vscode.Uri.file(jsonPath),
                  new vscode.Position(0, 0),
                );
              }
            } else if (sourceId) {
              const source = frameworkGetNode({ project, source: sourceId });
              if (!source?.original_file_path) return;
              const sourcePath = path.join(
                project.pathSystem,
                source.original_file_path,
              );

              let sourceLine = 0;
              const schemaName = sourceId.split('.')[0];
              const fileLines = fs
                .readFileSync(sourcePath, 'utf-8')
                .split('\n');
              const schemaStart = fileLines.findIndex((l) =>
                l.includes(`name: ${schemaName}`),
              );
              if (schemaStart >= 0) {
                const tableName = sourceId.split('.')[1];
                const tableStart = fileLines
                  .splice(schemaStart)
                  .findIndex((l) => l.includes(`name: ${tableName}`));
                if (tableStart >= 0) {
                  sourceLine = schemaStart + tableStart;
                }
              }
              return new vscode.Location(
                vscode.Uri.file(sourcePath),
                new vscode.Position(sourceLine, 0),
              );
            }
          },
        },
      ),
    );
  }

  /**
   * Register VS Code event handlers
   */
  registerEventHandlers(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.window.onDidChangeActiveTextEditor(async (editor) => {
        const document = editor?.document;
        if (!document) return;
        try {
          await this.coder.handleTextDocument(document);
        } catch (err) {
          this.coder.log.error('ERROR HANDLING DOCUMENT: ', err);
        }
      }),
    );
  }

  deactivate() {
    clearInterval(this.intervalPendingSyncs);
    this.dbt.deactivate();
  }
}
