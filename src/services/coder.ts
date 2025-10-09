import { Api } from '@services/api';
import { Framework } from '@services/framework';
import { Lightdash } from '@services/lightdash';
import { Trino } from '@services/trino';
import { State } from '@services/webviewcontroller';
import type { ApiMessage } from '@shared/api/types';
import { CoderFileInfo } from '@shared/coder/types';
import { getDjConfig } from '@services/config';
import {
  FILE_REGEX,
  FILE_WATCHER_PATTERN,
  GIT_LOG_PATH,
  IGNORE_PATHS_REGEX,
  SUPPORTED_EXTENSIONS,
  VIEW_ID,
} from '@shared/constants';
import type { DbtProject, DbtProjectManifest } from '@shared/dbt/types';
import { getDbtModelId, getDbtProperties } from '@shared/dbt/utils';
import {
  frameworkGetModelId,
  frameworkGetModelName,
  frameworkGetSourceId,
  frameworkMakeSourceName,
} from '@shared/framework/utils';
import type { GitAction } from '@shared/git/types';
import { gitLastLog } from '@shared/git/utils';
import { sqlFormat } from '@shared/sql/utils';
import { convertArgsForEnv, WORKSPACE_ROOT } from 'admin';
import * as fs from 'fs';
import { spawn } from 'node:child_process';
import * as path from 'path';
import * as vscode from 'vscode';
import { DJLogger } from './djLogger';
import { StateManager } from './statemanager';

export class Coder {
  api: Api;
  context: vscode.ExtensionContext;
  gitPending: boolean;
  framework: Framework;
  lastFileChange: Date | null;
  lastGitLog: { action: GitAction | null; line: string };
  lightdash: Lightdash;
  log: DJLogger;
  trino: Trino;
  watcher: vscode.FileSystemWatcher;
  stateManager: StateManager;

  constructor(context: vscode.ExtensionContext) {
    this.context = context;
    this.gitPending = false;
    this.lastFileChange = null;
    this.log = new DJLogger();

    this.framework = new Framework({ coder: this });
    this.lightdash = new Lightdash({ coder: this });
    this.trino = new Trino({ coder: this });

    // Initialize state management first
    try {
      this.stateManager = new StateManager(context);
    } catch (error) {
      this.log.error('[Coder] Failed to initialize state management:', error);
      throw new Error(
        `State management initialization failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }

    // Create state service
    const stateService = new State(this.stateManager);

    this.api = new Api({
      dbt: this.framework.dbt,
      framework: this.framework,
      lightdash: this.lightdash,
      trino: this.trino,
      state: stateService,
    });

    try {
      const gitLogFile = fs.readFileSync(
        path.join(WORKSPACE_ROOT, '.git', 'logs', 'HEAD'),
      );
      this.lastGitLog = gitLastLog(gitLogFile.toString());
    } catch {
      this.lastGitLog = { action: null, line: '' };
    }

    this.watcher = this.initWatcher();
  }

  async activate() {
    vscode.window.withProgress(
      {
        title: 'DJ Loading',
        location: vscode.ProgressLocation.Notification,
        cancellable: false,
      },
      async (progress, token) => {
        this.log.info('EXTENSION LOADING');

        const trackProgress = this.createProgressTracker(progress);

        try {
          await this.initializeServices(trackProgress);
          await this.loadProjectManifests(trackProgress);
          await this.finalizeActivation(trackProgress);

          this.log.info('EXTENSION READY');
        } catch (error) {
          this.log.error('EXTENSION ACTIVATION FAILED:', error);
          throw error;
        }
      },
    );
  }

  private createProgressTracker(
    progress: vscode.Progress<{ increment?: number; message?: string }>,
  ) {
    // Steps: tree views, framework, dbt, trino, lightdash, projects (N), finalization
    const totalSteps = 5 + this.framework.dbt.projects.size + 1;
    const incrementPerStep = 100 / totalSteps;

    return (message: string) => {
      progress.report({
        increment: incrementPerStep,
        message,
      });
    };
  }

  private async initializeServices(trackProgress: (message: string) => void) {
    trackProgress('Registering tree view providers');
    this.registerSubscriptions();

    trackProgress('Activating framework');
    await this.framework.activate(this.context);

    trackProgress('Activating dbt features');
    await this.framework.dbt.activate(this.context);

    trackProgress('Activating trino features');
    await this.trino.activate(this.context);

    trackProgress('Activating lightdash features');
    await this.lightdash.activate(this.context);
  }

  private async loadProjectManifests(trackProgress: (message: string) => void) {
    const projects = Array.from(this.framework.dbt.projects.values());

    for (const project of projects) {
      trackProgress(`Loading manifest: ${project.name}`);
      let manifest: DbtProjectManifest | null = null;

      try {
        manifest = await this.framework.dbt.fetchManifest({ project });
      } catch (err) {
        this.log.error('ERROR FETCHING INITIAL MANIFEST', err);
        return;
      }

      if (!manifest) {
        this.log.error(
          `SKIPPING: NO MANIFEST FOUND FOR PROJECT: ${project.name}`,
        );
        return;
      }

      try {
        await this.framework.dbt.handleManifest({ manifest, project });
        project.manifest = manifest;
      } catch (err) {
        this.log.error('ERROR HANDLING INITIAL MANIFEST', err);
      }

      // Don't await in case Trino is slow / times out
      this.framework.handleLoadEtlSources({ project }).catch((err) => {
        this.log.error(`ERROR LOADING ETL SOURCES: ${project.name}`, err);
      });
    }
  }

  private async finalizeActivation(trackProgress: (message: string) => void) {
    trackProgress('Handling initial document');

    try {
      await this.handleCurrentDocument();
    } catch (err) {
      this.log.error('ERROR HANDLING INITIAL DOCUMENT: ', err);
    }
  }

  /**
   * Return the current document (if one is open in the editor)
   */
  getCurrentDocument() {
    return vscode.window.activeTextEditor?.document;
  }

  /**
   * Return the path to the current document (if it exists)
   */
  getCurrentPath() {
    return this.getCurrentDocument()?.uri.fsPath;
  }

  /**
   * Fetch info about the current document
   */
  async fetchCurrentInfo() {
    const info = await this.fetchFileInfoFromPath(this.getCurrentPath());
    return info;
  }

  /**
   * Handles the active editor document
   */
  async handleCurrentDocument() {
    try {
      await this.handleTextDocument(this.getCurrentDocument());
    } catch (err) {
      this.log.error('ERROR HANDLING OPENED DOCUMENT: ', err);
    }
  }

  /**
   * Fetches information about a file based on its path
   * @param filePath The full system path for a given file
   * @returns Relevant information about the file
   */
  async fetchFileInfoFromPath(filePath?: string): Promise<CoderFileInfo> {
    if (!filePath) return null;

    const fileRegex = FILE_REGEX;

    const fileMatch = new RegExp(fileRegex).exec(filePath);
    if (fileMatch) {
      const uri = vscode.Uri.file(filePath);
      const [, name, extension] = fileMatch;
      const workspacePath = filePath.replace(WORKSPACE_ROOT, '');

      // Save resources by only checking these extensions
      if (!SUPPORTED_EXTENSIONS.includes(extension) && name !== 'HEAD') {
        return null;
      }

      let project: DbtProject | null = null;
      for (const [projectName, _project] of this.framework.dbt.projects) {
        const projectMatch = new RegExp(
          `${_project.pathSystem}${fileRegex}`,
        ).exec(filePath);
        if (projectMatch) {
          project = _project;
          break;
        }
      }

      if (project) {
        // If this file was found in a dbt project, we're now looking for dbt and framework files
        const projectPath = filePath.replace(project.pathSystem, '');
        const projectPathParts = projectPath.split('/').filter(Boolean);
        switch (extension) {
          case 'sql': {
            if (project.macroPaths.includes(projectPathParts[0])) {
              // Macro file was edited
              return { type: 'macro', name, project };
            }

            const modelId = getDbtModelId({
              modelName: name,
              projectName: project.name,
            });
            const model = modelId
              ? this.framework.dbt.models.get(modelId)
              : null;
            if (!model) return null;

            if (project.modelPaths.includes(projectPathParts[0]) && model) {
              // dbt model file
              const filePrefix = filePath.split('.sql')[0];
              return { type: 'model', filePrefix, model, project };
            }

            if (
              projectPathParts[0] === 'target' &&
              projectPathParts[1] === 'compiled'
            ) {
              // Compiled SQL file was updated
              const sqlFile = await vscode.workspace.fs.readFile(
                vscode.Uri.file(filePath),
              );
              let sql = sqlFile.toString();
              try {
                sql = sqlFormat(sql);
              } catch {
                // Fail silently
              }
              return {
                type: 'compiled',
                filePath,
                model,
                name,
                project,
                sql,
              };
            }
            return null;
          }
          case 'json': {
            if (projectPathParts[0] === 'target' || name === 'manifest') {
              // Manifest file was updated
              const manifest = await this.framework.dbt.fetchManifest({
                project,
              });
              if (!manifest) return null;
              return { type: 'manifest', project, manifest };
            }
          }
          case 'model.json': {
            const modelJson = await this.framework.fetchModelJson(
              vscode.Uri.file(filePath),
            );
            if (!modelJson) return null;
            const modelName = frameworkGetModelName(modelJson);
            const modelId = getDbtModelId({
              modelName,
              projectName: project.name,
            });
            const model = this.framework.dbt.models.get(modelId || '');
            return {
              type: 'framework-model',
              filePath,
              model,
              modelJson,
              project,
            };
          }
          case 'source.json': {
            const sourceJson = await this.framework.fetchSourceJson(
              vscode.Uri.file(filePath),
            );

            if (!sourceJson) return null;
            return {
              type: 'framework-source',
              filePath,
              project,
              sourceJson,
            };
          }
          case 'yml': {
            const ymlFile = await vscode.workspace.fs.readFile(uri);
            const properties = getDbtProperties(ymlFile.toString());
            return {
              type: 'yml',
              filePath,
              project,
              properties,
            };
          }
        }
      } else {
        // If this was found outside a dbt project, we're looking for other system files
        switch (workspacePath) {
          case `/${GIT_LOG_PATH}`: {
            try {
              const gitLogFile = fs.readFileSync(
                path.join(WORKSPACE_ROOT, GIT_LOG_PATH),
              );
              return {
                type: 'git-log',
                log: gitLastLog(gitLogFile.toString()),
              };
            } catch {
              return null;
            }
          }
        }
      }
    }

    // If we get here, means we didn't find a match
    return null;
  }

  /**
   * Handles the supplied editor document
   */
  async handleTextDocument(document?: vscode.TextDocument) {
    const documentPath = document?.uri.fsPath;
    const info = await this.fetchFileInfoFromPath(documentPath);

    switch (info?.type) {
      case 'framework-model':
      case 'model': {
        if (!info?.model) return;
        this.framework.dbt.handleModelNavigate(info);
        break;
      }
      default:
        this.framework.dbt.viewModelActions.setData([
          this.framework.dbt.treeItemJsonSync,
          this.framework.dbt.treeItemProjectClean,
          this.framework.dbt.treeItemModelCreate,
          this.framework.dbt.treeItemSourceCreate,
          this.lightdash.treeItemLightdashPreview,
          this.framework.dbt.treeItemDeferRun,
        ]);
        this.framework.dbt.viewSelectedResource.setData(
          this.framework.dbt.buildSelectedResource(info),
        );
    }
  }

  /**
   * Handles file change events
   */
  async handleWatcherEvent({
    type,
    uri,
  }: {
    type: 'change' | 'create' | 'delete';
    uri: vscode.Uri;
  }) {
    try {
      // First check whether a schema file was changed
      if (uri.fsPath.endsWith('.schema.json')) {
        return;
      }

      const currentDocument = this.getCurrentDocument();

      // Assume it was a dbt related file change
      switch (type) {
        case 'change': {
          const changedPath = uri.fsPath;
          const info = await this.fetchFileInfoFromPath(changedPath);
          if (!info) return;
          switch (info.type) {
            case 'compiled': {
              if (
                currentDocument &&
                currentDocument.uri.fsPath === info.model.pathSystemFile
              ) {
                // If we are currently viewing the updated model, update the views
                await this.handleTextDocument(currentDocument);
              }
              break;
            }
            case 'framework-model': {
              const id = frameworkGetModelId(info);
              if (!id) break;
              const pathJson = info.filePath;
              if (this.framework.shouldSkipSync({ id, pathJson })) break;

              // Don't await since this could take a while and we don't want to block the file watcher
              this.framework.handleGenerateModelFiles(info).catch((err) => {
                this.log.error('ERROR GENERATING MODEL FILES', err);
              });
              break;
            }
            case 'framework-source': {
              const sourceName = frameworkMakeSourceName(info.sourceJson);
              const id = frameworkGetSourceId({
                project: info.project,
                source: sourceName,
              });
              if (!id) break;
              const pathJson = info.filePath;
              if (this.framework.shouldSkipSync({ id, pathJson })) break;

              // Don't await since this could take a while and we don't want to block the file watcher
              this.framework.handleGenerateSourceFiles(info).catch((err) => {
                this.log.error('ERROR GENERATING SOURCE FILES', err);
              });
              break;
            }
            case 'git-log': {
              if (this.lastGitLog.line === info.log.line) {
                // If the last log line didn't change, do nothing
              } else {
                // Otherwise, set a new last log line and handle the change
                this.lastGitLog = info.log;
                // Re-sync all json files after these git actions
                switch (info.log.action) {
                  case 'checkout':
                  case 'pull': {
                    this.log.info('GIT ACTION: ', info.log.action);
                    this.gitPending = true;
                    setTimeout(() => {
                      // Keeps this true for 2 seconds, so that if file changes come in during the window, we know to trigger a full sync
                      this.gitPending = false;
                    }, 2000);
                  }
                }
              }
              break;
            }
            case 'manifest': {
              await this.framework.dbt.handleManifest(info);
              break;
            }
            case 'macro':
            case 'model':
            case 'yml': {
              this.lastFileChange = new Date();
              break;
            }
          }
          break;
        }
        case 'create': {
          const info = await this.fetchFileInfoFromPath(uri.fsPath);
          if (!info) return;
          switch (info?.type) {
            case 'framework-model': {
              const id = frameworkGetModelId(info);
              if (!id) break;
              const pathJson = info.filePath;
              if (this.framework.shouldSkipSync({ id, pathJson })) break;

              // Don't await since this could take a while and we don't want to block the file watcher
              this.framework.handleGenerateModelFiles(info).catch((err) => {
                this.log.error('ERROR GENERATING MODEL FILES', err);
              });
              break;
            }
            case 'framework-source': {
              const sourceName = frameworkMakeSourceName(info.sourceJson);
              const id = frameworkGetSourceId({
                project: info.project,
                source: sourceName,
              });
              if (!id) break;
              const pathJson = info.filePath;
              if (this.framework.shouldSkipSync({ id, pathJson })) break;

              // Don't await since this could take a while and we don't want to block the file watcher
              this.framework.handleGenerateSourceFiles(info).catch((err) => {
                this.log.error('ERROR GENERATING SOURCE FILES', err);
              });
              break;
            }
            case 'macro':
            case 'model':
            case 'yml': {
              this.lastFileChange = new Date();
              break;
            }
          }
          break;
        }
        default:
        // TODO: Handle delete
      }
    } catch (err) {
      this.log.error('FILE WATCHER ERROR: ', err);
    }
  }

  /**
   * Handler for api request messages coming from webviews
   */
  async handleWebviewMessage({
    message,
    webview,
  }: {
    message: ApiMessage;
    webview: vscode.Webview;
  }) {
    // When a message is received from the webview, we it should be a normal api payload with an id for resolving the request
    this.log.info('WEBVIEW MESSAGE RECEIVED: ', message);
    const _channelId = message._channelId;
    try {
      const response = await this.api.handleApi(message);
      webview.postMessage({ _channelId, response });
    } catch (err: any) {
      this.log.error('ERROR HANDLING WEBVIEW MESSAGE: ', err);
      webview.postMessage({
        _channelId,
        err: { message: err?.message || 'Unknown Error 1' },
      });
    }
  }

  createWebviewMessageHandler(panel: vscode.WebviewPanel, panelType: string) {
    return async (message: any) => {
      // Check if it's a close panel message
      if (message.type === 'close-panel' && message.panelType === panelType) {
        panel.dispose();
        return;
      }

      // Check if it's an open external URL message
      if (message.type === 'open-external-url' && message.url) {
        try {
          await vscode.env.openExternal(vscode.Uri.parse(message.url));
        } catch (error) {
          console.error('[Coder] Failed to open external URL:', error);
        }
        return;
      }

      this.handleWebviewMessage({ message, webview: panel.webview });
    };
  }

  /**
   * Watcher for all sql files in workspace
   */
  initWatcher() {
    const watcher =
      vscode.workspace.createFileSystemWatcher(FILE_WATCHER_PATTERN);
    const ignoreRegex = IGNORE_PATHS_REGEX;
    watcher.onDidChange(async (uri) => {
      if (ignoreRegex.test(uri.fsPath)) return;
      return this.handleWatcherEvent({ type: 'change', uri });
    });
    watcher.onDidCreate(async (uri) => {
      if (ignoreRegex.test(uri.fsPath)) return;
      return this.handleWatcherEvent({ type: 'create', uri });
    });
    return watcher;
  }

  registerSubscriptions() {
    // Register Subscriptions
    this.context.subscriptions.push(
      vscode.window.registerTreeDataProvider(
        VIEW_ID.PROJECT_NAVIGATOR,
        this.framework.dbt.viewProjectNavigator,
      ),
      vscode.window.registerTreeDataProvider(
        VIEW_ID.MODEL_ACTIONS,
        this.framework.dbt.viewModelActions,
      ),
      vscode.window.registerTreeDataProvider(
        VIEW_ID.SELECTED_RESOURCE,
        this.framework.dbt.viewSelectedResource,
      ),
      vscode.window.registerTreeDataProvider(
        VIEW_ID.QUERY_ENGINE,
        this.trino.viewQueryEngine,
      ),
    );
  }

  runProcess({
    command: _command,
    logger,
    path: _path,
  }: {
    command: string;
    logger?: { error?: (text: string) => void; info?: (text: string) => void };
    path?: string;
  }): Promise<string> {
    const { command, path } = convertArgsForEnv({
      command: _command,
      path: _path,
      venv: getDjConfig().pythonVenvPath,
    });

    const cwd = `${WORKSPACE_ROOT}/${path}`;

    return new Promise((resolve, reject) => {
      let stderror = '';
      let stdout = '';
      const process = spawn(command, {
        cwd,
        shell: true,
      });
      process.stdout.on('data', (data) => {
        logger?.info?.('GOT DATA');
        if (logger?.info) logger.info(data.toString());
        stdout += data.toString();
      });
      process.stderr.on('data', (data) => {
        if (logger?.error) logger.error(data.toString());
        stderror += data.toString();
      });
      process.on('exit', (code) => {
        switch (code) {
          case 0:
            resolve(stdout);
            break;
          default:
            reject(stderror || `Error Code ${code}`);
            break;
        }
      });
    });
  }

  /**
   * Get Python virtual environment configuration for terminal
   * Returns environment variables and PATH modifications needed for the venv
   */
  getVenvEnvironment(): { [key: string]: string } {
    try {
      const { pythonVenvPath } = getDjConfig();
      if (!pythonVenvPath) return {};

      const absVenv = path.isAbsolute(pythonVenvPath)
        ? pythonVenvPath
        : path.join(WORKSPACE_ROOT, pythonVenvPath);

      const binPath = path.join(absVenv, 'bin');
      const activatePath = path.join(binPath, 'activate');

      if (fs.existsSync(activatePath)) {
        // Set up environment variables as if venv was activated
        const currentPath = process.env.PATH || '';
        return {
          VIRTUAL_ENV: absVenv,
          PATH: `${binPath}:${currentPath}`,
          // Remove PYTHONHOME if set (venv requirement)
          PYTHONHOME: undefined as any,
        };
      } else {
        return {};
      }
    } catch {
      return {}; // Fail-safe
    }
  }

  /**
   * Execute a dbt command in a properly configured terminal
   * Handles virtual environment setup and provides consistent error handling
   */
  async executeDbtCommand(
    command: string,
    terminalName: string,
    cwd: string,
  ): Promise<void> {
    try {
      const terminal = await vscode.window.createTerminal({
        name: terminalName,
        cwd: cwd,
        env: this.getVenvEnvironment(),
      });
      terminal.show();
      terminal.sendText(command);
    } catch (err) {
      this.log.error(`ERROR EXECUTING DBT COMMAND: ${command}`, err);
      throw err;
    }
  }

  async showOrOpenFile(
    pathOrUri: string | vscode.Uri,
    options?: {
      viewColumn?: vscode.ViewColumn;
    },
  ) {
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

  deactivate() {
    this.watcher.dispose();
    this.api.deactivate();
    this.framework.deactivate();
    this.lightdash.deactivate();
    this.trino.deactivate();
    this.log.dispose();
  }
}
