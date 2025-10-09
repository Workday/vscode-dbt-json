import { Coder } from '@services/coder';
import {
  getDbtProjectExcludePaths,
  getDjConfig,
  isDbtProjectNameConfigured,
} from '@services/config';
import { Framework } from '@services/framework';
import { DJService } from '@services/types';
import { assertExhaustive, jsonParse } from '@shared';
import { ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import { CoderFileInfo } from '@shared/coder/types';
import {
  COMMAND_ID,
  DBT_MSG,
  DEFAULT_DBT_PATH,
  VIEW_ID,
} from '@shared/constants';
import type {
  DbtModel,
  DbtProject,
  DbtProjectManifest,
  DbtProjectManifestMacro,
  DbtSeed,
  DbtSource,
} from '@shared/dbt/types';
import { getDbtProjectProperties } from '@shared/dbt/utils';
import { FrameworkSchemaBase } from '@shared/framework/types';
import {
  frameworkGetMacro,
  frameworkGetNode,
  frameworkGetNodeColumns,
  frameworkMakeSourceId,
} from '@shared/framework/utils';
import { sqlFormat } from '@shared/sql/utils';
import { getHtml } from '@shared/web/utils';
import {
  BASE_AIRFLOW_PATH,
  BASE_MACROS_PATH,
  DJ_SCHEMAS_PATH,
  TreeData,
  TreeDataInstance,
  TreeItem,
  WORKSPACE_ROOT,
} from 'admin';
import * as fs from 'fs';
import * as _ from 'lodash';
import * as path from 'path';
import * as vscode from 'vscode';

export class Dbt implements DJService {
  coder: Coder;
  framework: Framework;
  handleApi: (payload: ApiPayload<'dbt'>) => Promise<ApiResponse>;
  macros = new Map<string, DbtProjectManifestMacro>();
  models = new Map<string, DbtModel>();
  // Flag to prevent multiple parse commands from running at once
  parsing: boolean = false;
  // Track how many parse requests came in
  parsingCounter: number = 0;
  projects = new Map<string, DbtProject>();
  ready: boolean = false;
  seeds = new Map<string, DbtSeed>();
  sources = new Map<string, DbtSource>();
  treeItemDeferRun: TreeItem = {
    command: {
      command: COMMAND_ID.DEFER_RUN,
      title: DBT_MSG.RUN_DEFER,
    },
    iconPath: new vscode.ThemeIcon('run-all-coverage'),
    label: DBT_MSG.RUN_DEFER,
  };
  treeItemJsonSync: TreeItem = {
    command: {
      command: COMMAND_ID.JSON_SYNC,
      title: DBT_MSG.SYNC_JSON_MODELS,
    },
    iconPath: new vscode.ThemeIcon('sync'),
    label: DBT_MSG.SYNC_JSON_MODELS,
  };
  treeItemModelCreate: TreeItem = {
    command: {
      command: COMMAND_ID.MODEL_CREATE,
      title: DBT_MSG.CREATE_MODEL,
    },
    iconPath: new vscode.ThemeIcon('add'),
    label: DBT_MSG.CREATE_MODEL,
  };
  treeItemModelCompile: TreeItem = {
    command: {
      command: COMMAND_ID.MODEL_COMPILE,
      title: DBT_MSG.COMPILE_MODEL,
    },
    iconPath: new vscode.ThemeIcon('beaker'),
    label: DBT_MSG.COMPILE_MODEL,
  };
  treeItemModelRun: TreeItem = {
    command: {
      command: COMMAND_ID.MODEL_RUN,
      title: DBT_MSG.RUN_MODEL,
    },
    iconPath: new vscode.ThemeIcon('play'),
    label: DBT_MSG.RUN_MODEL,
  };
  treeItemModelRunLineage: TreeItem = {
    command: {
      command: COMMAND_ID.MODEL_RUN_LINEAGE,
      title: DBT_MSG.RUN_MODEL_LINEAGE,
    },
    iconPath: new vscode.ThemeIcon('run-all'),
    label: DBT_MSG.RUN_MODEL_LINEAGE,
  };
  treeItemProjectClean: TreeItem = {
    command: {
      command: COMMAND_ID.PROJECT_CLEAN,
      title: DBT_MSG.CLEAN_PROJECT,
    },
    iconPath: new vscode.ThemeIcon('debug-restart'),
    label: DBT_MSG.CLEAN_PROJECT,
  };
  treeItemSourceCreate: TreeItem = {
    command: {
      command: COMMAND_ID.SOURCE_CREATE,
      title: DBT_MSG.CREATE_SOURCE,
    },
    iconPath: new vscode.ThemeIcon('add'),
    label: DBT_MSG.CREATE_SOURCE,
  };

  // Webview panels
  webviewPanelSourceCreate: vscode.WebviewPanel | undefined;
  webviewPanelModelCreate: vscode.WebviewPanel | undefined;

  // Tree views
  viewModelActions: TreeDataInstance;
  viewProjectNavigator: TreeDataInstance;
  viewSelectedResource: TreeDataInstance;

  constructor({ coder, framework }: { coder: Coder; framework: Framework }) {
    this.coder = coder;
    this.framework = framework;

    this.viewModelActions = new TreeDataInstance([
      { label: 'Extension loading...' },
    ]);
    this.viewProjectNavigator = new TreeDataInstance([
      { label: 'Extension loading...' },
    ]);
    this.viewSelectedResource = new TreeDataInstance([
      { label: 'Extension loading...' },
    ]);

    this.handleApi = async (payload) => {
      switch (payload.type) {
        case 'dbt-fetch-modified-models': {
          const { projectName } = payload.request;
          const project = this.projects.get(projectName);
          if (!project) throw new Error('Project not found');
          const models: string[] = [];
          const diffLocalResult = await this.coder.runProcess({
            command: 'git --no-pager diff --name-only HEAD',
          });
          const diffMasterResult = await this.coder.runProcess({
            command: 'git --no-pager diff --name-only origin/master..',
          });
          const resultArray = [
            ...diffLocalResult.toString().split('\n').filter(Boolean),
            ...diffMasterResult.toString().split('\n').filter(Boolean),
          ];
          // Use Set to avoid duplicate checks and array.includes() calls
          const modelSet = new Set<string>();
          for (const filePath of resultArray) {
            for (const modelPath of project.modelPaths) {
              const checkPath = path.join(project.pathRelative, modelPath);
              this.coder.log.info({ checkPath, filePath });
              const match = new RegExp(
                `^${checkPath}\/(?:.+\/)*((?:[a-z]|[0-9]|_)+)\.sql$`,
              ).exec(filePath);
              if (match) {
                const modelName = match[1];
                modelSet.add(modelName);
              }
            }
          }
          return apiResponse<typeof payload.type>(Array.from(modelSet));
        }
        case 'dbt-fetch-projects': {
          const projects: DbtProject[] = [];
          for (const project of this.projects.values()) {
            projects.push(project);
          }
          return apiResponse<typeof payload.type>(projects);
        }
        case 'dbt-run-model': {
          const { modelName, projectName } = payload.request;
          const project = this.projects.get(projectName);
          if (!project) throw new Error('Project not found');
          await this.coder.runProcess({
            command: `dbt run --select "${modelName}"`,
            path: project.pathRelative,
          });
          return apiResponse<typeof payload.type>(null);
        }
        case 'dbt-run-model-lineage': {
          const { modelName, projectName } = payload.request;
          const project = this.projects.get(projectName);
          if (!project) throw new Error('Project not found');
          await this.coder.runProcess({
            command: `dbt run --select "+${modelName}+"`,
            path: project.pathRelative,
          });
          return apiResponse<typeof payload.type>(null);
        }
        case 'dbt-parse-project': {
          const { logger, project } = payload.request;
          await this.coder.runProcess({
            command: 'dbt parse',
            path: project.pathRelative,
            logger,
          });
          const manifest = await this.fetchManifest({ project });
          if (!manifest) throw new Error('Manifest not found');
          return apiResponse<typeof payload.type>(manifest);
        }
        default:
          return assertExhaustive<ApiResponse>(payload);
      }
    };
  }

  async activate(context: vscode.ExtensionContext) {
    // Find all dbt projects in the workspace
    const projectYmlUris = await vscode.workspace.findFiles(
      '**/dbt_project.yml',
      getDbtProjectExcludePaths(),
    );
    // Initialize projects in parallel
    await Promise.all(
      projectYmlUris.map((projectYmlUri) => this.initProject(projectYmlUri)),
    );

    this.initHoverProvider(context);

    // Register commands
    this.registerCommands(context);

    this.ready = true;
  }

  /**
   * Returns project navigator tree view data
   */
  buildProjectNavigator({ project }: { project: DbtProject }): TreeData {
    const { models, seeds, sources } =
      this.framework.getManifestResources(project);
    const orderedResources = this.framework.getOrderedResources({ project });

    const itemsSources: TreeItem[] = [];
    const itemsModelsStaging: TreeItem[] = [];
    const itemsModelsIntermediate: TreeItem[] = [];
    const itemsModelsMart: TreeItem[] = [];

    for (const [sourceId, source] of Object.entries(sources)) {
      const sourceName = sourceId.split('.').slice(2).join('.');
      itemsSources.push({
        id: sourceId,
        label: sourceName,
        // description: source.description,
        // tooltip: source.tooltip,
        children: [],
        command: {
          title: 'Open Source',
          command: COMMAND_ID.SOURCE_NAVIGATE,
          arguments: [source],
        },
      });
    }
    // Use a single sort comparator function
    const sortByLabel = (a: TreeItem, b: TreeItem) => {
      const aLabel = _.toLower(a.label as string);
      const bLabel = _.toLower(b.label as string);
      return aLabel.localeCompare(bLabel);
    };

    itemsSources.sort(sortByLabel);

    for (const [modelId, model] of Object.entries(models)) {
      const modelName = modelId.split('.').slice(2).join('.');
      const [modelLayer, ...modelLabelParts] = modelName.split('__');
      const modelLabel = modelLabelParts.join('__');
      const modelItem = {
        id: modelId,
        label: modelLabel,
        // description: modelName,
        // tooltip: modelName,
        children: [],
        command: {
          title: 'Open Model',
          command: COMMAND_ID.MODEL_NAVIGATE,
          arguments: [model],
        },
      };
      switch (modelLayer) {
        case 'stg':
          itemsModelsStaging.push(modelItem);
          break;
        case 'int':
          itemsModelsIntermediate.push(modelItem);
          break;
        case 'mart':
          itemsModelsMart.push(modelItem);
          break;
      }
    }
    itemsModelsStaging.sort(sortByLabel);
    itemsModelsIntermediate.sort(sortByLabel);
    itemsModelsMart.sort(sortByLabel);

    const roots = [...this.framework.modelTreeRoots.values()].sort(sortByLabel);

    const treeData = [
      {
        label: 'Sources',
        collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
        children: itemsSources,
      },
      {
        label: 'Staging Models',
        collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
        children: itemsModelsStaging,
      },
      {
        label: 'Intermediate Models',
        collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
        children: itemsModelsIntermediate,
      },
      {
        label: 'Mart Models',
        collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
        children: itemsModelsMart,
      },
    ];

    return treeData;
  }

  /**
   * Returns selected resource tree view data
   */
  buildSelectedResource(info?: CoderFileInfo): TreeData {
    const treeData: TreeData = [];

    switch (info?.type) {
      case 'framework-model':
      case 'model': {
        const { model, project } = info;
        if (!model) return treeData;

        const { dimensions, facts } = frameworkGetNodeColumns({
          from: { model: model.name },
          project,
        });
        treeData.push({ label: 'Type', description: 'Model' });
        treeData.push({ label: 'Name', description: model.name });
        treeData.push({
          label: 'Dimensions',
          children:
            dimensions?.map((d) => ({
              label: d.name,
              command: {
                title: 'Column Origin',
                command: COMMAND_ID.COLUMN_ORIGIN,
                arguments: [d],
              },
            })) || [],
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          description: String(dimensions?.length || 0),
        });
        treeData.push({
          label: 'Facts',
          children:
            facts?.map((f) => ({
              label: f.name,
              command: {
                title: 'Column Origin',
                command: COMMAND_ID.COLUMN_ORIGIN,
                arguments: [f],
              },
            })) || [],
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          description: String(facts?.length || 0),
        });

        const nodeId = model.unique_id || '';
        const modelNameSelected = model.name || '';
        const modelLabelSelected = modelNameSelected.split('__').pop() || '';

        const treeDataParents: TreeData = [];
        const modelIdsParents =
          info.project.manifest.parent_map[nodeId]?.filter((id) =>
            id.startsWith('model.'),
          ) || [];
        for (const modelIdParent of modelIdsParents) {
          const modelParent = this.framework.dbt.models.get(modelIdParent);
          if (!modelParent) continue;
          const modelNameParent = modelIdParent.split('.').slice(2).join('.');
          const modelLabelParent = modelNameParent.split('__').pop() || '';
          treeDataParents.push({
            id: modelIdParent,
            label: modelLabelParent,
            description: modelNameParent,
            tooltip: modelNameParent,
            children: [],
            command: {
              title: 'Open Model',
              command: COMMAND_ID.MODEL_NAVIGATE,
              arguments: [modelParent],
            },
          });
        }

        const treeDataChildren: TreeData = [];
        const modelIdsChildren =
          info.project.manifest.child_map[nodeId]?.filter((id) =>
            id.startsWith('model.'),
          ) || [];
        for (const modelIdChild of modelIdsChildren) {
          const modelNameChild = modelIdChild.split('.').slice(2).join('.');
          const modelChild = this.framework.dbt.models.get(modelIdChild);
          if (!modelChild) continue;
          const modelLabelChild = modelNameChild.split('__').pop() || '';
          treeDataChildren.push({
            id: modelIdChild,
            label: modelLabelChild,
            description: modelNameChild,
            tooltip: modelNameChild,
            children: [],
            command: {
              title: 'Open Model',
              command: COMMAND_ID.MODEL_NAVIGATE,
              arguments: [modelChild],
            },
          });

          treeData.push({
            children: [
              {
                children: [
                  {
                    children: treeDataParents,
                    collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
                    label: 'Parents',
                  },
                  {
                    children: treeDataChildren,
                    collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
                    label: 'Children',
                  },
                ],
                collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
                description: modelNameSelected,
                label: modelLabelSelected,
                tooltip: modelNameSelected,
              },
            ],
            collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
            label: 'Selected',
          });
        }
        break;
      }
      case 'framework-source':
      case 'yml': {
        const { filePath, project } = info;
        const sources: DbtSource[] = [];
        if (info?.type === 'framework-source') {
          for (const table of info.sourceJson.tables || []) {
            const sourceId = frameworkMakeSourceId({
              ...info.sourceJson,
              project,
              table: table.name,
            });
            const source = this.framework.dbt.sources.get(sourceId);
            if (source) sources.push(source);
          }
        } else if (info?.type === 'yml') {
          if ('sources' in info.properties && info.properties.sources?.length) {
            for (const _source of info.properties.sources) {
              for (const table of _source.tables || []) {
                this.coder.log.info('TABLE', table);
                const sourceId = frameworkMakeSourceId({
                  ..._source,
                  project,
                  table: table.name,
                });
                this.coder.log.info('ID', sourceId);
                const source = this.framework.dbt.sources.get(sourceId);
                if (source) sources.push(source);
              }
            }
          }
        }
        if (!sources.length) return treeData;
        const sourcesByName = _.groupBy(sources, (s) => s.source_name);
        treeData.push({
          label: 'Sources',
          children: _.map(sourcesByName, (sourceTables, sourceName) => ({
            label: sourceName || '',
            children: [
              {
                label: 'Tables',
                children: sourceTables.map((table) => ({
                  label: table.name,
                  command: {
                    title: 'Find Source Origin',
                    command: COMMAND_ID.SOURCE_ORIGIN,
                    arguments: [
                      {
                        filePath,
                        projectName: project.name,
                        tableName: table.name,
                      },
                    ],
                  },
                })),
                collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
                description: String(sourceTables?.length || 0),
              },
            ],
            collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
          })),
          collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
        });
        break;
      }
    }

    return treeData;
  }

  getModelNodeId({
    project,
    modelName,
  }: {
    project: DbtProject;
    modelName: string;
  }) {
    const nodeId = `model.${project.name}.${modelName}`;
    return nodeId;
  }

  getProjectFromPath(path: string): DbtProject | null {
    for (const [projectName, project] of this.projects) {
      if (new RegExp(`^${project.pathSystem}`).test(path)) return project;
    }
    return null;
  }

  // Initialize the hover provider after the projects have been loaded
  initHoverProvider(context: vscode.ExtensionContext) {
    const self = this;

    // Create hover provider to idenify macros within workspace
    context.subscriptions.push(
      vscode.languages.registerHoverProvider(
        { pattern: '**/*.sql' },
        {
          provideHover(document, position, token) {
            const textLine = document.lineAt(position.line).text;
            const textLeading = textLine
              .slice(0, position.character)
              .replaceAll(/.+(\)|\s)/g, '');
            const text = textLeading + textLine.slice(position.character);
            const ref = text.match(/((?:[A-z]|[0-9]|\.)+)\(/)?.[1];
            if (!ref) return;

            const project = self.getProjectFromPath(document.fileName);
            if (!project) return;

            const refId =
              ref.split('.').length === 1 ? `${project.name}.${ref}` : ref;
            const macro = self.macros.get(refId);

            const hoverText = macro?.description || macro?.macro_sql;
            if (!hoverText) return;

            return new vscode.Hover(hoverText);
          },
        },
      ),
    );

    // Create hover provider to idenify model.json files within workspace
    context.subscriptions.push(
      vscode.languages.registerHoverProvider(
        { pattern: '**/*.model.json' },
        {
          provideHover(document, position, token) {
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

            const project = self.getProjectFromPath(document.fileName);
            if (!project) return;

            let hoverText = '';

            if (macroName) {
              const macro = frameworkGetMacro({ project, macro: macroName });
              if (!macro) return;
              hoverText = `
**Macro: ${macro.name}**

*SQL*

${macro.macro_sql}`;
              // ${macro.macro_sql.replaceAll('\n', '\n\n')}`;
            } else if (modelName) {
              const model = frameworkGetNode({
                project,
                model: modelName,
              });
              if (!model) return;
              hoverText = `
**Model: ${model.name}**

*Columns*`;
              for (const c of Object.values(model.columns || {})) {
                if (!c) continue;
                hoverText += '\n\n' + c.name;
                if (c.meta.type) hoverText += ` - ${c.meta.type}`;
                if (c.meta.data_type) hoverText += ` - ${c.meta.data_type}`;
              }
            } else if (sourceId) {
              const source = frameworkGetNode({
                project,
                source: sourceId,
              });
              if (!source) return;
              hoverText = `
**Source: ${source.name}**

*Columns*
`;
              for (const c of Object.values(source.columns || {})) {
                if (!c) continue;
                hoverText += '\n\n' + c.name;
                if (c.meta.type) hoverText += ` - ${c.meta.type}`;
                if (c.meta.data_type) hoverText += ` - ${c.meta.data_type}`;
              }
            }

            if (!hoverText) return;

            return new vscode.Hover(hoverText.trim());
          },
        },
      ),
    );
  }

  /**
   * Initialize a dbt project by parsing the dbt project file and
   * writing the airflow dags and macros files if they are configured.
   * This will be called recursively for each nested package.
   * @param projectYmlUri The URI of the dbt project file
   */
  async initProject(projectYmlUri: vscode.Uri) {
    /**
     * DBT Project File Content
     */
    const projectFile = await vscode.workspace.fs.readFile(projectYmlUri);

    /**
     * DBT Project Properties
     */
    const projectProperties = getDbtProjectProperties(projectFile.toString());

    // If the project name is not set, return
    if (!projectProperties.name) return;

    const { dbtProjectNames } = getDjConfig();
    this.coder.log.info('PROJECT NAMES CONFIGURED: ', dbtProjectNames);

    // Check if this project can be initialized based on configuration
    const isProjectAllowed = isDbtProjectNameConfigured(projectProperties.name);
    if (!isProjectAllowed) {
      this.coder.log.info(`SKIPPING PROJECT: ${projectProperties.name}`);
      return;
    }

    this.coder.log.info(
      'INITIALIZING PROJECT: ',
      projectProperties.name,
      projectYmlUri.fsPath,
    );

    // Begin building the project object
    const pathSystem = projectYmlUri.fsPath.replace(/\/dbt_project.yml$/, '');
    const pathRelative = pathSystem.replace(
      new RegExp(`^${WORKSPACE_ROOT}\/`),
      '',
    );
    const modelPaths = projectProperties['model-paths'] || ['models'];

    const project: DbtProject = {
      macroPaths: projectProperties['macro-paths'] || ['macros'],
      // macros: {},
      manifest: {
        child_map: {},
        disabled: {},
        docs: {},
        exposures: {},
        group_map: {},
        groups: {},
        macros: {},
        metadata: {},
        metrics: {},
        nodes: {},
        parent_map: {},
        saved_queries: {},
        selectors: {},
        semantic_models: {},
        sources: {},
      },
      modelPaths,
      // models: {},
      name: projectProperties.name,
      packagePath: projectProperties['packages-install-path'] || 'dbt_packages',
      // packages: [],
      pathRelative,
      pathSystem,
      properties: projectProperties,
      targetPath: projectProperties['target-path'] || 'target',
      variables: projectProperties.vars || {},
    };

    this.coder.log.info('SETTING PROJECT');
    this.projects.set(project.name, project);

    // Write the dags contributed by the extension so they'll be available for airflow
    await this.writeAirflowDags();

    // Write the macros contributed by the extension before parsing so they'll get picked up in the next update
    await this.writeMacroFiles(project);
  }

  /**
   * Write Airflow DAG files contributed by the extension
   * This method does nothing if the Airflow generate DAGs is not enabled.
   * @private
   */
  private async writeAirflowDags(): Promise<void> {
    const { airflowTargetVersion, airflowGenerateDags, airflowDagsPath } =
      getDjConfig();

    if (!airflowGenerateDags) return;

    let airflowVersionFolder = 'v2_7';

    switch (airflowTargetVersion) {
      case '2.10':
        airflowVersionFolder = 'v2_10';
        break;
    }

    if (airflowVersionFolder) {
      this.coder.log.info('WRITING AIRFLOW FILES');
      const AIRFLOW_FILES: string[] = [
        'services.py',
        'source_etl.py',
        'utils.py',
        'variables.py',
      ];

      // Use Promise.all for parallel file operations
      const writePromises = AIRFLOW_FILES.map(async (airflowFile) => {
        try {
          const pathRead = path.join(
            BASE_AIRFLOW_PATH,
            airflowVersionFolder,
            airflowFile,
          );
          const pathWrite = path.join(
            WORKSPACE_ROOT,
            airflowDagsPath,
            airflowFile,
          );
          // Use async file reading instead of sync
          const content = await vscode.workspace.fs.readFile(
            vscode.Uri.file(pathRead),
          );
          await vscode.workspace.fs.writeFile(
            vscode.Uri.file(pathWrite),
            content,
          );
        } catch (err) {
          this.coder.log.error(`Error writing ${airflowFile}:`, err);
        }
      });

      await Promise.all(writePromises);
    }
  }

  /**
   * Write macro files contributed by the extension
   * @param project The dbt project to write macros for if dbtMacroPath is set.
   * @private
   */
  private async writeMacroFiles(project: DbtProject): Promise<void> {
    const { dbtMacroPath } = getDjConfig();

    if (!dbtMacroPath) return;

    this.coder.log.info('WRITING MACRO FILES');
    try {
      // Use async readdir
      const macroFileNames = await vscode.workspace.fs.readDirectory(
        vscode.Uri.file(BASE_MACROS_PATH),
      );

      // Process all macro files in parallel
      const writePromises = macroFileNames
        .filter(([name, type]) => type === vscode.FileType.File)
        .map(async ([macroFileName]) => {
          try {
            const sourcePath = vscode.Uri.file(
              path.join(BASE_MACROS_PATH, macroFileName),
            );
            const targetPath = vscode.Uri.file(
              path.join(
                project.pathSystem,
                project.macroPaths[0],
                dbtMacroPath,
                macroFileName,
              ),
            );

            // Read and write using async operations
            const content = await vscode.workspace.fs.readFile(sourcePath);
            await vscode.workspace.fs.writeFile(targetPath, content);
          } catch (err) {
            this.coder.log.error(
              `Error writing macro file ${macroFileName}:`,
              err,
            );
          }
        });

      await Promise.all(writePromises);
    } catch (err) {
      this.coder.log.error('Error writing macro files:', err);
    }
  }

  /**
   * Fetches the compiled sql for a model
   */
  async fetchSql({
    project,
    modelPath,
    modelName,
  }: {
    project: DbtProject;
    modelPath: string;
    modelName: string;
  }) {
    try {
      let compiledSql = '';
      const compiledSqlPath = `${project.pathSystem}/target/compiled/${project.name}/${modelPath}/${modelName}.sql`;

      if (fs.existsSync(compiledSqlPath)) {
        const compiledSqlFile = await vscode.workspace.fs.readFile(
          vscode.Uri.file(compiledSqlPath),
        );
        compiledSql = compiledSqlFile.toString();
        try {
          compiledSql = sqlFormat(compiledSql);
        } catch {
          // Fail silently when compiling sql
        }
        return compiledSql;
      } else {
        return null;
      }
    } catch {
      return null;
    }
  }

  /**
   * Fetches a project's manifest file from the system path
   */
  async fetchManifest({ project }: { project: DbtProject }) {
    try {
      const manifestFile = await vscode.workspace.fs.readFile(
        vscode.Uri.file(
          `${project.pathSystem}/${project.targetPath}/manifest.json`,
        ),
      );
      const manifestString = manifestFile.toString();
      if (!manifestString) return null;
      return jsonParse(manifestString) as DbtProjectManifest;
    } catch {
      return null;
    }
  }

  async handleManifest({
    manifest,
    project,
  }: {
    manifest: DbtProjectManifest;
    project: DbtProject;
  }) {
    const isProjectAllowed = isDbtProjectNameConfigured(project.name);
    if (!isProjectAllowed) {
      this.coder.log.info('Skipping manifest for', project.name);
      return null;
    }

    project = { ...project, manifest };

    // When we get a new dbt manifest, we'll use it to update the project info and json schemas
    this.projects.set(project.name, project);

    // Update project macros
    for (const [macroKey, macro] of Object.entries(manifest.macros)) {
      const macroId = macroKey.split('.').slice(1).join('.');
      this.macros.set(macroId, macro);
    }

    // Handle groups
    const groupEnum: string[] = [];
    for (const group of Object.values(manifest.groups || {})) {
      if (!group?.name) continue;
      this.framework.groupNames.add(group.name);
    }
    for (const groupId of this.framework.groupNames.values()) {
      groupEnum.push(groupId);
    }
    try {
      const modelGroupSchema = jsonParse(
        (
          await vscode.workspace.fs.readFile(
            vscode.Uri.file(
              path.join(DJ_SCHEMAS_PATH, 'model.group.schema.json'),
            ),
          )
        ).toString(),
      ) as FrameworkSchemaBase;
      modelGroupSchema.enum = groupEnum;
      await vscode.workspace.fs.writeFile(
        vscode.Uri.file(path.join(DJ_SCHEMAS_PATH, 'model.group.schema.json')),
        Buffer.from(JSON.stringify(modelGroupSchema)),
      );
    } catch (err) {
      this.coder.log.error('Error setting group schema', err);
    }

    // Handle models & seeds
    const modelEnum = new Set<string>();

    // Process nodes in batches to reduce memory pressure
    const nodeEntries = Object.entries(manifest.nodes || {});
    const BATCH_SIZE = 100;

    for (let i = 0; i < nodeEntries.length; i += BATCH_SIZE) {
      const batch = nodeEntries.slice(i, i + BATCH_SIZE);

      for (const [manifestId, manifestNode] of batch) {
        if (
          !manifestNode?.name ||
          !manifestNode?.resource_type ||
          !['model', 'seed'].includes(manifestNode.resource_type)
        ) {
          // We only care about models and seeds here
          continue;
        }

        const childMap = manifest.child_map[manifestId] || [];
        const parentMap = manifest.parent_map[manifestId] || [];
        const pathRelativeFile = manifestNode?.original_file_path || '';
        const pathRelativeDirectory = path.dirname(pathRelativeFile);
        const pathSystemDirectory = path.join(
          project.pathSystem,
          pathRelativeDirectory,
        );
        const pathSystemFile = path.join(project.pathSystem, pathRelativeFile);

        switch (manifestNode.resource_type) {
          case 'model': {
            const modelRef = manifestNode.name;
            modelEnum.add(modelRef);
            const modelName = modelRef;
            const modelId = manifestId;
            if (!modelId) continue;

            const model: DbtModel = {
              ...manifestNode,
              childMap,
              description: manifestNode.description || '',
              name: manifestNode.name || '',
              parentMap,
              pathRelativeDirectory,
              pathSystemDirectory,
              pathSystemFile,
            };
            this.models.set(modelId, model);
            if (
              manifest.parent_map[modelId]?.some((p) => p.startsWith('source.'))
            ) {
              // TODO: Change the roots to sources instead of models
              const modelLabel = modelName.split('__').pop() || '';
              this.framework.modelTreeRoots.set(modelName, {
                id: modelId,
                label: modelLabel,
                description: modelName,
                tooltip: modelName,
                children: [],
                command: {
                  title: 'Open Model',
                  command: COMMAND_ID.MODEL_NAVIGATE,
                  arguments: [model],
                },
              });
            }
            break;
          }
          case 'seed': {
            const seedId = manifestId;
            const seedRef = manifestNode.name;
            // Treating seeds as models for now
            modelEnum.add(seedRef);
            const seed: DbtSeed = {
              ...manifestNode,
              childMap,
              parentMap: [],
              pathRelativeDirectory,
              pathSystemDirectory,
              pathSystemFile,
            };
            this.seeds.set(seedId, seed);
            break;
          }
        }
      }
    }

    // Convert Set to Array for the enum
    const modelEnumArray = Array.from(modelEnum);
    if (modelEnumArray.length) {
      // Don't overwrite if new model enums are empty, this likely means the manifest parsing failed
      try {
        const modelRefSchema = jsonParse(
          (
            await vscode.workspace.fs.readFile(
              vscode.Uri.file(
                path.join(DJ_SCHEMAS_PATH, 'model.ref.schema.json'),
              ),
            )
          ).toString(),
        ) as FrameworkSchemaBase;
        modelRefSchema.enum = modelEnumArray;
        await vscode.workspace.fs.writeFile(
          vscode.Uri.file(path.join(DJ_SCHEMAS_PATH, 'model.ref.schema.json')),
          Buffer.from(JSON.stringify(modelRefSchema)),
        );
      } catch (err) {
        this.coder.log.error('Error setting model schema');
      }
    }
    //

    // Handle sources
    for (const [manifestId, manifestSource] of Object.entries(
      project.manifest.sources || {},
    )) {
      if (!(manifestSource?.resource_type === 'source')) continue;
      const sourceId = manifestId;
      const sourceRef = sourceId.split('.').slice(2).join('.');
      this.framework.sourceRefs.add(sourceRef);
      const childMap = manifest.child_map[sourceId] || [];
      const pathRelativeFile = manifestSource.original_file_path || '';
      const pathRelativeDirectory = path.dirname(pathRelativeFile);
      const pathSystemDirectory = path.join(
        project.pathSystem,
        pathRelativeDirectory,
      );
      const pathSystemFile = path.join(project.pathSystem, pathRelativeFile);
      const source: DbtSource = {
        ...manifestSource,
        childMap,
        parentMap: [],
        pathRelativeDirectory,
        pathSystemDirectory,
        pathSystemFile,
      };
      this.sources.set(sourceId, source);
    }
    const sourceEnum = [...this.framework.sourceRefs.values()];
    if (sourceEnum.length) {
      // Don't overwrite if new source enums are empty, this likely means the manifest parsing failed
      try {
        const sourceRefSchema = jsonParse(
          (
            await vscode.workspace.fs.readFile(
              vscode.Uri.file(
                path.join(DJ_SCHEMAS_PATH, 'source.ref.schema.json'),
              ),
            )
          ).toString(),
        ) as FrameworkSchemaBase;
        sourceRefSchema.enum = sourceEnum;
        await vscode.workspace.fs.writeFile(
          vscode.Uri.file(path.join(DJ_SCHEMAS_PATH, 'source.ref.schema.json')),
          Buffer.from(JSON.stringify(sourceRefSchema)),
        );
      } catch (err) {
        this.coder.log.error('Error setting source schema', err);
      }
    }
    //

    this.viewProjectNavigator.setData(this.buildProjectNavigator({ project }));

    this.coder.log.info('HANDLED MANIFEST');
  }

  /**
   * Handles logic when user navigates to a model file
   */
  async handleModelNavigate(info: CoderFileInfo) {
    if (
      !(info?.type === 'framework-model' || info?.type === 'model') ||
      !info?.model
    ) {
      return;
    }

    this.viewModelActions.setData([
      this.treeItemJsonSync,
      this.treeItemProjectClean,
      this.treeItemModelCreate,
      this.treeItemSourceCreate,
      this.coder.lightdash.treeItemLightdashPreview,
      this.treeItemDeferRun,
      this.treeItemModelRun,
      this.treeItemModelRunLineage,
      this.treeItemModelCompile,
    ]);
    this.viewSelectedResource.setData(this.buildSelectedResource(info));
  }

  registerCommands(context: vscode.ExtensionContext) {
    this.coder.log.info('Dbt: Registering commands');
    this.registerModelCommands(context);
    this.registerSourceCommands(context);
    this.registerUtilityCommands(context);
    this.coder.log.info('Dbt: Commands registered successfully');
  }

  /**
   * Register model commands - Model Compile, Model Create, Model Run, Model Run Lineage
   * @param context
   */
  private registerModelCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(COMMAND_ID.MODEL_COMPILE, async () => {
        try {
          const currentPath = this.coder.getCurrentPath();
          if (!currentPath) return;

          const info = await this.coder.fetchFileInfoFromPath(currentPath);
          const model = info && 'model' in info && info?.model;
          if (model) {
            await this.coder.executeDbtCommand(
              `dbt compile --select "${model.name}"`,
              DBT_MSG.COMPILE_MODEL,
              info.project.pathSystem,
            );
          }

          try {
            const project = info && 'project' in info && info?.project;
            if (!project) return;

            switch (info?.type) {
              case 'framework-model':
              case 'model':
              case 'yml': {
                const modelName = path
                  .basename(currentPath)
                  .replace(new RegExp(`\.(?:model\.json|sql|yml)$`), '');
                const modelPath = path
                  .dirname(currentPath)
                  .replace(project.pathSystem, '');
                await this.coder.showOrOpenFile(
                  path.join(
                    project.pathSystem,
                    project.targetPath || 'target',
                    'compiled',
                    project.name,
                    modelPath,
                    modelName + '.sql',
                  ),
                  { viewColumn: vscode.ViewColumn.Beside },
                );
                break;
              }
            }
          } catch (err) {
            this.coder.log.error('ERROR JUMPING TO COMPILED: ', err);
          }
        } catch (err) {
          this.coder.log.error('ERROR COMPILING MODEL: ', err);
        }
      }),

      vscode.commands.registerCommand(COMMAND_ID.MODEL_CREATE, () => {
        if (this.webviewPanelModelCreate) {
          this.webviewPanelModelCreate.reveal();
        } else {
          const panel = vscode.window.createWebviewPanel(
            VIEW_ID.MODEL_CREATE,
            DBT_MSG.CREATE_MODEL,
            vscode.ViewColumn.One,
            { enableScripts: true },
          );

          panel.onDidDispose(() => {
            this.webviewPanelModelCreate = undefined;
          });

          this.webviewPanelModelCreate = panel;
          const html = getHtml({
            extensionUri: this.coder.context.extensionUri,
            route: '/model/create',
            webview: panel.webview,
          });
          panel.webview.html = html;

          // Handle webview messages including state management
          panel.webview.onDidReceiveMessage(
            this.coder.createWebviewMessageHandler(panel, 'model-create'),
          );
        }
      }),

      vscode.commands.registerCommand(COMMAND_ID.MODEL_RUN, async () => {
        const info = await this.coder.fetchCurrentInfo();
        if (!(info && 'model' in info && info.model)) return;

        await this.coder.executeDbtCommand(
          `dbt run --select "${info.model.name}"`,
          DBT_MSG.RUN_MODEL,
          info.project.pathSystem,
        );
      }),

      vscode.commands.registerCommand(
        COMMAND_ID.MODEL_RUN_LINEAGE,
        async () => {
          try {
            const info = await this.coder.fetchCurrentInfo();
            if (!(info && 'model' in info && info.model)) return;

            await this.coder.executeDbtCommand(
              `dbt run --select "+${info.model.name}+"`,
              DBT_MSG.RUN_MODEL_LINEAGE,
              info.project.pathSystem,
            );
          } catch (err) {
            this.coder.log.error('ERROR RUNNING MODEL LINEAGE', err);
          }
        },
      ),
    );
  }

  /**
   * Register source commands - Source Create
   * @param context
   */
  private registerSourceCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(COMMAND_ID.SOURCE_CREATE, () => {
        if (this.webviewPanelSourceCreate) {
          this.webviewPanelSourceCreate.reveal();
        } else {
          const panel = vscode.window.createWebviewPanel(
            VIEW_ID.SOURCE_CREATE,
            DBT_MSG.CREATE_SOURCE,
            vscode.ViewColumn.One,
            { enableScripts: true },
          );
          panel.onDidDispose(() => {
            this.webviewPanelSourceCreate = undefined;
          });
          this.webviewPanelSourceCreate = panel;
          const html = getHtml({
            extensionUri: this.coder.context.extensionUri,
            route: '/source/create',
            webview: panel.webview,
          });
          panel.webview.html = html;

          // Handle webview messages including state management
          panel.webview.onDidReceiveMessage(
            this.coder.createWebviewMessageHandler(panel, 'source-create'),
          );
        }
      }),
    );
  }

  /**
   * Register utility commands - Project Clean, Defer Run
   * @param context
   */
  private registerUtilityCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(COMMAND_ID.PROJECT_CLEAN, async () => {
        try {
          this.coder.log.info('CLEANING PROJECT');
          const info = await this.coder.fetchCurrentInfo();
          const project = (info && 'project' in info && info?.project) || null;

          await this.coder.executeDbtCommand(
            'dbt clean && dbt deps && dbt seed',
            DBT_MSG.CLEAN_PROJECT,
            project?.pathSystem || path.join(WORKSPACE_ROOT, DEFAULT_DBT_PATH),
          );
          this.coder.log.info('FINISHED CLEANING PROJECT');
        } catch (err) {
          this.coder.log.info('ERROR CLEANING PROJECT', err);
        }
      }),

      vscode.commands.registerCommand(COMMAND_ID.DEFER_RUN, async () => {
        try {
          this.coder.log.info('RUNNING DEFER');
          const info = await this.coder.fetchCurrentInfo();

          let project = (info && 'project' in info && info.project) || null;

          // If no project found from file context, try to find the first project
          if (!project) {
            const projects = this.coder.framework.dbt.projects.values();
            for (const _project of projects) {
              project = _project;
            }
          }
          // If no project found, throw an error
          if (!project) {
            throw new Error('No project found');
          }

          const diffModels = await this.coder.api.handleApi({
            type: 'dbt-fetch-modified-models',
            request: { projectName: project.name },
          });
          if (!diffModels || diffModels.length === 0) {
            vscode.window.showWarningMessage('No model changes from master');
            return;
          }

          // Build the select string for the defer run
          let select = '';
          for (const model of diffModels) {
            select += (select ? ' ' : '') + `${model}+`;
          }

          await this.coder.executeDbtCommand(
            `dbt run --select "${select}" --defer --state ${project.pathSystem}/prod_state`,
            'Run Defer',
            project.pathSystem,
          );
        } catch (err) {
          this.coder.log.info('ERROR RUNNING DEFER', err);
        }
      }),
    );
  }

  // Dispose Source Create Webview Panel
  disposeWebviewPanelSourceCreate() {
    this.webviewPanelSourceCreate?.dispose();
  }

  // Dispose Model Create Webview Panel
  disposeWebviewPanelModelCreate() {
    this.webviewPanelModelCreate?.dispose();
  }

  /**
   * Clear cached data to free up memory
   */
  clearCache() {
    // Clear all maps to free up memory
    this.macros.clear();
    this.models.clear();
    this.seeds.clear();
    this.sources.clear();

    // Clear project manifests to save memory
    for (const project of this.projects.values()) {
      project.manifest = {
        child_map: {},
        disabled: {},
        docs: {},
        exposures: {},
        group_map: {},
        groups: {},
        macros: {},
        metadata: {},
        metrics: {},
        nodes: {},
        parent_map: {},
        saved_queries: {},
        selectors: {},
        semantic_models: {},
        sources: {},
      };
    }
  }

  // Any cleanup that needs to happen when the extension is deactivated would go here
  deactivate() {
    this.disposeWebviewPanelSourceCreate();
    this.disposeWebviewPanelModelCreate();
    this.clearCache();
  }
}
