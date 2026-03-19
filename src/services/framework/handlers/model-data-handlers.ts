/**
 * Model Data Handlers - Model data retrieval and validation
 *
 * Handles:
 * - Getting current model edit data
 * - Getting model data by name
 * - Getting original model files (JSON, SQL, YAML)
 * - Checking if model exists
 */

import {
  frameworkGetModelName,
  frameworkGetModelPrefix,
} from '@services/framework/utils';
import type { ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import { getDbtModelId } from '@shared/dbt/utils';
import type { FrameworkModel } from '@shared/framework/types';
import * as fs from 'fs';
import * as vscode from 'vscode';

import type { FrameworkContext } from '../context';

export class ModelDataHandlers {
  constructor(private readonly ctx: FrameworkContext) {}

  /**
   * Get the most recent model edit draft data
   */
  async handleGetCurrentModelData(
    _payload: ApiPayload<'framework'> & {
      type: 'framework-get-current-model-data';
    },
  ): Promise<ApiResponse> {
    try {
      // Get all available edit drafts
      const editDrafts = await this.ctx.stateManager.getEditDrafts();

      if (editDrafts.length === 0) {
        return apiResponse<'framework-get-current-model-data'>({
          modelData: {},
          editFormType: '',
          isEditMode: false,
        });
      }

      // Get the most recent edit draft (they're sorted by lastModified desc)
      const mostRecentDraft = editDrafts[0];
      const { formType } = mostRecentDraft;

      // Load the temp data for this model
      const modelData = await this.ctx.stateManager.getFormState(formType);

      if (modelData) {
        return apiResponse<'framework-get-current-model-data'>({
          modelData: modelData,
          editFormType: formType,
          isEditMode: true,
        });
      }

      // Fallback if no data found
      return apiResponse<'framework-get-current-model-data'>({
        modelData: {},
        editFormType: '',
        isEditMode: false,
      });
    } catch (error: unknown) {
      this.ctx.log.warn('Error getting current model data:', error);
      return apiResponse<'framework-get-current-model-data'>({
        modelData: {},
        editFormType: '',
        isEditMode: false,
      });
    }
  }

  /**
   * Get model data by model name (searches all projects)
   */
  async handleGetModelData(
    payload: ApiPayload<'framework'> & { type: 'framework-get-model-data' },
  ): Promise<ApiResponse> {
    try {
      const modelName = payload.request.modelName;
      if (!modelName) {
        return apiResponse<typeof payload.type>(null);
      }

      // Find model in all projects
      for (const [projectName, project] of this.ctx.dbt.projects) {
        const modelId = getDbtModelId({ modelName, projectName });
        const model = this.ctx.dbt.models.get(modelId || '');

        if (model) {
          // Try to get the model.json file
          const modelJsonPath = model.pathSystemFile.replace(
            '.sql',
            '.model.json',
          );
          try {
            const modelJson = await this.ctx.fetchModelJson(
              vscode.Uri.file(modelJsonPath),
            );
            if (modelJson) {
              const enrichedModelData = {
                ...modelJson,
                projectName: project.name,
                originalModelPath: modelJsonPath,
              };
              return apiResponse<typeof payload.type>(
                enrichedModelData as unknown as FrameworkModel,
              );
            }
          } catch (err: unknown) {
            this.ctx.log.warn(
              `Could not fetch model JSON for ${modelName}:`,
              err,
            );
          }
        }
      }

      return apiResponse<typeof payload.type>(null);
    } catch (err: unknown) {
      this.ctx.log.error('Error fetching model data:', err);
      return apiResponse<typeof payload.type>(null);
    }
  }

  /**
   * Read original model files (JSON, SQL, YAML) for comparison/preview
   */
  async handleGetOriginalModelFiles(
    payload: ApiPayload<'framework'> & {
      type: 'framework-get-original-model-files';
    },
  ): Promise<ApiResponse> {
    const { originalModelPath } = payload.request;

    try {
      // Read the original .model.json file
      const jsonUri = vscode.Uri.file(originalModelPath);
      const jsonFile = await vscode.workspace.fs.readFile(jsonUri);
      const jsonContent = jsonFile.toString();

      // Derive SQL and YAML file paths from the JSON path
      const sqlPath = originalModelPath.replace('.model.json', '.sql');
      const yamlPath = originalModelPath.replace('.model.json', '.yml');

      // Read SQL file
      let sqlContent = '';
      try {
        const sqlUri = vscode.Uri.file(sqlPath);
        const sqlFile = await vscode.workspace.fs.readFile(sqlUri);
        sqlContent = sqlFile.toString();
      } catch (error: unknown) {
        this.ctx.log.warn(`Could not read SQL file at ${sqlPath}:`, error);
        sqlContent = '-- Original SQL file not found';
      }

      // Read YAML file
      let yamlContent = '';
      try {
        const yamlUri = vscode.Uri.file(yamlPath);
        const yamlFile = await vscode.workspace.fs.readFile(yamlUri);
        yamlContent = yamlFile.toString();
      } catch (error: unknown) {
        this.ctx.log.warn(`Could not read YAML file at ${yamlPath}:`, error);
        yamlContent = '# Original YAML file not found';
      }

      return apiResponse<typeof payload.type>({
        json: jsonContent,
        sql: sqlContent,
        yaml: yamlContent,
      });
    } catch (error: unknown) {
      this.ctx.log.error('Error reading original model files:', error);
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      return apiResponse<typeof payload.type>({
        json: `// Error reading original JSON:\n// ${errorMessage}`,
        sql: `-- Error reading original SQL:\n// ${errorMessage}`,
        yaml: `# Error reading original YAML:\n# ${errorMessage}`,
      });
    }
  }

  /**
   * Check if a model file already exists (prevents duplicate creation)
   */
  handleCheckModelExists(
    payload: ApiPayload<'framework'> & { type: 'framework-check-model-exists' },
  ): Promise<ApiResponse> {
    const { projectName, modelJson } = payload.request;

    // Get the project
    const project = this.ctx.dbt.projects.get(projectName);
    if (!project) {
      throw new Error('Project not found');
    }

    // Generate the file name using existing utility functions
    const fileName = frameworkGetModelName(modelJson);
    const modelPrefix = frameworkGetModelPrefix({
      modelJson,
      project,
    });

    // Check if file exists
    let exists = false;
    let filePath = '';

    if (modelPrefix && fileName) {
      filePath = `${modelPrefix}.model.json`;
      exists = fs.existsSync(filePath);
    }

    return Promise.resolve(
      apiResponse<typeof payload.type>({
        exists,
        fileName: fileName ? `${fileName}.model.json` : '',
        filePath,
      }),
    );
  }
}
