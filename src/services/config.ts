import { CoderConfig } from '@shared/coder/types';
import {
  DEFAULT_AIRFLOW_DAGS_PATH,
  DEFAULT_DBT_MACRO_PATH,
} from '@shared/constants';
import { FRAMEWORK_JSON_SYNC_EXCLUDE_PATHS } from '@shared/framework/constants';
import * as vscode from 'vscode';

/**
 * Update VSCode JSON schema associations in settings.
 * @param schemas Array of schema associations, each with fileMatch and url.
 * Example:
 *   updateVSCodeJsonSchemas([
 *     { fileMatch: ['*.model.json'], url: '.dj/schemas/model.schema.json' },
 *     { fileMatch: ['*.source.json'], url: '.dj/schemas/source.schema.json' }
 *   ]);
 */
export function updateVSCodeJsonSchemas(
  schemas: { fileMatch: string[]; url: string }[],
) {
  return vscode.workspace.getConfiguration('json').update('schemas', schemas);
}

/**
 * Get the complete DJ configuration object
 * @returns Complete configuration object or default values in test environment
 */
export function getDjConfig(): CoderConfig {
  const config = vscode.workspace.getConfiguration('dj') || {};

  return {
    airflowTargetVersion: config.get('airflowTargetVersion') || undefined,
    airflowGenerateDags: config.get('airflowGenerateDags') || false,
    airflowDagsPath: config.get('airflowDagsPath') || DEFAULT_AIRFLOW_DAGS_PATH,
    dbtProjectNames: config.get('dbtProjectNames') || undefined,
    dbtMacroPath: config.get('dbtMacroPath') || DEFAULT_DBT_MACRO_PATH,
    pythonVenvPath: config.get('pythonVenvPath') || undefined,
    trinoPath: config.get('trinoPath') || undefined,
    lightdashProjectPath: config.get('lightdashProjectPath') || undefined,
    lightdashProfilesPath: config.get('lightdashProfilesPath') || undefined,
  };
}

/**
 * Get configured dbt project names from VS Code settings
 * @returns Array of project names or null if none configured
 */
export function getConfigDbtProjectNames(): string[] | null {
  const { dbtProjectNames } = getDjConfig();

  if (Array.isArray(dbtProjectNames) && dbtProjectNames.length > 0) {
    return dbtProjectNames;
  }

  return null;
}

/**
 * Get dynamic exclude paths for dbt_project.yml file searches, including configured Python venv path
 * @returns Glob pattern string for excluding paths
 */
export function getDbtProjectExcludePaths(): string {
  const excludePatterns = [...FRAMEWORK_JSON_SYNC_EXCLUDE_PATHS];

  const { pythonVenvPath } = getDjConfig();

  // Add custom venv path if configured
  if (pythonVenvPath) {
    // Remove leading/trailing slashes and ensure proper glob pattern
    const cleanPath = pythonVenvPath.replace(/^\/+|\/+$/g, '');
    if (cleanPath) {
      // Add both relative and absolute path patterns
      excludePatterns.push(`**/${cleanPath}/**`);
      if (!cleanPath.includes('*')) {
        excludePatterns.push(`${cleanPath}/**`);
      }
    }
  }

  return `{${excludePatterns.join(',')}}`;
}

/**
 * Check if a project name is allowed based on current configuration
 * @param projectName The name of the dbt project to check
 * @returns true if the project name is configured or no dbt project names are set, false otherwise
 */
export function isDbtProjectNameConfigured(projectName: string): boolean {
  const dbtProjectNames = getConfigDbtProjectNames();

  // If no dbt project names are set, all projects are allowed
  if (!dbtProjectNames) {
    return true;
  }

  return dbtProjectNames.includes(projectName);
}
