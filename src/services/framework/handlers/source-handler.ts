import { frameworkMakeSourcePrefix } from '@services/framework/utils';
import { jsonParse } from '@shared';
import type { ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import type {
  FrameworkDataType,
  FrameworkSource,
} from '@shared/framework/types';
import * as _ from 'lodash';
import * as vscode from 'vscode';

import type { FrameworkContext } from '../context';

/**
 * Handles API requests for creating Framework source files.
 *
 * Sources represent database tables from Trino that can be used as inputs to models.
 * This handler fetches column metadata from Trino and creates/updates source.json files.
 */
export class SourceHandler {
  constructor(private readonly ctx: FrameworkContext) {}

  /**
   * Creates or updates a source.json file with a new table definition.
   *
   * Flow:
   * 1. Validate project exists
   * 2. Fetch columns from Trino for the specified table
   * 3. Calculate source file path based on catalog/schema
   * 4. Read existing source.json if it exists (merge with new table)
   * 5. Validate table doesn't already exist
   * 6. Add new table with column definitions
   * 7. Sort tables by name
   * 8. Write updated source.json
   * 9. Clear form state
   * 10. Open file in editor
   */
  async handleSourceCreate(
    payload: Extract<
      ApiPayload<'framework'>,
      { type: 'framework-source-create' }
    >,
  ): Promise<ApiResponse> {
    const { projectName, trinoCatalog, trinoSchema, trinoTable } =
      payload.request;
    const project = this.ctx.dbt.projects.get(projectName);
    if (!project) {
      throw new Error('Project not found');
    }

    const trinoColumnsResponse = await this.ctx.api.handleApi({
      type: 'trino-fetch-columns',
      request: {
        catalog: trinoCatalog,
        schema: trinoSchema,
        table: trinoTable,
      },
    });

    // Type guard: ensure we have columns array
    if (!trinoColumnsResponse || !Array.isArray(trinoColumnsResponse)) {
      throw new Error('Failed to fetch Trino columns');
    }

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
    } catch {
      // File doesn't exist, will create new one
    }
    if (tables.find((t) => t.name === trinoTable)) {
      // We're throwing this outside the try/catch to avoid catching the error
      throw new Error('Source table already exists');
    }
    tables.push({
      name: trinoTable,
      columns: trinoColumnsResponse.map((c: any) => ({
        name: c.column,
        data_type: c.type as FrameworkDataType,
        description: c.comment || '',
      })),
    });
    _.sortBy(tables, ['name']);
    await vscode.workspace.fs.writeFile(
      sourceJsonUri,
      Buffer.from(JSON.stringify({ ...newSourceJson, tables }, null, '    ')),
    );

    try {
      await this.ctx.api.handleApi({
        type: 'state-clear',
        request: { formType: 'source-create' },
      });
    } catch (error: unknown) {
      this.ctx.log.warn('Failed to clear source create form state:', error);
    }

    this.ctx.dbt.disposeWebviewPanelSourceCreate();
    vscode.window.showTextDocument(sourceJsonUri);

    return apiResponse<typeof payload.type>('Source created');
  }
}
