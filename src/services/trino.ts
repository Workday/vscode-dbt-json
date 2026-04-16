import type { Coder } from '@services/coder';
import { COMMAND_ID } from '@services/constants';
import type { ApiEnabledService } from '@services/types';
import { buildProcessEnv } from '@services/utils/process';
import { getHtml } from '@services/webview/utils';
import { assertExhaustive } from '@shared';
import type { ApiMessage, ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import type { FrameworkEtlSource } from '@shared/framework/types';
import { QUERY_NOT_AVAILABLE } from '@shared/trino/constants';
import type {
  TrinoApi,
  TrinoSystemNode,
  TrinoSystemQuery,
  TrinoSystemQueryWithTask,
  TrinoTable,
  TrinoTableColumn,
} from '@shared/trino/types';
import {
  djSqlPath,
  djSqlWrite,
  getTrinoConfig,
  TreeDataInstance,
  WORKSPACE_ROOT,
} from 'admin';
import { spawn } from 'child_process';
import * as vscode from 'vscode';

const POLLING_INTERVAL_SYSTEM_INFO = 60000; // 60 seconds

export class Trino implements ApiEnabledService<'trino'> {
  coder: Coder;
  currentSchema: string | null = null;
  handleApi: (payload: ApiPayload<'trino'>) => Promise<ApiResponse>;
  systemNodes: TrinoSystemNode[] | null = null;
  tables = new Map<string, TrinoTable>();
  timeoutSystemInfo: NodeJS.Timeout | null = null;
  viewQueryEngine: TreeDataInstance;

  constructor({ coder }: { coder: Coder }) {
    this.coder = coder;

    this.handleApi = async (payload) => {
      switch (payload.type) {
        case 'trino-fetch-catalogs': {
          const catalogsRaw = await this.handleQuery(`show catalogs`);
          const catalogs = catalogsRaw.map((r) => r['Catalog']);
          return apiResponse<typeof payload.type>(catalogs);
        }
        case 'trino-fetch-columns': {
          const { catalog, schema, table } = payload.request;
          const columnsRaw = await this.handleQuery(
            `show columns from "${catalog}"."${schema}"."${table}"`,
          );
          const columns: TrinoTableColumn[] = columnsRaw.map((r) => {
            return {
              column: r['Column'],
              type: r['Type'],
              extra: r['Extra'],
              comment: r['Comment'] || '',
            };
          });
          return apiResponse<typeof payload.type>(columns);
        }
        case 'trino-fetch-current-schema': {
          const result = await this.handleQuery(
            `select current_schema as schema`,
          );
          const currentSchema: string = result[0]?.['schema'] ?? '';
          return apiResponse<typeof payload.type>(currentSchema);
        }
        case 'trino-fetch-etl-sources': {
          const { projectName, etlSchema } = payload.request;
          const schemaName = etlSchema || 'source_etl';

          const etlSourcesRaw = await this.handleQuery(
            `select source_id, properties, etl_active from ${projectName}.${schemaName}.dbt_sources`,
          );
          const etlSources: FrameworkEtlSource[] = etlSourcesRaw.map((r) => {
            return {
              etl_active: r['etl_active'],
              properties: r['properties'],
              source_id: r['source_id'],
            };
          });
          return apiResponse<typeof payload.type>(etlSources);
        }
        case 'trino-fetch-schemas': {
          const { catalog } = payload.request;
          const schemasRaw = await this.handleQuery(
            `show schemas from ${catalog}`,
          );
          const schemas = schemasRaw.map((r) => r['Schema']);
          return apiResponse<typeof payload.type>(schemas);
        }
        case 'trino-fetch-system-nodes': {
          const nodesRaw = await this.handleQuery(
            'select * from system.runtime.nodes',
          );
          const nodes: TrinoSystemNode[] = nodesRaw.map((r) => {
            return {
              coordinator: Boolean(r['coordinator']),
              http_uri: r['http_uri'],
              node_id: r['node_id'],
              node_version: Number(r['node_version']),
              state: r['state'],
            };
          });
          return apiResponse<typeof payload.type>(nodes);
        }
        case 'trino-fetch-system-queries': {
          const { schema } = payload.request;
          let sql = `
select
  "created",
  "end",
  "query_id",
  "source",
  "started",
  "state"
from
  system.runtime.queries
where
  source like 'dbt-trino-%'`;
          if (schema) {
            sql += `
  and (query like '%."${schema}".%' or query like '%"schema": "${schema}"%')`;
          }
          sql += `
order by created desc;`;
          const queriesRaw = await this.handleQuery(sql, {
            filename: payload.type,
          });
          const queries: TrinoSystemQuery[] = queriesRaw.map((r) => {
            return {
              // analysis_time_ms: Number(r['analysis_time_ms']),
              created: r['created'],
              end: r['end'],
              // error_code: r['error_code'],
              // error_type: r['error_type'],
              // last_heartbeat: r['last_heartbeat'],
              // planning_time_ms: Number(r['planning_time_ms']),
              // queued_time_ms: Number(r['queued_time_ms']),
              // query: r['query'],
              query_id: r['query_id'],
              // resource_group_id: r['resource_group_id'],
              source: r['source'],
              started: r['started'],
              state: r['state'],
              // user: r['user'],
            };
          });
          return apiResponse<typeof payload.type>(queries);
        }
        case 'trino-fetch-system-query-with-task': {
          const sql = `
select
  t."completed_splits",
  q."created",
  q."end",
  q."query_id",
  t."running_splits",
  q."source",
  t."splits",
  q."started",
  q."state",
  t."queued_splits"
from
  system.runtime.queries q left join system.runtime.tasks t on q.query_id = t.query_id
where
  q.query_id = '${payload.request.id}';`;
          // We are handling this as a non json output because of this trino issue: https://github.com/trinodb/trino/issues/18525
          const queryRaw = await this.handleQuery(sql, {
            filename: payload.type,
          });
          const r = queryRaw[0];
          if (!r) {
            throw new Error(QUERY_NOT_AVAILABLE);
          }
          const queryWithTask = r as TrinoSystemQueryWithTask;
          return apiResponse<typeof payload.type>(queryWithTask);
        }
        case 'trino-fetch-system-query-sql': {
          this.coder.log.info('Fetching system query', payload.request.id);
          // We are handling this as a non json output because of this trino issue: https://github.com/trinodb/trino/issues/18525
          const querySqlRaw = await this.handleQuery(
            `select "query" from system.runtime.queries where query_id = '${payload.request.id}';`,
            {
              raw: true,
              filename: payload.type,
            },
          );
          const querySql = querySqlRaw;
          //
          this.coder.log.info('Fetched system query', querySql);
          return apiResponse<typeof payload.type>(querySql);
        }
        case 'trino-fetch-tables': {
          const { catalog, schema } = payload.request;
          const tablesRaw = await this.handleQuery(
            `show tables from ${catalog}.${schema}`,
          );
          const tables = tablesRaw.map((r) => r['Table']);
          return apiResponse<typeof payload.type>(tables);
        }
        default:
          return assertExhaustive<ApiResponse>(payload);
      }
    };

    this.viewQueryEngine = new TreeDataInstance([
      { label: 'Extension loading...' },
    ]);
  }

  activate(context: vscode.ExtensionContext): void {
    // Don't await this so we don't block the extension from loading
    // Failures will be caught in handleSystemInfo
    void this.handleSystemInfo();

    // Register commands
    this.registerCommands(context);
  }

  /**
   * Register Trino-specific commands - Query View, Test Trino Connection
   * @param context
   */
  registerCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(
        COMMAND_ID.QUERY_VIEW,
        (queryId: string) => {
          const panel = vscode.window.createWebviewPanel(
            `dj_query_view_${queryId}`,
            `Query View: ${queryId}`,
            vscode.ViewColumn.One,
            { enableFindWidget: true, enableScripts: true },
          );
          panel.webview.html = getHtml({
            extensionUri: context.extensionUri,
            route: `/query/view/${queryId}`,
            webview: panel.webview,
          });
          panel.webview.onDidReceiveMessage(async (message: ApiMessage) =>
            this.coder.handleWebviewMessage({
              message,
              webview: panel.webview,
            }),
          );
        },
      ),

      vscode.commands.registerCommand(
        COMMAND_ID.TEST_TRINO_CONNECTION,
        async () => {
          try {
            this.coder.log.info('Testing Trino connection...');

            const trinoConfig = getTrinoConfig();
            this.coder.log.info('Trino configuration:', trinoConfig);

            const result = await this.coder.trino.handleQuery(
              'SELECT 1 as test',
              {
                raw: true,
              },
            );

            vscode.window.showInformationMessage(
              `✅ Trino connection successful! Using: ${trinoConfig.path}`,
            );
            this.coder.log.info('Trino connection test result:', result);
          } catch (error: unknown) {
            const errorMessage =
              error instanceof Error ? error.message : String(error);
            vscode.window.showErrorMessage(
              `❌ Trino connection failed: ${errorMessage}`,
            );
            this.coder.log.error('Trino connection test failed:', error);
          }
        },
      ),
    );
  }

  /**
   * Execute Trino query with file or direct execution
   */
  handleQuery(
    sql: string,
    options?: {
      raw?: false;
      filename?: TrinoApi['type'] | 'data-explorer-query.sql';
    },
  ): Promise<Record<string, any>[]>;
  handleQuery(
    sql: string,
    options?: {
      raw: true;
      filename?: TrinoApi['type'] | 'data-explorer-query.sql';
    },
  ): Promise<string>;
  async handleQuery(
    sql: string,
    options?: {
      raw?: boolean;
      filename?: TrinoApi['type'] | 'data-explorer-query.sql';
    },
  ): Promise<Record<string, any>[] | string> {
    const { path: trinoCommand } = getTrinoConfig();
    const { filename } = options ?? {};

    // Log SQL for debugging
    this.coder.log.info(`[Trino] SQL: ${sql}`);

    let command = trinoCommand;
    if (filename) {
      // Use file-based execution when filename is specified
      const filepath = djSqlPath({ name: filename });
      // Ensure SQL ends with semicolon for file-based execution
      const sqlWithSemicolon = sql.trim().endsWith(';')
        ? sql
        : `${sql.trim()};`;
      djSqlWrite({ name: filename, sql: sqlWithSemicolon });
      command += ` --file '${filepath}'`;
      this.coder.log.info(`[Trino] Using --file: ${filepath}`);
    } else {
      // Use --execute for direct queries (single quotes preserve double quotes in SQL)
      command += ` --execute '${sql}'`;
      this.coder.log.info(`[Trino] Using --execute`);
    }

    if (!options?.raw) {
      // Use CSV_HEADER format instead of JSON to properly handle complex types (arrays, maps, structs)
      // The Trino CLI's JSON format has a known limitation where it cannot serialize
      // java.util.Collections$UnmodifiableList (arrays) without a proper ObjectCodec
      command += ` --output-format=CSV_HEADER`;
    }

    this.coder.log.info(`[Trino] Command: ${command}`);

    try {
      const result = await this.executeTrinoCommand(command);

      if (options?.raw) {
        return result;
      }

      return this.parseCsvOutput(result);
    } catch (error: unknown) {
      this.coder.log.error('Trino query failed:', error);

      // Check for command not found error
      const errorString = String(error);
      if (errorString.includes('command not found')) {
        if (trinoCommand) {
          throw new Error(
            `Trino CLI (trino-cli) not found at configured path: ${trinoCommand}. Please verify the path is correct and the file is executable. You can update this in VS Code Settings under "DJ > Trino Path".`,
          );
        } else {
          throw new Error(
            `Trino CLI (trino-cli) not found in PATH. Please configure the full path to your Trino executable in VS Code Settings under "DJ > Trino Path" (e.g., /usr/local/bin).`,
          );
        }
      }

      // Parse and format Trino error for better readability
      const trinoError = this.parseTrinoError(error);
      throw new Error(trinoError);
    }
  }

  /**
   * Execute Trino command with proper environment setup (venv support)
   */
  private executeTrinoCommand(command: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const env = buildProcessEnv();

      const childProcess = spawn(command, {
        cwd: WORKSPACE_ROOT,
        env,
        shell: true,
      });

      let stdout = '';
      let stderr = '';

      childProcess.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      childProcess.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      childProcess.on('exit', (code) => {
        if (code === 0) {
          resolve(stdout);
        } else {
          reject(new Error(stderr || `Process exited with code ${code}`));
        }
      });

      childProcess.on('error', (error) => {
        reject(error);
      });
    });
  }

  /**
   * Parse CSV output from Trino CLI into an array of objects
   * Handles complex types like arrays, maps, and structs that are represented as strings
   */
  private parseCsvOutput(csvData: string): Record<string, any>[] {
    const lines = csvData.split('\n').filter(Boolean);
    if (lines.length === 0) {
      return [];
    }

    // Parse header row
    const headers = this.parseCsvLine(lines[0]);

    // Parse data rows
    const results: Record<string, any>[] = [];
    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCsvLine(lines[i]);
      const row: Record<string, any> = {};

      for (let j = 0; j < headers.length; j++) {
        const value = values[j];
        row[headers[j]] = this.parseTrinoValue(value);
      }

      results.push(row);
    }

    return results;
  }

  /**
   * Parse a single CSV line, handling quoted fields and embedded commas
   */
  private parseCsvLine(line: string): string[] {
    const result: string[] = [];
    let current = '';
    let inQuotes = false;
    let i = 0;

    while (i < line.length) {
      const char = line[i];

      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          // Escaped quote
          current += '"';
          i += 2;
          continue;
        }
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        result.push(current);
        current = '';
      } else {
        current += char;
      }
      i++;
    }

    // Add the last field
    result.push(current);

    return result;
  }

  /**
   * Parse a Trino value from CSV string representation
   * Handles arrays, maps, structs, nulls, and primitive types
   */
  private parseTrinoValue(value: string | undefined): any {
    if (value === undefined || value === '' || value === 'NULL') {
      return null;
    }

    const trimmed = value.trim();

    // Handle NULL values
    if (trimmed.toUpperCase() === 'NULL') {
      return null;
    }

    // Try to parse as JSON for arrays and maps
    if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
      try {
        // First try standard JSON parsing
        return JSON.parse(trimmed);
      } catch {
        // Trino may output maps/structs with = instead of : (e.g., {key=value})
        // Try to convert Trino format to JSON format
        try {
          const jsonified = this.trinoToJson(trimmed);
          return JSON.parse(jsonified);
        } catch {
          // If all parsing fails, return as string (the frontend can still display it)
          return trimmed;
        }
      }
    }

    // Try to parse as number
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
      const num = parseFloat(trimmed);
      if (!isNaN(num)) {
        return num;
      }
    }

    // Try to parse as boolean
    if (trimmed.toLowerCase() === 'true') {
      return true;
    }
    if (trimmed.toLowerCase() === 'false') {
      return false;
    }

    // Return as string
    return trimmed;
  }

  /**
   * Convert Trino's map/struct format to JSON format
   * Trino outputs maps as {key=value, key2=value2} instead of JSON's {"key": "value"}
   */
  private trinoToJson(trinoStr: string): string {
    // This is a best-effort conversion for simple cases
    // For complex nested structures, it may not work perfectly
    let result = trinoStr;

    // Replace = with : for key-value pairs, but be careful with values containing =
    // Match pattern: word= or "word"= at the start of a key-value pair
    result = result.replace(/(\{|, )([a-zA-Z_][a-zA-Z0-9_]*)=/g, '$1"$2":');

    // Handle unquoted string values (simple alphanumeric values)
    // This is tricky because we don't want to double-quote already quoted strings or numbers
    result = result.replace(/:([a-zA-Z][a-zA-Z0-9_]*)(,|})/g, ':"$1"$2');

    return result;
  }

  /**
   * Parse Trino CLI error messages to extract meaningful information
   */
  private parseTrinoError(error: unknown): string {
    const errorStr = error instanceof Error ? error.message : String(error);

    // Remove ANSI color codes
    const cleaned = errorStr.replace(/\x1b\[[0-9;]*m/g, '');

    // Extract meaningful error lines
    const lines = cleaned
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .filter((line) => !line.includes('Error running command'))
      .filter((line) => !line.startsWith('at ')) // Remove stack traces
      .filter((line) => !line.includes('node:internal')); // Remove Node.js internals

    // Return formatted error or original if parsing fails
    return lines.length > 0 ? lines.join('\n') : errorStr;
  }

  async handleSystemInfo() {
    try {
      this.currentSchema ??= await this.coder.api.handleApi({
        type: 'trino-fetch-current-schema',
        request: null,
      });

      this.systemNodes = await this.coder.api.handleApi({
        type: 'trino-fetch-system-nodes',
        request: null,
      });

      const queryNodes = this.systemNodes?.filter((n) => !n.coordinator) ?? [];
      const queries = await this.coder.api.handleApi({
        type: 'trino-fetch-system-queries',
        request: { schema: this.currentSchema },
      });
      this.viewQueryEngine.setData([
        { label: 'Trino' },
        {
          label: 'Nodes',
          description: String(queryNodes.length),
          children:
            queryNodes.map((n) => ({
              label: n.node_id,
              iconPath:
                n.state === 'active'
                  ? new vscode.ThemeIcon('pass-filled')
                  : new vscode.ThemeIcon('error'),
            })) ?? [],
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
        },
        {
          label: 'My Queries',
          description: String(queries.length),
          children:
            queries.map((q) => ({
              label: q.state,
              description: q.query_id,
              iconPath:
                q.state === 'FINISHED'
                  ? new vscode.ThemeIcon('pass-filled')
                  : q.state === 'RUNNING'
                    ? new vscode.ThemeIcon('sync')
                    : q.state === 'QUEUED'
                      ? new vscode.ThemeIcon('circle-large')
                      : q.state === 'FAILED'
                        ? new vscode.ThemeIcon('error')
                        : undefined,
              command: {
                title: 'View Query',
                command: COMMAND_ID.QUERY_VIEW,
                arguments: [q.query_id],
              },
            })) ?? [],
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
        },
      ]);
    } catch (err: unknown) {
      this.coder.log.error('Error fetching query engine info', err);
    }
    this.timeoutSystemInfo = setTimeout(
      () => void this.handleSystemInfo(),
      POLLING_INTERVAL_SYSTEM_INFO,
    );
  }

  deactivate() {
    if (this.timeoutSystemInfo) {
      clearTimeout(this.timeoutSystemInfo);
    }
  }
}
