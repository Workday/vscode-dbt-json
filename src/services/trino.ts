import { Coder } from '@services/coder';
import { DJService } from '@services/types';
import { assertExhaustive, jsonParse } from '@shared';
import { ApiMessage, ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import { COMMAND_ID } from '@shared/constants';
import { FrameworkEtlSource } from '@shared/framework/types';
import { QUERY_NOT_AVAILABLE } from '@shared/trino/constants';
import {
  TrinoApi,
  TrinoSystemNode,
  TrinoSystemQuery,
  TrinoSystemQueryWithTask,
  TrinoTable,
  TrinoTableColumn,
} from '@shared/trino/types';
import { getHtml } from '@shared/web/utils';
import { djSqlPath, djSqlWrite, getTrinoConfig, TreeDataInstance } from 'admin';
import * as vscode from 'vscode';

const POLLING_INTERVAL_SYSTEM_INFO = 60000; // 60 seconds

export class Trino implements DJService {
  coder: Coder;
  currentSchema: string | null = null;
  handleApi: (payload: ApiPayload<'trino'>) => Promise<ApiResponse>;
  systemNodes: TrinoSystemNode[] | null = null;
  systemQueries: TrinoSystemQuery[] | null = null;
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
              comment: r['Comment'],
            };
          });
          return apiResponse<typeof payload.type>(columns);
        }
        case 'trino-fetch-current-schema': {
          const result = await this.handleQuery(
            `select current_schema as schema`,
          );
          const currentSchema: string = result[0]?.['schema'] || '';
          return apiResponse<typeof payload.type>(currentSchema);
        }
        case 'trino-fetch-etl-sources': {
          const { projectName } = payload.request;

          const etlSourcesRaw = await this.handleQuery(
            `select source_id, properties, etl_active from ${projectName}.source_etl.dbt_sources`,
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
          let sql = `
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

  async activate(context: vscode.ExtensionContext) {
    // Don't await this so we don't block the extension from loading
    // Failures will be caught in handleSystemInfo
    this.handleSystemInfo();

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
        async (queryId: string) => {
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
          } catch (error) {
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

  handleQuery(
    sql: string,
    options?: { raw?: false; filename?: TrinoApi['type'] },
  ): Promise<Record<string, any>[]>;
  handleQuery(
    sql: string,
    options?: { raw: true; filename?: TrinoApi['type'] },
  ): Promise<string>;
  async handleQuery(
    sql: string,
    options?: { raw?: boolean; filename?: TrinoApi['type'] },
  ): Promise<Record<string, any>[] | string> {
    const { path: trinoCommand } = getTrinoConfig();
    const { filename, raw = false } = options || {};

    // Log the command being used for debugging
    this.coder.log.debug(`Using Trino command: ${trinoCommand}`);

    let command = trinoCommand;
    if (filename) {
      const filepath = djSqlPath({ name: filename });
      djSqlWrite({ name: filename, sql });
      command += ` --file '${filepath}'`;
    } else {
      command += ` --execute '${sql}'`;
    }

    try {
      if (options?.raw) {
        const result = await this.coder.runProcess({ command });
        return result;
      }

      command += ` --output-format=JSON`;
      const result = await this.coder.runProcess({ command });
      return result
        .split('\n')
        .filter(Boolean)
        .map((r) => jsonParse(r));
    } catch (error: any) {
      this.coder.log.error('Trino query failed:', error);
      if (error.toString().includes('command not found')) {
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
      throw error;
    }
  }

  async handleSystemInfo() {
    try {
      if (!this.currentSchema) {
        this.currentSchema = await this.coder.api.handleApi({
          type: 'trino-fetch-current-schema',
          request: null,
        });
      }
      const nodes = await this.coder.api.handleApi({
        type: 'trino-fetch-system-nodes',
        request: null,
      });
      this.systemNodes = nodes;
      const queryNodes = nodes.filter((n) => !n.coordinator);
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
            })) || [],
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
            })) || [],
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
        },
      ]);
    } catch (err) {
      this.coder.log.error('Error fetching query engine info', err);
    }
    this.timeoutSystemInfo = setTimeout(
      () => void this.handleSystemInfo(),
      POLLING_INTERVAL_SYSTEM_INFO,
    );
  }

  deactivate() {
    if (this.timeoutSystemInfo) clearTimeout(this.timeoutSystemInfo);
  }
}
