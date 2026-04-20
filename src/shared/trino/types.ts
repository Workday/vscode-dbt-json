import type { FrameworkEtlSource } from '@shared/framework/types';

export type TrinoApi =
  | {
      type: 'trino-fetch-catalogs';
      service: 'trino';
      request: null;
      response: string[];
    }
  | {
      type: 'trino-fetch-columns';
      service: 'trino';
      request: { catalog: string; schema: string; table: string };
      response: TrinoTableColumn[];
    }
  | {
      type: 'trino-fetch-current-schema';
      service: 'trino';
      request: null;
      response: string;
    }
  | {
      type: 'trino-fetch-etl-sources';
      service: 'trino';
      request: { projectName: string; etlSchema?: string };
      response: FrameworkEtlSource[];
    }
  | {
      type: 'trino-fetch-schemas';
      service: 'trino';
      request: { catalog: string };
      response: string[];
    }
  | {
      type: 'trino-fetch-system-nodes';
      service: 'trino';
      request: null;
      response: TrinoSystemNode[];
    }
  | {
      type: 'trino-fetch-system-queries';
      service: 'trino';
      request: { schema?: string };
      response: TrinoSystemQuery[];
    }
  | {
      type: 'trino-fetch-system-query-with-task';
      service: 'trino';
      request: { id: string };
      response: TrinoSystemQueryWithTask;
    }
  | {
      type: 'trino-fetch-system-query-sql';
      service: 'trino';
      request: { id: string };
      response: string;
    }
  | {
      type: 'trino-fetch-tables';
      service: 'trino';
      request: { catalog: string; schema: string };
      response: string[];
    };

export type TrinoSystemNode = {
  http_uri: string;
  node_id: string;
  node_version: number;
  coordinator: boolean;
  state: string;
};

export type TrinoSystemQuery = {
  // analysis_time_ms: number;
  created: string; // ISO 8601 format
  end: string; // ISO 8601 format
  // error_code: string;
  // error_type: string;
  // last_heartbeat: string; // ISO 8601 format
  // planning_time_ms: number;
  // queued_time_ms: number;
  query?: string;
  query_id: string;
  // resource_group_id: string[];
  source: string;
  started: string; // ISO 8601 format
  state: 'FAILED' | 'FINISHED' | 'QUEUED' | 'RUNNING';
  // user: string;
};

export type TrinoSystemQueryWithTask = TrinoSystemQuery &
  Partial<Omit<TrinoSystemTask, 'query_id' | 'state'>>;

export type TrinoSystemTask = {
  completed_splits: number;
  created: string; // ISO 8601 format
  end: string; // ISO 8601 format
  last_heartbeat: string; // ISO 8601 format
  node_id: string;
  output_bytes: number;
  output_rows: number;
  physical_input_bytes: number;
  physical_written_bytes: number;
  processed_input_bytes: number;
  processed_input_rows: number;
  query_id: string;
  queued_splits: number;
  raw_input_bytes: number;
  raw_input_rows: number;
  running_splits: number;
  split_blocked_time_ms: number;
  split_cpu_time_ms: number;
  split_scheduled_time_ms: number;
  splits: number;
  stage_id: string;
  start: string; // ISO 8601 format
  state: string;
  task_id: string;
};

export type TrinoTable = {
  catalog: string;
  columns: TrinoTableColumn[];
  id: string;
  schema: string;
  table: string;
};

export type TrinoTableColumn = {
  column: string;
  comment: string;
  extra: string;
  type: string;
};
