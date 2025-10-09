import type { SchemaColumnAgg } from '@shared/schema/types/column.agg.schema';
import type { SchemaColumnDataType } from '@shared/schema/types/column.data_type.schema';
import type { SchemaColumnDataTests } from '@shared/schema/types/column.data_tests.schema';
import type { SchemaLightdashMetric } from '@shared/schema/types/lightdash.metric.schema';
import type {
  SchemaModel,
  SchemaModelSelectInterval,
} from '@shared/schema/types/model.schema';
import type { SchemaModelLightdash } from '@shared/schema/types/model.lightdash.schema';
import type { SchemaModelHaving } from '@shared/schema/types/model.having.schema';
import type { SchemaModelWhere } from '@shared/schema/types/model.where.schema';
import type { SchemaModelSelectCol } from '@shared/schema/types/model.select.col.schema';
import type { SchemaModelSelectColWithAgg } from '@shared/schema/types/model.select.col.with.agg.schema';
import type { SchemaModelSelectExpr } from '@shared/schema/types/model.select.expr.schema';
import type { SchemaModelSelectExprWithAgg } from '@shared/schema/types/model.select.expr.with.agg.schema';
import type { SchemaModelSelectModel } from '@shared/schema/types/model.select.model.schema';
import type { SchemaModelSelectModelWithAgg } from '@shared/schema/types/model.select.model.with.agg.schema';
import type { SchemaModelSelectSource } from '@shared/schema/types/model.select.source.schema';
import type { SchemaSourcePartition } from '@shared/schema/types/source.schema';
import type { SchemaSource } from '@shared/schema/types/source.schema';

import {
  LightdashDimension,
  LightdashMetric,
  LightdashMetrics,
  LightdashTable,
} from '@shared/lightdash/types';
import { ApiRequest, ApiResponse } from '@shared/api/types';

export type FrameworkApi =
  | {
      type: 'framework-model-create';
      service: 'framework';
      request: Pick<FrameworkModel, 'group' | 'name' | 'topic' | 'type'> & {
        group: string;
        name: string;
        topic: string;
        type: FrameworkModel['type'];
        projectName: string;
      };
      response: string;
    }
  | {
      type: 'framework-source-create';
      service: 'framework';
      request: {
        projectName: string;
        trinoCatalog: string;
        trinoSchema: string;
        trinoTable: string;
      };
      response: string;
    };

async function apiHandler(p: {
  type: 'framework-model-create';
  request: ApiRequest<'framework-model-create'>;
}): Promise<ApiResponse<'framework-model-create'>>;
async function apiHandler(p: {
  type: 'framework-source-create';
  request: ApiRequest<'framework-source-create'>;
}): Promise<ApiResponse<'framework-source-create'>>;
async function apiHandler(
  p: Omit<FrameworkApi, 'response' | 'service'>,
): Promise<unknown> {
  return null;
}
export type FrameworkApiHandler = typeof apiHandler;

export type FrameworkColumnAgg = SchemaColumnAgg;

export type FrameworkColumn = {
  name: string;
  data_tests?: FrameworkColumnDataTests;
  data_type?: FrameworkDataType;
  description?: string;
  tags?: string[];
  meta: FrameworkColumnMeta;
};
export type FrameworkColumnMeta = {
  type: 'dim' | 'fct';
  // Only used for datetime column
  interval?: 'day' | 'hour' | 'month' | 'year';
  // Should get stripped out when inherited
  agg?: FrameworkColumnAgg;
  aggs?: FrameworkColumnAgg[];
  exclude_from_group_by?: boolean;
  expr?: string;
  origin?: { id: string };
  override_suffix_agg?: boolean;
  prefix?: string;
  // Meta for Lightdash
  dimension?: LightdashDimension;
  metrics?: LightdashMetrics;
  metrics_merge?: LightdashMetric;
};

export type FrameworkColumnLightdashMetric = SchemaLightdashMetric;

export type FrameworkColumnName = FrameworkDims | FrameworkFcts;

export type FrameworkDims = FrameworkPartitionName | 'datetime';
export type FrameworkFcts = 'portal_source_count';

export type FrameworkColumnDataTests = SchemaColumnDataTests;

export type FrameworkMetrics = {
  [name: string]: Omit<FrameworkColumnLightdashMetric, 'name'>;
};

export type FrameworkDataType = SchemaColumnDataType;
// export type FrameworkDimension = SchemaModelSelectDim;
// export type FrameworkFact = SchemaModelSelectFct;

export type FrameworkInterval = 'hour' | 'day' | 'month' | 'year';

export type FrameworkEtlSource = {
  etl_active: boolean;
  properties: string;
  source_id: string;
};

export type FrameworkModel = SchemaModel;
export type FrameworkModelHaving = SchemaModelHaving;
export type FrameworkModelLightdash = SchemaModelLightdash;
export type FrameworkModelMeta = LightdashTable & {
  local_tags?: string[];
  metrics?: LightdashMetrics;
  portal_partition_columns?: string[];
};
export type FrameworkModelWhere = SchemaModelWhere;

export type FrameworkManifestModel = {
  description: string;
  dimensions: FrameworkColumn[];
  facts: FrameworkColumn[];
  name: string;
};

export type FrameworkMetricType =
  | 'average'
  | 'boolean'
  | 'count'
  | 'count_distinct'
  | 'date'
  | 'max'
  | 'median'
  | 'min'
  | 'number'
  | 'percentile'
  | 'string'
  | 'sum';

export type FrameworkPartitionName =
  | 'portal_partition_daily'
  | 'portal_partition_hourly'
  | 'portal_partition_monthly';

export type FrameworkProjectOption = {
  label: string;
  value: string;
};

export type FrameworkProjectOptions = Array<FrameworkProjectOption>;

export type FrameworkSchemaBase = {
  $id?: string;
  $ref?: string;
  additionalProperties?: boolean;
  anyOf?: FrameworkSchemaBase[];
  const?: string;
  description?: string;
  enum?: string[];
  items?: FrameworkSchemaBase;
  minItems?: number;
  properties?: Record<string, FrameworkSchemaBase>;
  required?: string[];
  type?: 'array' | 'object' | 'string';
};

export type FrameworkSelected =
  | string
  | SchemaModelSelectCol
  | SchemaModelSelectColWithAgg
  | SchemaModelSelectExpr
  | SchemaModelSelectExprWithAgg
  | SchemaModelSelectInterval
  | SchemaModelSelectModel
  | SchemaModelSelectModelWithAgg
  | SchemaModelSelectSource;

// TODO: Populate from schema
export type FrameworkSource = SchemaSource;

export type FrameworkSourcePartition = SchemaSourcePartition;

export type FrameworkSourceColumnMetaFilter =
  | 'event_date'
  | 'event_day'
  | 'event_month'
  | 'event_year';

export type FrameworkSourceMeta = {
  etl?: {
    lookback_days?: number;
    sql_event_date_updated_timestamps?: string;
    sql_retry?: string;
  };
  event_datetime?: {
    data_type?: FrameworkDataType;
    expr: string;
    interval?: FrameworkInterval;
    use_range?: boolean;
  };
  local_tags?: string[];
  partition_date?: {
    compile_dates?: boolean;
    data_type?: FrameworkDataType;
    expr: string;
    interval?: FrameworkInterval;
    use_event_dates?: boolean;
    use_range?: boolean;
  };
  partitions?: FrameworkSourcePartition[];
  portal_partition_columns?: string[];
  portal_source_count?: {
    exclude?: boolean;
    metric_label?: string;
  };
  table_function?: {
    // dbt seems to rename catalog to database in the manifest
    catalog?: string;
    database?: string;
    //
    arg: string;
    dialect: 'bigquery';
    name: string;
    schema: string;
  };
  where?: {
    expr?: string;
  };
};
export type FrameworkSourceTableMeta = FrameworkSourceMeta & {
  // If there are any meta keys specific to tables, we can add them here
};

export type FrameworkSourceColumnMeta = {
  type?: 'dim' | 'fct';
  lightdash?: {
    dimension?: LightdashDimension;
    // metrics?: LightdashMetrics;
  };
};

export type FrameworkSyncOp =
  | {
      type: 'delete';
      path: string;
    }
  | {
      type: 'write';
      text: string;
      path: string;
    };

export type FrameworkSyncPayload = {
  timestamp: string;
  roots?: { id: string; pathJson: string }[];
};
