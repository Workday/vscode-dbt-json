import { SchemaLightdashDimension } from '@shared/schema/types/lightdash.dimension.schema';
import { SchemaLightdashMetric } from '@shared/schema/types/lightdash.metric.schema';
import { SchemaLightdashTable } from '@shared/schema/types/lightdash.table.schema';

export type LightdashApi = {
  type: 'lightdash-start-preview';
  service: 'lightdash';
  request: null;
  response: { url: string } | { error: string };
};

export type LightdashDimension = SchemaLightdashDimension & {};

export type LightdashMetric = Omit<SchemaLightdashMetric, 'name'>; // Name is on the schema because we're inputing as array
export type LightdashMetrics = Record<string, LightdashMetric>;

export type LightdashTable = SchemaLightdashTable & {
  // These properties are saved to the meta in a different format than the schema
  metrics?: Record<string, LightdashMetric>;
  required_attributes?: Record<string, string | string[]>;
  required_filters?: Record<string, string>[];
};
