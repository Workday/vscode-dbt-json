import type { SchemaLightdashDimension } from '@shared/schema/types/lightdash.dimension.schema';
import type { SchemaLightdashMetric } from '@shared/schema/types/lightdash.metric.schema';
import type { SchemaLightdashTable } from '@shared/schema/types/lightdash.table.schema';

export type LightdashModel = {
  name: string;
  tags: string[];
  description?: string;
};

export type LightdashPreview = {
  name: string;
  url: string;
  createdAt: string;
  models: string[];
  status: 'active' | 'inactive';
};

export type LightdashPreviewLog = {
  level: 'info' | 'success' | 'error' | 'warning';
  message: string;
  timestamp: string;
  isProgress?: boolean;
  isPreviewSuccess?: boolean;
};

export type LightdashApi =
  | {
      type: 'lightdash-fetch-models';
      service: 'lightdash';
      request: null;
      response: LightdashModel[];
    }
  | {
      type: 'lightdash-start-preview';
      service: 'lightdash';
      request: {
        previewName: string;
        selectedModels: string[];
      };
      response: { success: boolean; url?: string; error?: string };
    }
  | {
      type: 'lightdash-stop-preview';
      service: 'lightdash';
      request: {
        previewName: string;
      };
      response: { success: boolean; error?: string };
    }
  | {
      type: 'lightdash-fetch-previews';
      service: 'lightdash';
      request: null;
      response: LightdashPreview[];
    }
  | {
      type: 'lightdash-get-preview-name';
      service: 'lightdash';
      request: null;
      response: string;
    }
  | {
      type: 'lightdash-add-log';
      service: 'lightdash';
      request: {
        log: LightdashPreviewLog;
      };
      response: { success: boolean };
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
