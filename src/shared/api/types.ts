import { DistributiveOmit } from '@shared';
import type { DbtApi } from '@shared/dbt/types';
import type { FrameworkApi } from '@shared/framework/types';
import type { LightdashApi } from '@shared/lightdash/types';
import type { TrinoApi } from '@shared/trino/types';

// State API types
export type StateApi =
  | {
      type: 'state-load';
      service: 'state';
      request: { formType: string };
      response: { data: Record<string, any> | null };
    }
  | {
      type: 'state-save';
      service: 'state';
      request: { formType: string; data: Record<string, any> };
      response: { success: boolean };
    }
  | {
      type: 'state-clear';
      service: 'state';
      request: { formType: string };
      response: { success: boolean };
    };

export type Apis = DbtApi | FrameworkApi | LightdashApi | TrinoApi | StateApi;

export type Api<T extends Apis['type'] = Apis['type']> = Extract<
  Apis,
  { type: T }
>;
export type ApiError =
  | {
      details?: Record<string, boolean | number | string> | string;
      message: string;
    }
  | Error
  | { response: { data: Record<string, string>; message?: string } };
export type ApiMessage = ApiHandlerPayload & { _channelId: string };
export type ApiPayload<T extends ApiService = ApiService> = DistributiveOmit<
  Extract<Api, { service: T }>,
  'response' | 'service'
>;
export type ApiRequest<T extends ApiType = ApiType> = Extract<
  Api,
  { type: T }
>['request'];
export type ApiResponse<T extends ApiType = ApiType> = Extract<
  Api,
  { type: T }
>['response'];
export type ApiService = Api['service'];
export type ApiType = Api['type'];

// Overload functions
async function apiHandler(p: {
  type: 'dbt-fetch-modified-models';
  request: ApiRequest<'dbt-fetch-modified-models'>;
}): Promise<ApiResponse<'dbt-fetch-modified-models'>>;
async function apiHandler(p: {
  type: 'dbt-fetch-projects';
  request: ApiRequest<'dbt-fetch-projects'>;
}): Promise<ApiResponse<'dbt-fetch-projects'>>;
async function apiHandler(p: {
  type: 'dbt-parse-project';
  request: ApiRequest<'dbt-parse-project'>;
}): Promise<ApiResponse<'dbt-parse-project'>>;
async function apiHandler(p: {
  type: 'dbt-run-model';
  request: ApiRequest<'dbt-run-model'>;
}): Promise<ApiResponse<'dbt-run-model'>>;
async function apiHandler(p: {
  type: 'dbt-run-model-lineage';
  request: ApiRequest<'dbt-run-model-lineage'>;
}): Promise<ApiResponse<'dbt-run-model-lineage'>>;
async function apiHandler(p: {
  type: 'framework-model-create';
  request: ApiRequest<'framework-model-create'>;
}): Promise<ApiResponse<'framework-model-create'>>;
async function apiHandler(p: {
  type: 'framework-source-create';
  request: ApiRequest<'framework-source-create'>;
}): Promise<ApiResponse<'framework-source-create'>>;
async function apiHandler(p: {
  type: 'lightdash-start-preview';
  request: ApiRequest<'lightdash-start-preview'>;
}): Promise<ApiResponse<'framework-source-create'>>;
async function apiHandler(p: {
  type: 'trino-fetch-catalogs';
  request: ApiRequest<'trino-fetch-catalogs'>;
}): Promise<ApiResponse<'trino-fetch-catalogs'>>;
async function apiHandler(p: {
  type: 'trino-fetch-columns';
  request: ApiRequest<'trino-fetch-columns'>;
}): Promise<ApiResponse<'trino-fetch-columns'>>;
async function apiHandler(p: {
  type: 'trino-fetch-current-schema';
  request: ApiRequest<'trino-fetch-current-schema'>;
}): Promise<ApiResponse<'trino-fetch-current-schema'>>;
async function apiHandler(p: {
  type: 'trino-fetch-etl-sources';
  request: ApiRequest<'trino-fetch-etl-sources'>;
}): Promise<ApiResponse<'trino-fetch-etl-sources'>>;
async function apiHandler(p: {
  type: 'trino-fetch-schemas';
  request: ApiRequest<'trino-fetch-schemas'>;
}): Promise<ApiResponse<'trino-fetch-schemas'>>;
async function apiHandler(p: {
  type: 'trino-fetch-tables';
  request: ApiRequest<'trino-fetch-tables'>;
}): Promise<ApiResponse<'trino-fetch-tables'>>;
async function apiHandler(p: {
  type: 'trino-fetch-system-nodes';
  request: ApiRequest<'trino-fetch-system-nodes'>;
}): Promise<ApiResponse<'trino-fetch-system-nodes'>>;
async function apiHandler(p: {
  type: 'trino-fetch-system-queries';
  request: ApiRequest<'trino-fetch-system-queries'>;
}): Promise<ApiResponse<'trino-fetch-system-queries'>>;
async function apiHandler(p: {
  type: 'trino-fetch-system-query-with-task';
  request: ApiRequest<'trino-fetch-system-query-with-task'>;
}): Promise<ApiResponse<'trino-fetch-system-query-with-task'>>;
async function apiHandler(p: {
  type: 'trino-fetch-system-query-sql';
  request: ApiRequest<'trino-fetch-system-query-sql'>;
}): Promise<ApiResponse<'trino-fetch-system-query-sql'>>;
async function apiHandler(p: {
  type: 'state-load';
  request: ApiRequest<'state-load'>;
}): Promise<ApiResponse<'state-load'>>;
async function apiHandler(p: {
  type: 'state-save';
  request: ApiRequest<'state-save'>;
}): Promise<ApiResponse<'state-save'>>;
async function apiHandler(p: {
  type: 'state-clear';
  request: ApiRequest<'state-clear'>;
}): Promise<ApiResponse<'state-clear'>>;
// Implementation function
async function apiHandler(
  p: Omit<Api, 'response' | 'service'>,
): Promise<unknown> {
  return null;
}
// Export function type
export type ApiHandler = typeof apiHandler;
export type ApiHandlerPayload = Parameters<ApiHandler>[0];
export type ApiHandlerReturn = ReturnType<ApiHandler>;
