import type { ApiResponse, ApiType } from '@shared/api/types';

export function apiResponse<T extends ApiType = never>(
  response: ApiResponse<T>,
) {
  return response as unknown as ApiResponse<T>;
}
