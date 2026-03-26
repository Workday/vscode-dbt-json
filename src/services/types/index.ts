import type { ApiPayload, ApiResponse, ApiService } from '@shared/api/types';
import type * as vscode from 'vscode';

// Re-export config types
export type { CoderConfig } from './config';

/**
 * Base interface for all DJ services.
 *
 * Services implement lifecycle methods for VS Code integration.
 */
export interface DJService {
  /**
   * Activate the service
   * Can be synchronous or asynchronous depending on service needs
   */
  activate(context: vscode.ExtensionContext): void | Promise<void>;

  /**
   * Register VS Code commands specific to this service
   */
  registerCommands?(context: vscode.ExtensionContext): void;

  /**
   * Register VS Code language providers (code lens, definitions, etc.)
   */
  registerProviders?(context: vscode.ExtensionContext): void;

  /**
   * Register VS Code event handlers
   */
  registerEventHandlers?(context: vscode.ExtensionContext): void;
}

/**
 * Extended interface for services that handle API requests.
 *
 * Uses generic constraint to ensure type-safe API handling based on service domain.
 * Services implementing this get full type inference for their specific API payloads.
 *
 * @example
 * export class Framework implements ApiEnabledService<'framework'> {
 *   async handleApi(payload: ApiPayload<'framework'>): Promise<ApiResponse> {
 *     // payload.type is narrowed to framework-specific types
 *     // Full autocomplete and type checking!
 *   }
 * }
 */
export interface ApiEnabledService<S extends ApiService = ApiService>
  extends DJService {
  /**
   * Handle API requests for this service's domain.
   *
   * @param payload - API payload narrowed to this service's types
   * @returns Promise resolving to the appropriate response
   */
  handleApi(payload: ApiPayload<S>): Promise<ApiResponse>;
}

/**
 * Consistent error handling utilities for services.
 *
 * Provides standardized error logging and response patterns.
 */
export class ServiceError extends Error {
  constructor(
    message: string,
    public readonly code?: string,
    public readonly details?: unknown,
  ) {
    super(message);
    this.name = 'ServiceError';
  }
}

/**
 * Standard error response format for API handlers.
 */
export interface ErrorResponse {
  success: false;
  error: string;
  code?: string;
  details?: unknown;
}

/**
 * Standard success response format for API handlers.
 */
export interface SuccessResponse<T = unknown> {
  success: true;
  data: T;
}

/**
 * Utility for consistent error handling in service methods.
 *
 * @param context - Service context with logger
 * @param operation - Description of the operation being performed
 * @param error - The error that occurred
 * @param level - Log level ('warn' | 'error')
 * @returns Standardized error response
 */
export function handleServiceError(
  logger: {
    warn: (message: string, ...args: unknown[]) => void;
    error: (message: string, ...args: unknown[]) => void;
  },
  operation: string,
  error: unknown,
  level: 'warn' | 'error' = 'error',
): ErrorResponse {
  const message = error instanceof Error ? error.message : String(error);
  const logMethod = level === 'warn' ? logger.warn : logger.error;

  logMethod(`[${operation}] ${message}`, error);

  return {
    success: false,
    error: message,
    code: error instanceof ServiceError ? error.code : undefined,
    details: error instanceof ServiceError ? error.details : undefined,
  };
}

/**
 * Utility for safe async operations that should not fail the entire request.
 *
 * @param operation - Description of the operation
 * @param fn - The async function to execute
 * @param context - Service context with logger
 * @param fallback - Optional fallback value if operation fails
 * @returns Result of operation or fallback value
 */
export async function safeAsync<T>(
  operation: string,
  fn: () => Promise<T>,
  context: { log: { warn: (message: string, ...args: unknown[]) => void } },
  fallback?: T,
): Promise<T | undefined> {
  try {
    return await fn();
  } catch (error: unknown) {
    context.log.warn(`[${operation}] Failed but continuing:`, error);
    return fallback;
  }
}

/**
 * Type guard for API responses with success/error structure.
 */
export function isApiSuccessResponse<T>(
  response: unknown,
): response is { success: true; data: T } {
  return (
    typeof response === 'object' &&
    response !== null &&
    'success' in response &&
    response.success === true &&
    'data' in response
  );
}

/**
 * Type guard for API error responses.
 */
export function isApiErrorResponse(response: unknown): response is {
  success: false;
  error: string;
  code?: string;
  details?: unknown;
} {
  return (
    typeof response === 'object' &&
    response !== null &&
    'success' in response &&
    response.success === false &&
    'error' in response &&
    typeof response.error === 'string'
  );
}

/**
 * Validates that a required parameter is present, throws ServiceError if not.
 *
 * @param value - The value to check
 * @param paramName - Name of the parameter for error message
 * @param operation - Operation context for error message
 */
export function requireParam<T>(
  value: T | null | undefined,
  paramName: string,
  operation: string,
): T {
  if (value === null || value === undefined) {
    throw new ServiceError(
      `Missing required parameter: ${paramName}`,
      'MISSING_PARAM',
      { paramName, operation },
    );
  }
  return value;
}

/**
 * Validates that a project exists, throws ServiceError if not.
 *
 * Common pattern across framework operations.
 *
 * @param project - The project to validate
 * @param projectName - Name for error message
 * @param operation - Operation context
 */
export function requireProject<T>(
  project: T | null | undefined,
  projectName: string,
  operation: string,
): T {
  if (project === null || project === undefined) {
    throw new ServiceError(
      `Project not found: ${projectName}`,
      'PROJECT_NOT_FOUND',
      { projectName, operation },
    );
  }
  return project;
}
