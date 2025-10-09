import { QUERY_NOT_AVAILABLE } from '@shared/trino/constants';

/**
 * Returns a structured error message for query-related issues.
 * @param {string} error - A specific error.
 * @returns {{label: string, description?: string}}
 */

export const getQueryErrorMessage = (
  error: string,
): { label: string; description?: string } => {
  switch (error) {
    case QUERY_NOT_AVAILABLE:
      // Case 1: The specific, known error
      return {
        label: 'Query details no longer available',
        description: `This query has exceeded the system's data retention period and its history has been cleared.`,
      };

    default:
      // Case 2: The fallback for anything else
      return {
        label: 'Error fetching query',
      };
  }
};
