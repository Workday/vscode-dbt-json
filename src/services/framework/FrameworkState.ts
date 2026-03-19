import type { FrameworkEtlSource } from '@shared/framework/types';
import type { TreeItem } from 'admin';
import type { Uri } from 'vscode';

/**
 * Shared state between Framework and Dbt services.
 *
 * Previously, Framework and Dbt had a bidirectional dependency where they
 * accessed each other's properties directly. This shared state object
 * breaks that circular dependency by providing a common place for state
 * that both services need to access.
 *
 * @example
 * // Before (bidirectional dependency):
 * // dbt.ts: this.framework.groupNames.add(...)
 * // framework.ts: this.dbt.projects.get(...)
 *
 * // After (shared state):
 * // dbt.ts: this.state.groupNames.add(...)
 * // framework.ts: this.dbt.projects.get(...) // Direct access, no issue
 */
export class FrameworkState {
  /**
   * Group names from dbt manifest.
   * Used by both Framework and Dbt to track available groups.
   */
  groupNames = new Set<string>();

  /**
   * Model tree roots for navigation.
   * Maps model names to their tree item representations.
   */
  modelTreeRoots = new Map<string, TreeItem>();

  /**
   * Source references for tracking source usage.
   * Set of source IDs that are referenced in the project.
   */
  sourceRefs = new Set<string>();

  /**
   * Model tree structure for dependency tracking.
   * Maps model IDs to their parent/child relationships and file URIs.
   */
  modelTree = new Map<
    string,
    {
      name: string;
      children: string[];
      parents: string[];
      uris: {
        json: Uri | null;
        sql: Uri | null;
        yml: Uri | null;
      };
    }
  >();

  /**
   * Source columns mapping.
   * Maps source IDs to their column names.
   */
  sourceColumns = new Map<string, string[]>();

  /**
   * ETL sources from Trino.
   * Maps source IDs to their ETL source metadata.
   */
  etlSources = new Map<string, FrameworkEtlSource>();

  /**
   * Clears all state.
   * Useful for testing or resetting the framework.
   */
  clear(): void {
    this.groupNames.clear();
    this.modelTreeRoots.clear();
    this.sourceRefs.clear();
    this.modelTree.clear();
    this.sourceColumns.clear();
    this.etlSources.clear();
  }
}
