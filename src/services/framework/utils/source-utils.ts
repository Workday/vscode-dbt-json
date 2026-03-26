/**
 * Source utility functions for Framework
 *
 * Functions for working with DBT sources, source metadata, and source references.
 * These are pure utility functions with no dependencies on other util modules.
 */

import type { DbtProject, DbtProjectManifestSource } from '@shared/dbt/types';
import type { FrameworkModel, FrameworkSource } from '@shared/framework/types';
import * as _ from 'lodash';
import * as path from 'path';

// Helper function for merging objects (imported from lodash or similar)
function mergeDeep(...objects: unknown[]): unknown {
  return _.merge({}, ...objects);
}

/**
 * Get a source from the DBT project manifest
 *
 * @param project - The DBT project
 * @param source - The source name
 * @returns The manifest source or null
 *
 * @example
 * ```typescript
 * const source = frameworkGetSource({ project, source: "raw.users" });
 * ```
 */
export function frameworkGetSource({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): Partial<DbtProjectManifestSource> | null {
  const sourceId = frameworkGetSourceId({ project, source });
  return project.manifest.sources?.[sourceId] ?? null;
}

/**
 * Get the unique ID for a source
 *
 * @param project - The DBT project
 * @param source - The source name
 * @returns The source ID (e.g., "source.project.source_name.table_name")
 *
 * @example
 * ```typescript
 * const sourceId = frameworkGetSourceId({ project, source: "raw.users" });
 * // Returns: "source.my_project.raw.users"
 * ```
 */
export function frameworkGetSourceId({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): string {
  return `source.${project.name}.${_.chain(source).split('.').takeRight(2).join('.').value()}`;
}

/**
 * Get all source IDs for a DBT project from a source JSON
 *
 * @param project - The DBT project
 * @param sourceJson - The framework source JSON
 * @returns Array of source IDs
 */
export function frameworkGetSourceIds({
  project,
  sourceJson,
}: {
  project: DbtProject;
  sourceJson: FrameworkSource;
}): string[] {
  const sourceIds: string[] = [];
  for (const table of sourceJson?.tables ?? []) {
    const sourceId = frameworkMakeSourceId({
      database: sourceJson.database,
      schema: sourceJson.schema,
      table: table.name,
      project,
    });
    sourceIds.push(sourceId);
  }
  return sourceIds;
}

/**
 * Get source metadata
 *
 * @param project - The DBT project
 * @param source - The source name
 * @returns The source metadata
 */
export function frameworkGetSourceMeta({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): Partial<DbtProjectManifestSource['source_meta']> {
  const sourceId = frameworkGetSourceId({ project, source });
  return mergeDeep(
    project.manifest.sources?.[sourceId]?.source_meta,
    project.manifest.sources?.[sourceId]?.meta,
  ) as Partial<DbtProjectManifestSource['source_meta']>;
}

/**
 * Get source properties from schema YAML
 *
 * @param project - The DBT project
 * @param source - The source name
 * @returns The source properties or null
 */
export function frameworkGetSourceProperties({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): Partial<DbtProjectManifestSource> | null {
  const sourceId = frameworkGetSourceId({ project, source });
  return project.manifest.sources?.[sourceId] ?? null;
}

/**
 * Get source reference from a framework model
 *
 * @param modelJson - The framework model JSON
 * @returns The source reference string or false if not a source-based model
 *
 * @example
 * ```typescript
 * const sourceRef = frameworkGetSourceRef(modelJson);
 * // Returns: "raw.users" or false
 * ```
 */
export function frameworkGetSourceRef(
  modelJson: FrameworkModel,
): string | null {
  return ('source' in modelJson.from && modelJson.from.source) || null;
}

// ========================================================================
// Source ID and Name Generation
// ========================================================================

/**
 * Generate a source ID from components
 *
 * @param database - The database name
 * @param project - The DBT project
 * @param schema - The schema name
 * @param table - The table name
 * @returns The source ID
 */
export function frameworkMakeSourceId({
  database,
  project,
  schema,
  table,
}: {
  database: string;
  project: { name: string };
  schema: string;
  table: string;
}): string {
  const sourceName = frameworkMakeSourceName({ database, schema });
  return `source.${project.name}.${sourceName}.${table}`;
}

/**
 * Generate a source name from components
 *
 * @param database - The database name
 * @param schema - The schema name
 * @returns The formatted source name
 *
 * @example
 * ```typescript
 * const name = frameworkMakeSourceName({ database: "analytics", schema: "raw" });
 * // Returns: "analytics__raw"
 * ```
 */
export function frameworkMakeSourceName({
  database,
  schema,
}: {
  database: string;
  schema: string;
}): string {
  return `${database}__${schema}`;
}

/**
 * Generate a source prefix (file path) from components
 *
 * @param database - The database name
 * @param project - The DBT project
 * @param schema - The schema name
 * @returns The source file path prefix
 */
export function frameworkMakeSourcePrefix({
  database,
  project,
  schema,
}: {
  database: string;
  project: DbtProject;
  schema: string;
}): string | null {
  const sourceName = frameworkMakeSourceName({ database, schema });
  const sourcePath = path.join(
    project.pathSystem,
    project.modelPaths[0] ?? 'models',
    'sources',
    database,
    sourceName,
  );
  return sourcePath;
}

/**
 * Generate a source reference string from components
 *
 * @param database - The database name
 * @param schema - The schema name
 * @param table - The table name
 * @returns The source reference (e.g., "analytics__raw.users")
 *
 * @example
 * ```typescript
 * const ref = frameworkMakeSourceRef({ database: "analytics", schema: "raw", table: "users" });
 * // Returns: "analytics__raw.users"
 * ```
 */
export function frameworkMakeSourceRef({
  database,
  schema,
  table,
}: {
  database: string;
  schema: string;
  table: string;
}): string {
  const sourceName = frameworkMakeSourceName({ database, schema });
  return `${sourceName}.${table}`;
}
