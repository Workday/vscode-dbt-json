import type { DbtProject } from '@shared/dbt/types';
import _ from 'lodash';

// Source options for model creation
export const generateSourceOptions = () => [
  { label: 'Staging', value: 'staging' },
  { label: 'Intermediate', value: 'intermediate' },
  { label: 'Marts', value: 'marts' },
];

const stagingTypeOptions = [
  { label: 'Select Model', value: 'stg_select_model' },
  { label: 'Select Source', value: 'stg_select_source' },
  { label: 'Union Source', value: 'stg_union_sources' },
];

const intermediateTypeOptions = [
  { label: 'Join Column', value: 'int_join_column' },
  { label: 'Join Model', value: 'int_join_models' },
  { label: 'Lookback Model', value: 'int_lookback_model' },
  { label: 'Rollup Model', value: 'int_rollup_model' },
  { label: 'Select Model', value: 'int_select_model' },
  { label: 'Union Model', value: 'int_union_models' },
];

const martsTypeOptions = [
  { label: 'Join Model', value: 'mart_join_models' },
  { label: 'Select Model', value: 'mart_select_model' },
];

// Type options based on selected source
export const generateTypeOptions = (source: string | undefined) => {
  switch (source) {
    case 'staging':
      return stagingTypeOptions;
    case 'intermediate':
      return intermediateTypeOptions;
    case 'marts':
      return martsTypeOptions;
    default:
      return [];
  }
};

// Derive source from model type (reverse mapping)
export const deriveSourceFromType = (type: string | undefined): string => {
  if (!type) return '';

  if (type.startsWith('stg_')) {
    return 'staging';
  } else if (type.startsWith('int_')) {
    return 'intermediate';
  } else if (type.startsWith('mart_')) {
    return 'marts';
  }

  return '';
};

// Materialization options
export const generateMaterializedOptions = () => [
  { label: 'Incremental', value: 'incremental' },
  { label: 'Ephemeral', value: 'ephemeral' },
];

// Generate project options from fetched projects
export const generateProjectOptions = (projects: DbtProject[] | null) =>
  _.map(projects, (p) => {
    const value = p.name;
    return { label: value, value };
  }) || [];

// Generate group options from selected project
export const generateGroupOptions = (project: DbtProject | null) => {
  const manifestGroups = _.map(project?.manifest?.groups, (g) => {
    const value = g?.name || '';
    return { label: value, value };
  });

  if (manifestGroups && manifestGroups.length > 0) {
    return manifestGroups;
  }

  return manifestGroups;
};
