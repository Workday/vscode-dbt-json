/**
 * Constants and configuration values for the DJ extension
 * Centralized to avoid hardcoded values throughout the codebase
 */

// Extension constants
export const OUTPUT_CHANNEL_NAME = 'DJ';

// File patterns and regex
export const FILE_WATCHER_PATTERN =
  '{/**/*.json,/**/*.sql,/**/*.yml,**/.git/logs/HEAD}';

export const IGNORE_PATHS_REGEX =
  /\.dbt\/|\.dj\/|\.vscode\/|\/_ext_\/|\/dbt_packages\/|\/target\/.+\//;

export const FILE_REGEX =
  '(?:\\/(?:\\w|\\d|-)+)*\\/((?:\\w|\\d|-)+)+(?:\\.((?:\\w|\\d|\\.|-)+))?$';

// Supported file extensions
export const SUPPORTED_EXTENSIONS = [
  'json',
  'model.json',
  'source.json',
  'sql',
  'yml',
];

// Git paths
export const GIT_LOG_PATH = '.git/logs/HEAD';

// Default paths
export const DEFAULT_DBT_PATH = 'dags/dbt';
export const DEFAULT_AIRFLOW_DAGS_PATH = 'dags/_ext_';
export const DEFAULT_DBT_MACRO_PATH = '_ext_';

// Command IDs
export const COMMAND_ID = {
  COLUMN_ORIGIN: 'dj.command.columnOrigin',
  DEFER_RUN: 'dj.command.deferRun',
  LIGHTDASH_PREVIEW: 'dj.command.lightdashPreview',
  MODEL_COMPILE: 'dj.command.modelCompile',
  MODEL_CREATE: 'dj.command.modelCreate',
  MODEL_NAVIGATE: 'dj.command.modelNavigate',
  MODEL_RUN: 'dj.command.modelRun',
  MODEL_RUN_LINEAGE: 'dj.command.modelRunLineage',
  PROJECT_CLEAN: 'dj.command.projectClean',
  SOURCE_CREATE: 'dj.command.sourceCreate',
  SOURCE_NAVIGATE: 'dj.command.sourceNavigate',
  SOURCE_ORIGIN: 'dj.command.sourceOrigin',
  SOURCE_REFRESH: 'dj.command.sourceRefresh',
  FRAMEWORK_JUMP_JSON: 'dj.command.frameworkJumpJson',
  FRAMEWORK_JUMP_MODEL: 'dj.command.frameworkJumpModel',
  FRAMEWORK_JUMP_YAML: 'dj.command.frameworkJumpYaml',
  JSON_SYNC: 'dj.command.jsonSync',
  QUERY_VIEW: 'dj.command.queryView',
  TEST_TRINO_CONNECTION: 'dj.command.testTrinoConnection',
} as const;

// View IDs
export const VIEW_ID = {
  PROJECT_NAVIGATOR: 'dj.view.projectNavigator',
  MODEL_ACTIONS: 'dj.view.modelActions',
  SELECTED_RESOURCE: 'dj.view.selectedResource',
  QUERY_ENGINE: 'dj.view.queryEngine',
  MODEL_CREATE: 'dj.view.modelCreate',
  SOURCE_CREATE: 'dj.view.sourceCreate',
} as const;

export const DBT_MSG = {
  CREATE_SOURCE: 'Create Source',
  CREATE_MODEL: 'Create Model',
  COMPILE_MODEL: 'Compile Model',
  CLEAN_AND_DEPS: 'Clean and Deps',
  CLEAN_PROJECT: 'Clean Project',
  RUN_DEFER: 'Run Defer',
  RUN_MODEL: 'Run Model',
  RUN_MODEL_LINEAGE: 'Run Model Lineage',
  SYNC_JSON_MODELS: 'Sync JSON Models',
} as const;
