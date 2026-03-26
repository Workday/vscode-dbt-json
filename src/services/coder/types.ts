/**
 * Coder service types (extension-only).
 */

import type { GitAction } from '@services/utils/git';
import type {
  DbtModel,
  DbtProject,
  DbtProjectManifest,
  DbtProperties,
} from '@shared/dbt/types';
import type { FrameworkModel, FrameworkSource } from '@shared/framework/types';

export type CoderContext = {
  trino: { tables: string[] };
  web: { route: { label: string; path: string } };
};

export type CoderFileInfo =
  | {
      type: 'compiled';
      filePath: string;
      model: DbtModel;
      name: string;
      project: DbtProject;
      sql: string;
    }
  | {
      type: 'git-log';
      log: { action: GitAction; line: string };
    }
  | {
      type: 'framework-model';
      filePath: string;
      model?: DbtModel; // We may not have a model yet
      modelJson: FrameworkModel;
      project: DbtProject;
    }
  | {
      type: 'framework-source';
      filePath: string;
      project: DbtProject;
      sourceJson: FrameworkSource;
    }
  | {
      type: 'macro';
      name: string;
      project: DbtProject;
    }
  | {
      type: 'manifest';
      manifest: DbtProjectManifest;
      project: DbtProject;
    }
  | {
      type: 'model';
      filePrefix: string;
      model: DbtModel;
      project: DbtProject;
    }
  | {
      type: 'yml';
      filePath: string;
      project: DbtProject;
      properties: Partial<DbtProperties>;
    }
  | null;
