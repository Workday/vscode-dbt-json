import type {
  DbtModel,
  DbtProject,
  DbtProjectManifest,
  DbtProperties,
} from '@shared/dbt/types';
import type { FrameworkModel, FrameworkSource } from '@shared/framework/types';
import type { GitAction } from '@shared/git/types';

export type AirflowTargetVersion = '2.7' | '2.8' | '2.9' | '2.10';

// Configuration object from VSCode for the DJ extension
export type CoderConfig = {
  airflowTargetVersion?: AirflowTargetVersion;
  airflowGenerateDags: boolean;
  airflowDagsPath: string;
  dbtProjectNames?: string[];
  dbtMacroPath: string;
  pythonVenvPath?: string;
  trinoPath?: string;
  lightdashProjectPath?: string;
  lightdashProfilesPath?: string;
};

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

export type VSCodeApi = {
  postMessage: (msg: object) => void;
};
