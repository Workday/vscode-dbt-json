export interface ModelTestInfo {
  modelName: string | null;
  projectName: string;
  projectPath: string;
}

// Model run configuration for lineage selection
export interface ModelConfig {
  upstream: boolean;
  downstream: boolean;
  fullLineage: boolean;
  upLimit: number;
  downLimit: number;
}

// Analytics summary after test run
export interface RunAnalytics {
  total: number;
  success: number;
  error: number;
  warning: number;
  successRate: number;
  failureRate: number;
}

// Default config for new models
export const DEFAULT_MODEL_CONFIG: ModelConfig = {
  upstream: false,
  downstream: false,
  fullLineage: false,
  upLimit: 0,
  downLimit: 0,
};
