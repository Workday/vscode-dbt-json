import type { ApiHandler } from '@shared/api/types';
import type { ModelConfig, RunAnalytics } from '@web/pages/ModelTest/types';
import { DEFAULT_MODEL_CONFIG } from '@web/pages/ModelTest/types';
import { create } from 'zustand';

export interface ModelTestLog {
  level: 'info' | 'success' | 'error' | 'warning';
  message: string;
  timestamp: string;
  isProgress?: boolean;
}

export type TestStatus =
  | 'idle'
  | 'progressing'
  | 'success'
  | 'error'
  | 'warning';

export interface TestQueueItem {
  name: string;
  status: TestStatus;
}

export interface TestDetail {
  name: string;
  testType: string;
  columnName?: string;
}

export interface ExistingDataTest {
  type: string;
  compare_model?: string;
  column_name?: string;
  join_type?: string;
  [key: string]: any;
}

export interface ActiveChangeModel {
  name: string;
  testCount: number;
  testDetails: TestDetail[];
  hasJoins: boolean;
  hasAggregates: boolean;
  hasPortalPartitionDaily: boolean;
  modelType: string | null;
  existingDataTests: ExistingDataTest[];
  fromModel: string | null;
  firstJoinType: string | null;
  checked: boolean;
  tests: string[];
  config: ModelConfig;
  isFromGitChanges: boolean;
}

export interface AvailableModel {
  name: string;
  testCount: number;
  testDetails: TestDetail[];
  hasJoins: boolean;
  hasAggregates: boolean;
  hasPortalPartitionDaily: boolean;
  modelType: string | null;
  existingDataTests: ExistingDataTest[];
  fromModel: string | null;
  firstJoinType: string | null;
}

interface ModelTestState {
  projectName: string;
  setProjectName: (name: string) => void;
  isLoading: boolean;
  setIsLoading: (loading: boolean) => void;
  searchTerm: string;
  setSearchTerm: (term: string) => void;
  isTesting: boolean;
  setIsTesting: (testing: boolean) => void;
  logs: string[];
  addLog: (log: string) => void;
  clearLogs: () => void;
  testQueue: TestQueueItem[];
  setTestQueue: (queue: TestQueueItem[]) => void;
  updateTestStatusByName: (name: string, status: TestStatus) => void;
  runAnalytics: RunAnalytics | null;
  setRunAnalytics: (analytics: RunAnalytics | null) => void;
  activeChanges: ActiveChangeModel[];
  setActiveChanges: (models: ActiveChangeModel[]) => void;
  toggleModelCheck: (name: string) => void;
  addToActiveChanges: (model: AvailableModel) => void;
  removeFromActiveChanges: (name: string) => void;
  revertAll: () => void;
  updateModelConfig: (name: string, config: Partial<ModelConfig>) => void;
  updateModelTests: (name: string, tests: string[]) => void;
  addTestsToModel: (name: string, tests: string[]) => void;
  toggleModelConfigField: (
    name: string,
    field: 'upstream' | 'downstream' | 'fullLineage',
  ) => void;
  applyBulkConfig: (config: Partial<ModelConfig>) => void;
  applyBulkTests: (tests: string[]) => void;
  addTestsWithBackend: (
    apiPost: ApiHandler,
    projectName: string,
    modelName: string,
    autoDetect: boolean,
    tests?: Array<{
      type: string;
      compare_model?: string;
      column_name?: string;
      join_type?: string;
    }>,
  ) => Promise<{
    success: boolean;
    addedTests: Array<{ type: string; [key: string]: any }>;
  }>;
  applyBulkTestsWithBackend: (
    apiPost: ApiHandler,
    projectName: string,
    tests: string[],
  ) => Promise<void>;
  revertAllWithBackend: (
    apiPost: ApiHandler,
    projectName: string,
  ) => Promise<void>;
  revertModelWithBackend: (
    apiPost: ApiHandler,
    projectName: string,
    modelName: string,
  ) => Promise<void>;
  availableModels: AvailableModel[];
  setAvailableModels: (models: AvailableModel[]) => void;
  bulkAddAvailable: () => void;
  reset: () => void;
}

const initialState = {
  projectName: '',
  isLoading: true,
  searchTerm: '',
  isTesting: false,
  logs: [] as string[],
  testQueue: [] as TestQueueItem[],
  runAnalytics: null as RunAnalytics | null,
  activeChanges: [] as ActiveChangeModel[],
  availableModels: [] as AvailableModel[],
};

export const useModelTestStore = create<ModelTestState>((set) => ({
  ...initialState,
  setProjectName: (name) => set({ projectName: name }),
  setIsLoading: (loading) => set({ isLoading: loading }),
  setSearchTerm: (term) => set({ searchTerm: term }),
  setIsTesting: (testing) => set({ isTesting: testing }),
  addLog: (log) => set((state) => ({ logs: [...state.logs, log] })),
  clearLogs: () => set({ logs: [], runAnalytics: null }),
  setTestQueue: (queue) => set({ testQueue: queue }),
  updateTestStatusByName: (name, status) =>
    set((state) => ({
      testQueue: state.testQueue.map((item) =>
        item.name === name ? { ...item, status } : item,
      ),
    })),
  setRunAnalytics: (analytics) => set({ runAnalytics: analytics }),
  setActiveChanges: (models) => set({ activeChanges: models }),
  toggleModelCheck: (name) =>
    set((state) => {
      const model = state.activeChanges.find((m) => m.name === name);
      if (!model) return state;
      if (model.checked && !model.isFromGitChanges) {
        return {
          activeChanges: state.activeChanges.filter((m) => m.name !== name),
          availableModels: [
            {
              name: model.name,
              testCount: model.testCount,
              testDetails: model.testDetails,
              hasJoins: model.hasJoins,
              hasAggregates: model.hasAggregates,
              hasPortalPartitionDaily: model.hasPortalPartitionDaily,
              modelType: model.modelType,
              existingDataTests: model.existingDataTests,
              fromModel: model.fromModel,
              firstJoinType: model.firstJoinType,
            },
            ...state.availableModels,
          ],
        };
      }
      return {
        activeChanges: state.activeChanges.map((m) =>
          m.name === name ? { ...m, checked: !m.checked } : m,
        ),
      };
    }),
  addToActiveChanges: (model) =>
    set((state) => {
      if (state.activeChanges.some((m) => m.name === model.name)) {
        return state;
      }
      return {
        availableModels: state.availableModels.filter(
          (m) => m.name !== model.name,
        ),
        activeChanges: [
          {
            ...model,
            testDetails: model.testDetails || [],
            hasJoins: model.hasJoins ?? false,
            hasAggregates: model.hasAggregates ?? false,
            hasPortalPartitionDaily: model.hasPortalPartitionDaily ?? false,
            modelType: model.modelType ?? null,
            existingDataTests: model.existingDataTests || [],
            fromModel: model.fromModel ?? null,
            firstJoinType: model.firstJoinType ?? null,
            checked: true,
            tests: [],
            config: { ...DEFAULT_MODEL_CONFIG },
            isFromGitChanges: false,
          },
          ...state.activeChanges,
        ],
      };
    }),
  removeFromActiveChanges: (name) =>
    set((state) => {
      const model = state.activeChanges.find((m) => m.name === name);
      if (!model) return state;
      if (model.isFromGitChanges) return state;
      return {
        activeChanges: state.activeChanges.filter((m) => m.name !== name),
        availableModels: [
          {
            name: model.name,
            testCount: model.testCount,
            testDetails: model.testDetails,
            hasJoins: model.hasJoins,
            hasAggregates: model.hasAggregates,
            hasPortalPartitionDaily: model.hasPortalPartitionDaily,
            modelType: model.modelType,
            existingDataTests: model.existingDataTests,
            fromModel: model.fromModel,
            firstJoinType: model.firstJoinType,
          },
          ...state.availableModels,
        ],
      };
    }),
  revertAll: () =>
    set((state) => {
      const modelsFromGit = state.activeChanges.filter(
        (m) => m.isFromGitChanges,
      );
      const modelsNotFromGit = state.activeChanges.filter(
        (m) => !m.isFromGitChanges,
      );
      const toAvailable = modelsNotFromGit.map((m) => ({
        name: m.name,
        testCount: m.testCount,
        testDetails: m.testDetails,
        hasJoins: m.hasJoins,
        hasAggregates: m.hasAggregates,
        hasPortalPartitionDaily: m.hasPortalPartitionDaily,
        modelType: m.modelType,
        existingDataTests: m.existingDataTests,
        fromModel: m.fromModel,
        firstJoinType: m.firstJoinType,
      }));
      return {
        activeChanges: modelsFromGit,
        availableModels: [...toAvailable, ...state.availableModels],
      };
    }),
  updateModelConfig: (name, config) =>
    set((state) => ({
      activeChanges: state.activeChanges.map((model) =>
        model.name === name
          ? { ...model, config: { ...model.config, ...config } }
          : model,
      ),
    })),
  updateModelTests: (name, tests) =>
    set((state) => ({
      activeChanges: state.activeChanges.map((model) =>
        model.name === name ? { ...model, tests } : model,
      ),
    })),
  addTestsToModel: (name, tests) =>
    set((state) => ({
      activeChanges: state.activeChanges.map((model) =>
        model.name === name
          ? { ...model, tests: [...new Set([...model.tests, ...tests])] }
          : model,
      ),
    })),
  toggleModelConfigField: (name, field) =>
    set((state) => ({
      activeChanges: state.activeChanges.map((model) =>
        model.name === name
          ? {
              ...model,
              config: { ...model.config, [field]: !model.config[field] },
            }
          : model,
      ),
    })),
  applyBulkConfig: (config) =>
    set((state) => ({
      activeChanges: state.activeChanges.map((model) =>
        model.checked
          ? { ...model, config: { ...model.config, ...config } }
          : model,
      ),
    })),
  applyBulkTests: (tests) =>
    set((state) => ({
      activeChanges: state.activeChanges.map((model) =>
        model.checked
          ? { ...model, tests: [...new Set([...model.tests, ...tests])] }
          : model,
      ),
    })),
  addTestsWithBackend: async (
    apiPost,
    projectName,
    modelName,
    autoDetect,
    tests,
  ) => {
    try {
      const response = await (
        apiPost as (p: {
          type: 'dbt-add-model-tests';
          request: {
            projectName: string;
            modelName: string;
            autoDetect?: boolean;
            tests?: Array<{
              type: string;
              compare_model?: string;
              column_name?: string;
              join_type?: string;
            }>;
          };
        }) => Promise<{ success: boolean; addedTests: Array<{ type: string }> }>
      )({
        type: 'dbt-add-model-tests',
        request: {
          projectName,
          modelName,
          autoDetect,
          tests,
        },
      });

      if (response.success && response.addedTests.length > 0) {
        const newTestDetails = response.addedTests.map(
          (t: { type: string; column_name?: string }) => ({
            name: `${t.type}${t.column_name ? `_${t.column_name}` : ''}`,
            testType: t.type,
            columnName: t.column_name,
          }),
        );

        set((state) => ({
          activeChanges: state.activeChanges.map((model) =>
            model.name === modelName
              ? {
                  ...model,
                  testCount: model.testCount + response.addedTests.length,
                  testDetails: [...model.testDetails, ...newTestDetails],
                  existingDataTests: [
                    ...model.existingDataTests,
                    ...response.addedTests,
                  ],
                }
              : model,
          ),
        }));
      }

      return response;
    } catch (error) {
      console.error('Failed to add tests:', error);
      return { success: false, addedTests: [] };
    }
  },
  applyBulkTestsWithBackend: async (apiPost, projectName, tests) => {
    const state = useModelTestStore.getState();
    const checkedModels = state.activeChanges.filter((m) => m.checked);

    const getApplicableTests = (
      testIds: string[],
      model: ActiveChangeModel,
    ): Array<{
      type: string;
      compare_model?: string;
      column_name?: string;
      join_type?: string;
    }> => {
      const result: Array<{
        type: string;
        compare_model?: string;
        column_name?: string;
        join_type?: string;
      }> = [];

      for (const testId of testIds) {
        if (
          testId === 'equal_row_count' ||
          testId === 'equal_or_lower_row_count'
        ) {
          if (!model.hasJoins || !model.fromModel) continue;
          result.push({
            type: testId,
            compare_model: `ref('${model.fromModel}')`,
            join_type: model.firstJoinType || 'left',
          });
        } else if (testId === 'no_null_aggregates') {
          if (!model.hasAggregates) continue;
          result.push({ type: testId, column_name: '' });
        } else {
          result.push({ type: testId });
        }
      }

      return result;
    };

    for (const model of checkedModels) {
      const testsPayload = getApplicableTests(tests, model);
      if (testsPayload.length === 0) continue;
      await state.addTestsWithBackend(
        apiPost,
        projectName,
        model.name,
        false,
        testsPayload,
      );
    }
  },
  revertAllWithBackend: async (apiPost, projectName) => {
    const state = useModelTestStore.getState();
    const modelNames = state.activeChanges.map((m) => m.name);

    if (modelNames.length === 0) return;

    try {
      await (
        apiPost as (p: {
          type: 'dbt-remove-model-tests';
          request: { projectName: string; modelNames: string[] };
        }) => Promise<{ success: boolean; modelsUpdated: number }>
      )({
        type: 'dbt-remove-model-tests',
        request: { projectName, modelNames },
      });

      const modelsFromGit = state.activeChanges.filter(
        (m) => m.isFromGitChanges,
      );
      const modelsNotFromGit = state.activeChanges.filter(
        (m) => !m.isFromGitChanges,
      );

      const toAvailable = modelsNotFromGit.map((m) => ({
        name: m.name,
        testCount: 0,
        testDetails: [],
        hasJoins: m.hasJoins,
        hasAggregates: m.hasAggregates,
        hasPortalPartitionDaily: m.hasPortalPartitionDaily,
        modelType: m.modelType,
        existingDataTests: [],
        fromModel: m.fromModel,
        firstJoinType: m.firstJoinType,
      }));

      const resetModelsFromGit = modelsFromGit.map((m) => ({
        ...m,
        testCount: 0,
        testDetails: [],
        existingDataTests: [],
      }));

      set({
        activeChanges: resetModelsFromGit,
        availableModels: [...toAvailable, ...state.availableModels],
      });
    } catch (error) {
      console.error('Failed to revert models:', error);
      state.revertAll();
    }
  },
  revertModelWithBackend: async (apiPost, projectName, modelName) => {
    const state = useModelTestStore.getState();
    const model = state.activeChanges.find((m) => m.name === modelName);
    if (!model) return;

    try {
      await (
        apiPost as (p: {
          type: 'dbt-remove-model-tests';
          request: { projectName: string; modelNames: string[] };
        }) => Promise<{ success: boolean; modelsUpdated: number }>
      )({
        type: 'dbt-remove-model-tests',
        request: { projectName, modelNames: [modelName] },
      });

      if (model.isFromGitChanges) {
        set({
          activeChanges: state.activeChanges.map((m) =>
            m.name === modelName
              ? {
                  ...m,
                  testCount: 0,
                  testDetails: [],
                  existingDataTests: [],
                }
              : m,
          ),
        });
      } else {
        const toAvailable = {
          name: model.name,
          testCount: 0,
          testDetails: [],
          hasJoins: model.hasJoins,
          hasAggregates: model.hasAggregates,
          hasPortalPartitionDaily: model.hasPortalPartitionDaily,
          modelType: model.modelType,
          existingDataTests: [],
          fromModel: model.fromModel,
          firstJoinType: model.firstJoinType,
        };

        set({
          activeChanges: state.activeChanges.filter(
            (m) => m.name !== modelName,
          ),
          availableModels: [toAvailable, ...state.availableModels],
        });
      }
    } catch (error) {
      console.error('Failed to revert model:', error);
      state.removeFromActiveChanges(modelName);
    }
  },
  setAvailableModels: (models) => set({ availableModels: models }),
  bulkAddAvailable: () =>
    set((state) => {
      const search = state.searchTerm.toLowerCase();
      const filtered = state.availableModels.filter((m) =>
        m.name.toLowerCase().includes(search),
      );
      if (filtered.length === 0) return state;

      const newActiveChanges = filtered.map((m) => ({
        ...m,
        testDetails: m.testDetails || [],
        hasJoins: m.hasJoins ?? false,
        hasAggregates: m.hasAggregates ?? false,
        hasPortalPartitionDaily: m.hasPortalPartitionDaily ?? false,
        modelType: m.modelType ?? null,
        existingDataTests: m.existingDataTests || [],
        fromModel: m.fromModel ?? null,
        firstJoinType: m.firstJoinType ?? null,
        checked: true,
        tests: [] as string[],
        config: { ...DEFAULT_MODEL_CONFIG },
        isFromGitChanges: false,
      }));

      return {
        activeChanges: [...newActiveChanges, ...state.activeChanges],
        availableModels: state.availableModels.filter(
          (m) => !filtered.find((f) => f.name === m.name),
        ),
      };
    }),
  reset: () => set(initialState),
}));
