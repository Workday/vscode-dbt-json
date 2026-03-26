import type {
  LightdashModel,
  LightdashPreview,
  LightdashPreviewLog,
} from '@web/pages/LightdashPreviewManager';
import { create } from 'zustand';

interface LightdashPreviewState {
  // Models
  models: LightdashModel[];
  selectedModels: Set<string>;
  isLoadingModels: boolean;

  // Preview name
  defaultPreviewName: string;
  previewNameSuffix: string;
  currentPreviewName: string | null;

  // Preview run state
  isRunning: boolean;
  logs: LightdashPreviewLog[];
  currentPreviewUrl: string | null;

  // Existing previews
  previews: LightdashPreview[];
  isLoadingPreviews: boolean;

  // Actions
  setModels: (models: LightdashModel[]) => void;
  toggleModel: (modelName: string) => void;
  selectAllModels: () => void;
  deselectAllModels: () => void;
  setSelectedModels: (modelNames: string[]) => void;
  setIsLoadingModels: (isLoading: boolean) => void;

  setDefaultPreviewName: (name: string) => void;
  setPreviewNameSuffix: (suffix: string) => void;
  setCurrentPreviewName: (name: string | null) => void;

  setIsRunning: (isRunning: boolean) => void;
  addLog: (log: LightdashPreviewLog) => void;
  clearLogs: () => void;
  setCurrentPreviewUrl: (url: string | null) => void;

  setPreviews: (previews: LightdashPreview[]) => void;
  setIsLoadingPreviews: (isLoading: boolean) => void;

  resetPreviewSection: () => void;
  reset: () => void;
}

const initialState = {
  models: [],
  selectedModels: new Set<string>(),
  isLoadingModels: false,
  defaultPreviewName: 'DJ Preview',
  previewNameSuffix: '',
  currentPreviewName: null,
  isRunning: false,
  logs: [],
  currentPreviewUrl: null,
  previews: [],
  isLoadingPreviews: false,
};

export const useLightdashPreviewStore = create<LightdashPreviewState>(
  (set) => ({
    ...initialState,

    setModels: (models) => set({ models }),

    toggleModel: (modelName) =>
      set((state) => {
        const newSelected = new Set(state.selectedModels);
        if (newSelected.has(modelName)) {
          newSelected.delete(modelName);
        } else {
          newSelected.add(modelName);
        }
        return { selectedModels: newSelected };
      }),

    selectAllModels: () =>
      set((state) => ({
        selectedModels: new Set(state.models.map((m) => m.name)),
      })),

    deselectAllModels: () => set({ selectedModels: new Set() }),

    setSelectedModels: (modelNames) =>
      set({ selectedModels: new Set(modelNames) }),

    setIsLoadingModels: (isLoading) => set({ isLoadingModels: isLoading }),

    setDefaultPreviewName: (name) => set({ defaultPreviewName: name }),

    setPreviewNameSuffix: (suffix) => set({ previewNameSuffix: suffix }),

    setCurrentPreviewName: (name) => set({ currentPreviewName: name }),

    setIsRunning: (isRunning) => set({ isRunning }),

    addLog: (log) =>
      set((state) => ({
        logs: [...state.logs, log],
      })),

    clearLogs: () => set({ logs: [] }),

    setCurrentPreviewUrl: (url) => set({ currentPreviewUrl: url }),

    setPreviews: (previews) => set({ previews }),

    setIsLoadingPreviews: (isLoading) => set({ isLoadingPreviews: isLoading }),

    resetPreviewSection: () =>
      set({
        logs: [],
        currentPreviewUrl: null,
        currentPreviewName: null,
        isRunning: false,
      }),

    reset: () => set(initialState),
  }),
);
