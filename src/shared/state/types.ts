export interface FormStateData {
  data: Record<string, any>;
  lastModified: string;
}

export interface FormStatesFile {
  version: string;
  lastUpdated: string;
  forms: Record<string, FormStateData>;
}

export const FORM_STATES_FILE_VERSION = '1.0.0';

export type StateMessage =
  | { type: 'state:load'; formType: string }
  | { type: 'state:save'; formType: string; data: Record<string, any> }
  | { type: 'state:clear'; formType: string }
  | { type: 'state:loaded'; formType: string; data: Record<string, any> | null }
  | { type: 'state:saved'; formType: string; success: boolean; error?: string }
  | {
      type: 'state:cleared';
      formType: string;
      success: boolean;
      error?: string;
    }
  | { type: 'state:error'; formType: string; error: string };
