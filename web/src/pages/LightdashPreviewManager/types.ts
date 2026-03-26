export type LightdashModel = {
  name: string;
  tags: string[];
  description?: string;
};

export type LightdashPreview = {
  name: string;
  url: string;
  createdAt: string;
  models: string[];
  status: 'active' | 'inactive';
};

export type LightdashPreviewLog = {
  level: 'info' | 'success' | 'error' | 'warning';
  message: string;
  timestamp: string;
  isProgress?: boolean;
  isPreviewSuccess?: boolean;
};
