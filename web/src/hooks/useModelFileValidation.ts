import { useApp } from '@web/context';
import { useModelStore } from '@web/stores/useModelStore';
import {
  generateModelFileName,
  hasRequiredFields,
} from '@web/utils/modelFileName';
import { useEffect, useState } from 'react';

import { useDebounce } from './useDebounce';

interface ModelFileValidationResult {
  fileName: string;
  isValidating: boolean;
  fileExists: boolean;
  filePath: string;
  isReady: boolean;
}

/**
 * Custom hook to validate if a model file already exists
 * Watches basicFields from ModelStore and checks file existence with debouncing
 *
 * @param mode - 'create' or 'edit' mode (validation is skipped in edit mode)
 * @returns Validation result with file name, existence status, and loading state
 */
export function useModelFileValidation(
  mode: 'create' | 'edit' = 'create',
): ModelFileValidationResult {
  const { api } = useApp();
  const basicFields = useModelStore((s) => s.basicFields);

  const [isValidating, setIsValidating] = useState(false);
  const [fileExists, setFileExists] = useState(false);
  const [filePath, setFilePath] = useState('');

  // Generate file name from current fields
  const fileName = generateModelFileName(basicFields);
  const isReady = hasRequiredFields(basicFields);

  // Debounce the fields to avoid excessive API calls
  const debouncedFields = useDebounce(
    JSON.stringify({
      type: basicFields.type,
      group: basicFields.group,
      topic: basicFields.topic,
      name: basicFields.name,
    }),
    1000,
  );

  useEffect(() => {
    // Skip validation in edit mode
    if (mode === 'edit') {
      setFileExists(false);
      setFilePath('');
      return;
    }

    // Only validate if all required fields are present
    if (!isReady) {
      setFileExists(false);
      setFilePath('');
      return;
    }

    // Validate file existence
    const validateFile = async () => {
      setIsValidating(true);

      try {
        const fields = JSON.parse(debouncedFields);

        const response = await api.post({
          type: 'framework-check-model-exists',
          request: {
            projectName: basicFields.projectName,
            modelJson: {
              type: fields.type,
              group: fields.group,
              topic: fields.topic,
              name: fields.name,
            },
          },
        });

        setFileExists(response.exists);
        setFilePath(response.filePath);
      } catch (error) {
        console.error('Error checking model file existence:', error);
        // On error, assume file doesn't exist to avoid blocking the user
        setFileExists(false);
        setFilePath('');
      } finally {
        setIsValidating(false);
      }
    };

    void validateFile();
  }, [debouncedFields, isReady, basicFields.projectName, mode, api]);

  return {
    fileName,
    isValidating,
    fileExists,
    filePath,
    isReady,
  };
}
