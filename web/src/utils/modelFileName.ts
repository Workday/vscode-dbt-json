import {
  MODEL_FIELD_MAX_LENGTH,
  MODEL_NAME_PATTERN,
  MODEL_TOPIC_GROUP_PATTERN,
} from './validationPatterns';

/**
 * Utility function to generate model file name from model fields
 * This mirrors the backend logic in frameworkGetModelName
 */

type ModelFields = {
  type?: string;
  group?: string;
  topic?: string;
  name?: string;
};

/**
 * Get model layer from type (stg, int, mart)
 */
function getModelLayer(type: string): 'stg' | 'int' | 'mart' | null {
  if (!type) return null;
  const layer = type.split('_')[0];
  if (layer === 'stg' || layer === 'int' || layer === 'mart') {
    return layer;
  }
  return null;
}

/**
 * Generate the model file name based on model fields
 * Returns the file name with .model.json extension
 * Returns empty string if required fields are missing
 *
 * @param modelFields - Object containing type, group, topic, and name
 * @returns File name string or empty string if validation fails
 */
export function generateModelFileName(modelFields: ModelFields): string {
  const { type, group, topic, name } = modelFields;

  // Check if all required fields are present
  if (!type || !group || !topic || !name) {
    return '';
  }

  // Get the model layer from type
  const modelLayer = getModelLayer(type);
  if (!modelLayer) {
    return '';
  }

  // Topic no longer supports slashes, use as-is
  const normalizedTopic = topic || '';

  // Build the file name: <layer>__<group>__<topic>__<name>.model.json
  const fileName = `${modelLayer}__${group}__${normalizedTopic}__${name}.model.json`;

  return fileName;
}

/**
 * Check if all required fields for file name generation are present and valid
 * Validates that name and topic match the same patterns used in input fields
 */
export function hasRequiredFields(modelFields: ModelFields): boolean {
  const { type, group, topic, name } = modelFields;

  // Check if all fields are present
  if (!type || !group || !topic || !name) {
    return false;
  }

  // Validate name
  if (!MODEL_NAME_PATTERN.test(name) || name.length > MODEL_FIELD_MAX_LENGTH) {
    return false;
  }

  // Validate topic
  if (
    !MODEL_TOPIC_GROUP_PATTERN.test(topic) ||
    topic.length > MODEL_FIELD_MAX_LENGTH
  ) {
    return false;
  }

  return true;
}
