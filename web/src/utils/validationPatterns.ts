/**
 * Centralized validation patterns for model fields.
 * These patterns match the JSON schema definitions in /schemas/*.schema.json
 *
 * Filename format: {layer}__{group}__{topic}__{name}.model.json
 * - Positions 0-2 (layer, group, topic): Must NOT contain __
 * - Position 3+ (name): May contain __
 */

/**
 * Pattern for model name field.
 * - Allows lowercase letters, numbers, and underscores
 * - Cannot start or end with underscore
 * - CAN contain double underscores (__)
 *
 * JSON Schema equivalent: ^(?!_)(?!.*_$)([a-z]|[0-9]|_)+$
 */
export const MODEL_NAME_PATTERN = /^(?!_)[a-z0-9_]+(?<!_)$/;

/**
 * Pattern for topic and group fields.
 * - Allows lowercase letters, numbers, and underscores
 * - Cannot start or end with underscore
 * - CANNOT contain double underscores (__)
 *
 * JSON Schema equivalent: ^(?!.*__.*)(?!_)(?!.*_$)([a-z]|[0-9]|_)+$
 */
export const MODEL_TOPIC_GROUP_PATTERN = /^(?!.*__)[a-z0-9_]+(?<!_)(?<!^_)$/;

/**
 * Maximum length for model field values
 */
export const MODEL_FIELD_MAX_LENGTH = 127;

/**
 * Validation messages
 */
export const VALIDATION_MESSAGES = {
  name: 'Name can only contain lowercase letters, numbers, and underscores. It cannot start or end with an underscore.',
  topic:
    'Topic can only contain lowercase letters, numbers, and underscores. It cannot start or end with an underscore. Double underscores are not allowed.',
  group:
    'Group can only contain lowercase letters, numbers, and underscores. It cannot start or end with an underscore. Double underscores are not allowed.',
  maxLength: (field: string) =>
    `${field} must be ${MODEL_FIELD_MAX_LENGTH} characters or fewer`,
};
