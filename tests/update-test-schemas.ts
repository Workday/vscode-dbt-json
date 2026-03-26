/**
 * Standalone script to update test schemas.
 * Run with: npm run update-test-schemas
 */

import { BASE_SCHEMAS_PATH, BASE_TESTS_PATH } from '@services/constants';
import * as fs from 'fs';
import * as path from 'path';

// Types
type TestParameter = {
  name: string;
  hasDefault: boolean;
  defaultValue?: string;
  isRequired: boolean;
  description?: string;
};

type ParsedTest = {
  name: string;
  parameters: TestParameter[];
  description?: string;
};

/**
 * Parses JSDoc-style comments to extract descriptions
 * Looks for @description and @param annotations
 */
function parseJSDocComments(content: string): {
  description?: string;
  paramDescriptions: Map<string, string>;
} {
  const paramDescriptions = new Map<string, string>();
  let description: string | undefined;

  // Extract the comment block before {% test %}
  const commentMatch = content.match(/\{#\s*([\s\S]*?)\s*#\}\s*\{%\s*test/);
  if (!commentMatch) {
    return { paramDescriptions };
  }

  const commentBlock = commentMatch[1];

  // Extract @description
  const descMatch = commentBlock.match(
    /@description\s+(.+?)(?=\n\s*@|\n\s*$)/s,
  );
  if (descMatch) {
    description = descMatch[1].trim().replace(/\s+/g, ' ');
  }

  // Extract @param entries
  const paramMatches = commentBlock.matchAll(
    /@param\s+(\w+)\s+(.+?)(?=\n\s*@|\n\s*(?:Expected|This test|\w+:)|$)/gs,
  );
  for (const match of paramMatches) {
    const paramName = match[1];
    const paramDesc = match[2].trim().replace(/\s+/g, ' ');
    paramDescriptions.set(paramName, paramDesc);
  }

  return { description, paramDescriptions };
}

/**
 * Parses a dbt test file to extract test name and parameters
 * Example: {% test equal_row_count(model, to) %}
 * Example: {% test no_null_aggregates(model, column_name, group_by_column, date_filter_column=None, date_filter_type="date") %}
 */
function parseTestFile(content: string): ParsedTest | null {
  // Match the test definition line
  // Pattern: {% test test_name(param1, param2, param3=default, ...) %}
  const testMatch = content.match(
    /\{%\s*test\s+([a-z_][a-z0-9_]*)\s*\(([^)]+)\)\s*%\}/i,
  );

  if (!testMatch) {
    return null;
  }

  const testName = testMatch[1];
  const paramsString = testMatch[2];

  // Parse JSDoc comments
  const { description, paramDescriptions } = parseJSDocComments(content);

  // Parse parameters
  const parameters: TestParameter[] = [];

  // Split by comma but be careful with nested quotes/parentheses
  const paramParts = paramsString.split(',').map((p) => p.trim());

  for (const param of paramParts) {
    // Skip 'model' parameter as it's implicit
    if (param === 'model') {
      continue;
    }

    // Check if parameter has default value
    const defaultMatch = param.match(/^([a-z_][a-z0-9_]*)\s*=\s*(.+)$/i);

    if (defaultMatch) {
      // Parameter with default value (optional)
      const name = defaultMatch[1];
      let defaultValue = defaultMatch[2].trim();

      // Remove quotes if present
      if (
        (defaultValue.startsWith('"') && defaultValue.endsWith('"')) ||
        (defaultValue.startsWith("'") && defaultValue.endsWith("'"))
      ) {
        defaultValue = defaultValue.slice(1, -1);
      }

      parameters.push({
        name,
        hasDefault: true,
        defaultValue,
        isRequired: false,
        description: paramDescriptions.get(name),
      });
    } else {
      // Required parameter
      parameters.push({
        name: param,
        hasDefault: false,
        isRequired: true,
        description: paramDescriptions.get(param),
      });
    }
  }

  return {
    name: testName,
    parameters,
    description,
  };
}

/**
 * Generates JSON schema entry for column-level test
 * Uses type-based format: { type: "test_name", param1: value1, ... }
 */
function generateColumnTestSchema(test: ParsedTest): object {
  const properties: Record<string, any> = {
    type: {
      const: test.name,
      ...(test.description && { description: test.description }),
    },
  };
  const required: string[] = ['type'];

  // Add all parameters as properties
  for (const param of test.parameters) {
    let propSchema: any;

    // Infer type from parameter name or default value
    if (param.name.includes('column')) {
      propSchema = { type: 'string', minLength: 1 };
    } else if (
      param.defaultValue === 'None' ||
      param.defaultValue === 'null' ||
      param.defaultValue === 'none'
    ) {
      propSchema = { type: 'string' };
    } else if (
      param.defaultValue &&
      /^(true|false)$/i.test(param.defaultValue)
    ) {
      propSchema = { type: 'boolean' };
    } else if (param.defaultValue && /^\d+$/.test(param.defaultValue)) {
      propSchema = { type: 'number' };
    } else if (param.name === 'join_type') {
      // Special case for join_type parameter
      propSchema = { type: 'string' };
    } else if (param.name.includes('type')) {
      // Enum for type parameters
      propSchema = {
        enum: ['date', 'timestamp', 'string', 'number'],
      };
    } else {
      propSchema = { type: 'string', minLength: 1 };
    }

    // Add description if available
    if (param.description) {
      propSchema.description = param.description;
    }

    properties[param.name] = propSchema;

    if (param.isRequired) {
      required.push(param.name);
    }
  }

  return {
    type: 'object',
    additionalProperties: false,
    required,
    properties,
  };
}

/**
 * Generates JSON schema entry for model-level test
 */
function generateModelTestSchema(test: ParsedTest): object {
  const properties: Record<string, any> = {
    type: {
      const: test.name,
      ...(test.description && { description: test.description }),
    },
  };
  const required: string[] = ['type'];

  // Add all parameters as properties
  for (const param of test.parameters) {
    let propSchema: any;

    if (param.name.includes('column')) {
      propSchema = { type: 'string', minLength: 1 };
    } else if (
      param.defaultValue === 'None' ||
      param.defaultValue === 'null' ||
      param.defaultValue === 'none'
    ) {
      propSchema = { type: 'string' };
    } else if (
      param.defaultValue &&
      /^(true|false)$/i.test(param.defaultValue)
    ) {
      propSchema = { type: 'boolean' };
    } else if (param.defaultValue && /^\d+$/.test(param.defaultValue)) {
      propSchema = { type: 'number' };
    } else if (param.name === 'join_type') {
      // Special case for join_type parameter
      propSchema = { type: 'string' };
    } else if (param.name.includes('type')) {
      propSchema = {
        enum: ['date', 'timestamp', 'string', 'number'],
      };
    } else {
      propSchema = { type: 'string', minLength: 1 };
    }

    // Add description if available
    if (param.description) {
      propSchema.description = param.description;
    }

    properties[param.name] = propSchema;

    if (param.isRequired) {
      required.push(param.name);
    }
  }

  return {
    type: 'object',
    additionalProperties: false,
    required,
    properties,
  };
}

function updateSchemas() {
  console.log('🔍 Scanning for generic tests...');

  const genericTestsPath = path.join(BASE_TESTS_PATH, 'generic');

  if (!fs.existsSync(genericTestsPath)) {
    console.error(`❌ Directory not found: ${genericTestsPath}`);
    process.exit(1);
  }

  const genericTestFileNames = fs.readdirSync(genericTestsPath);

  // Parse test files and collect schema definitions
  const columnTestSchemas: object[] = [];
  const modelTestSchemas: object[] = [];
  const currentTestNames: string[] = [];

  console.log(`📁 Found ${genericTestFileNames.length} test files\n`);

  for (const testFileName of genericTestFileNames) {
    if (!testFileName.endsWith('.sql')) {
      continue;
    }

    const testFilePath = path.join(genericTestsPath, testFileName);
    const testFileContent = fs.readFileSync(testFilePath, 'utf8');

    // Parse test file to extract schema information
    const parsedTest = parseTestFile(testFileContent);
    if (parsedTest) {
      console.log(`  ✓ Parsed test: ${parsedTest.name}`);
      currentTestNames.push(parsedTest.name);
      columnTestSchemas.push(generateColumnTestSchema(parsedTest));
      modelTestSchemas.push(generateModelTestSchema(parsedTest));
    } else {
      console.log(`  ⚠ Could not parse: ${testFileName}`);
    }
  }

  console.log('\n📝 Updating column.data_tests.schema.json...');

  // Update column.data_tests.schema.json
  const columnSchemaPath = path.join(
    BASE_SCHEMAS_PATH,
    'column.data_tests.schema.json',
  );
  const columnSchema = JSON.parse(fs.readFileSync(columnSchemaPath, 'utf8'));

  // Build list of all test names for string enum (includes built-in + custom tests)
  const allTestNames = [
    'not_null',
    'unique',
    'accepted_values',
    'relationships',
    ...currentTestNames,
  ].sort();

  // Remove schemas for tests that no longer exist (preserve built-in tests and string enum)
  const originalColumnCount = columnSchema.items.anyOf.length;
  columnSchema.items.anyOf = columnSchema.items.anyOf.filter((item: any) => {
    // Keep string enum for simple tests - will be updated below
    if (item.type === 'string') {
      return true;
    }
    // Keep built-in tests with special config (unique with where clause)
    if (item.type === 'object' && item.properties) {
      const testName = Object.keys(item.properties)[0];
      if (
        testName &&
        ['unique'].includes(testName) &&
        item.properties[testName].properties?.config
      ) {
        return true;
      }
      // Check if test with type field should be kept
      if (item.properties.type?.const) {
        const testType = item.properties.type.const;
        // Keep built-in dbt tests
        if (['accepted_values', 'relationships'].includes(testType)) {
          return true;
        }
        // Check custom tests
        const exists = currentTestNames.includes(testType);
        if (!exists) {
          console.log(`  - Removed: ${testType}`);
        }
        return exists;
      }
    }
    return false;
  });

  // Update string enum with all test names
  const stringEnumIndex = columnSchema.items.anyOf.findIndex(
    (item: any) => item.type === 'string',
  );
  if (stringEnumIndex >= 0) {
    columnSchema.items.anyOf[stringEnumIndex].enum = allTestNames;
    console.log(`  ↻ Updated string enum with ${allTestNames.length} tests`);
  }

  // Update or add test schemas (type-based format)
  for (const testSchema of columnTestSchemas) {
    const testName = (testSchema as any).properties.type.const;
    const existingIndex = columnSchema.items.anyOf.findIndex(
      (item: any) =>
        item.type === 'object' &&
        item.properties?.type &&
        item.properties.type.const === testName,
    );

    if (existingIndex >= 0) {
      columnSchema.items.anyOf[existingIndex] = testSchema;
      console.log(`  ↻ Updated: ${testName}`);
    } else {
      columnSchema.items.anyOf.push(testSchema);
      console.log(`  + Added: ${testName}`);
    }
  }

  fs.writeFileSync(
    columnSchemaPath,
    JSON.stringify(columnSchema, null, 2) + '\n',
  );

  const columnAdded = columnSchema.items.anyOf.length - originalColumnCount;
  console.log(
    `  ✓ Column schema updated (${columnSchema.items.anyOf.length} tests)`,
  );

  console.log('\n📝 Updating model.data_tests.schema.json...');

  // Update model.data_tests.schema.json
  const modelSchemaPath = path.join(
    BASE_SCHEMAS_PATH,
    'model.data_tests.schema.json',
  );
  const modelSchema = JSON.parse(fs.readFileSync(modelSchemaPath, 'utf8'));

  // Remove schemas for tests that no longer exist (preserve built-in tests)
  const originalModelCount = modelSchema.items.anyOf.length;
  modelSchema.items.anyOf = modelSchema.items.anyOf.filter((item: any) => {
    if (item.properties?.type?.const) {
      const testName = item.properties.type.const;
      if (testName !== 'unique') {
        const exists = currentTestNames.includes(testName);
        if (!exists) {
          console.log(`  - Removed: ${testName}`);
        }
        return exists;
      }
    }
    return true; // Keep built-in tests
  });

  // Update or add test schemas
  for (const testSchema of modelTestSchemas) {
    const testName = (testSchema as any).properties.type.const;
    const existingIndex = modelSchema.items.anyOf.findIndex(
      (item: any) =>
        item.properties?.type && item.properties.type.const === testName,
    );

    if (existingIndex >= 0) {
      modelSchema.items.anyOf[existingIndex] = testSchema;
      console.log(`  ↻ Updated: ${testName}`);
    } else {
      modelSchema.items.anyOf.push(testSchema);
      console.log(`  + Added: ${testName}`);
    }
  }

  fs.writeFileSync(
    modelSchemaPath,
    JSON.stringify(modelSchema, null, 2) + '\n',
  );

  const modelAdded = modelSchema.items.anyOf.length - originalModelCount;
  console.log(
    `  ✓ Model schema updated (${modelSchema.items.anyOf.length} tests)`,
  );

  console.log('\n✅ Schema update complete!');
  console.log(`\n📊 Summary:`);
  console.log(`   Tests found: ${currentTestNames.length}`);
  console.log(`   Tests: ${currentTestNames.join(', ')}`);
}

// Run the script
try {
  updateSchemas();
} catch (error) {
  console.error('\n❌ Error updating schemas:', error);
  process.exit(1);
}
