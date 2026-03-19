import { describe, expect, test } from '@jest/globals';
import { InputField, OutputField, Prompt } from '@services/agent/decorators';
import { CreateModelSignature } from '@services/agent/prompts/createmodel';
import { UpdateAIHintsSignature } from '@services/agent/prompts/updateaihints';
import { buildPromptFromDecorators } from '@services/agent/utils';

describe('Decorator System', () => {
  test('should extract metadata from decorated class', () => {
    @Prompt('Test prompt instruction')
    class TestSignature {
      @InputField('Test input description')
      inputField: string | undefined;

      @OutputField('Test output description')
      outputField: string | undefined;
    }

    const promptText = buildPromptFromDecorators(TestSignature);

    expect(promptText).toContain('Test prompt instruction');
    expect(promptText).toContain('## Inputs');
    expect(promptText).toContain('- **inputField**: Test input description');
    expect(promptText).toContain('## Outputs');
    expect(promptText).toContain('- **outputField**: Test output description');
  });

  test('should work with UpdateAIHintsSignature', () => {
    const promptText = buildPromptFromDecorators(UpdateAIHintsSignature);

    expect(promptText).toContain('Automate adding ai hints');
    expect(promptText).toContain('## Inputs');
    expect(promptText).toContain(
      '- **filePath**: The File path for excel sheet with ai hints.',
    );
    expect(promptText).toContain(
      '- **sheetName**: Sheet name(s) in the excel file. Comma separated if multiple.',
    );
    expect(promptText).toContain(
      '- **baseModel**: Base Model that we trying to update ai hints for.',
    );
    expect(promptText).toContain(
      '- **tag**: The tag to filter models to apply them for AI Agent in Lightdash.',
    );
  });

  test('should work with CreateModelSignature', () => {
    const promptText = buildPromptFromDecorators(CreateModelSignature);

    expect(promptText).toContain(
      'Create a model json for getting started with new model development',
    );
    expect(promptText).toContain('## Inputs');
    expect(promptText).toContain(
      '- **type**: Type of the model - mart, staging, intermediate, source, etc.',
    );
    expect(promptText).toContain(
      '- **group**: Group of the model - analytics, finops, sales, marketing, etc.',
    );
    expect(promptText).toContain(
      '- **topic**: Topic of the model - aws_cur, gcp_billing, salesforce, etc.',
    );
    expect(promptText).toContain(
      '- **name**: Name of the model - accounts_billing_daily, opportunities_facts, etc.',
    );
  });
});
