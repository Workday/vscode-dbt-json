import { AGENT_PROMPT_METADATA_KEY } from '@services/agent/constants';
import type { AgentPromptMetadata } from '@services/agent/types';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getOrCreatePromptMetadata(target: any): AgentPromptMetadata {
  target[AGENT_PROMPT_METADATA_KEY] ??= {
    instruction: undefined,
    inputs: [],
    outputs: [],
  };
  return target[AGENT_PROMPT_METADATA_KEY];
}

/**
 * Class decorator for defining AI agent prompts.
 * Stores instruction metadata on the class.
 */
export function Prompt(instruction: string) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return function <T extends new (...args: any[]) => object>(
    constructor: T,
    _context: ClassDecoratorContext,
  ) {
    // Store metadata directly on constructor for modern decorators
    const metadata = getOrCreatePromptMetadata(constructor);
    metadata.instruction = instruction;
    return constructor;
  };
}

/**
 * Property decorator for marking input fields.
 * Stores field name and description in class metadata.
 */
export function InputField(description: string) {
  return function (
    _target: undefined,
    context: ClassFieldDecoratorContext,
  ): void {
    // Store metadata on the class constructor via addInitializer
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    context.addInitializer(function (this: any) {
      const metadata = getOrCreatePromptMetadata(this.constructor);
      metadata.inputs.push({ name: String(context.name), description });
    });
  };
}

/**
 * Property decorator for marking output fields.
 * Stores field name and description in class metadata.
 */
export function OutputField(description: string) {
  return function (
    _target: undefined,
    context: ClassFieldDecoratorContext,
  ): void {
    // Store metadata on the class constructor via addInitializer
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    context.addInitializer(function (this: any) {
      const metadata = getOrCreatePromptMetadata(this.constructor);
      metadata.outputs.push({ name: String(context.name), description });
    });
  };
}
