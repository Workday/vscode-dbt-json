import { AGENT_PROMPT_METADATA_KEY } from '@services/agent/constants';
import type { AgentPromptMetadata } from '@services/agent/types';
import * as fs from 'fs';
import * as path from 'path';

export function buildPromptFromDecorators<
  T extends { new (...args: any[]): any },
>(target: T): string {
  // Create a temporary instance to trigger field decorator initializers
  try {
    new target();
  } catch {
    // Ignore constructor errors, we just need the field decorators to run
  }

  // Try to get metadata from the class's Symbol.metadata first (new decorator format)
  let promptMetadata = (target as any)[Symbol.metadata]?.[
    AGENT_PROMPT_METADATA_KEY
  ] as AgentPromptMetadata | undefined;

  // Fallback to old format if new format not found
  promptMetadata ??= (target as any)[AGENT_PROMPT_METADATA_KEY] as
    | AgentPromptMetadata
    | undefined;

  if (!promptMetadata) {
    throw new Error(
      'No prompt metadata found. Use @Prompt, @InputField, or @OutputField decorators on the class.',
    );
  }

  const parts: string[] = [];

  // Add instruction first
  if (promptMetadata.instruction) {
    parts.push(promptMetadata.instruction);
  }

  if (promptMetadata.inputs.length > 0) {
    parts.push('\n## Inputs');
    for (const input of promptMetadata.inputs) {
      parts.push(`- **${String(input.name)}**: ${input.description}`);
    }
  }

  if (promptMetadata.outputs.length > 0) {
    parts.push('\n## Outputs');
    for (const output of promptMetadata.outputs) {
      parts.push(`- **${String(output.name)}**: ${output.description}`);
    }
  }

  return parts.join('\n');
}

let agentsMdCache: string | undefined;

export function generateAgentsMd(): string {
  agentsMdCache ??= fs.readFileSync(
    path.resolve(__dirname, '../../templates/_AGENTS.md'),
    'utf-8',
  );
  return agentsMdCache;
}
