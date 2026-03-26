export interface AgentPromptMetadata {
  instruction?: string;
  inputs: { name: string | symbol; description: string }[];
  outputs: { name: string | symbol; description: string }[];
}
