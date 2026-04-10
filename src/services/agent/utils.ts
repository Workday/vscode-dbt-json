import * as fs from 'fs';
import * as path from 'path';

let agentsMdCache: string | undefined;

export function generateAgentsMd(): string {
  agentsMdCache ??= fs.readFileSync(
    path.resolve(__dirname, '../../templates/_AGENTS.md'),
    'utf-8',
  );
  return agentsMdCache;
}
