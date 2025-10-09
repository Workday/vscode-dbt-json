import type { GitAction } from '@shared/git/types';

export function gitLastLog(logs: string): { action: GitAction; line: string } {
  const lines = logs.toString().split('\n').filter(Boolean);
  const line = lines.pop() || '';
  const action = /\t((?:[a-z]|-|\(|\)|\s)+): .+$/
    .exec(line)?.[1]
    ?.split(' ')[0] as GitAction;
  return { action, line };
}
