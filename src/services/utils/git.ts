/**
 * Git utilities for extension-only operations.
 */

export type GitAction =
  | 'checkout'
  | 'commit'
  //   | 'commit (merge)'
  | 'fast-forward'
  | 'pull'
  | 'rebase'
  //   | 'rebase (pick)'
  //   | 'rebase (finish)'
  //   | 'rebase (start)'
  | 'reset';

/**
 * Parse the last git log entry to extract the action and full line.
 *
 * @param logs - Content of .git/logs/HEAD file
 * @returns Object containing the git action and the full log line
 *
 * @example
 * ```typescript
 * const logs = fs.readFileSync('.git/logs/HEAD', 'utf8');
 * const { action, line } = gitLastLog(logs);
 * console.log('Last git action:', action); // 'checkout', 'pull', etc.
 * ```
 */
export function gitLastLog(logs: string): { action: GitAction; line: string } {
  const lines = logs.toString().split('\n').filter(Boolean);
  const line = lines.pop() ?? '';
  const action = /\t((?:[a-z]|-|\(|\)|\s)+): .+$/
    .exec(line)?.[1]
    ?.split(' ')[0] as GitAction;
  return { action, line };
}
