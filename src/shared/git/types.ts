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
