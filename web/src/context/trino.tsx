import { useState } from 'react';

import { TrinoContext, type TrinoValue } from './TrinoContext';

export function TrinoProvider({
  children,
  initialValue,
}: {
  children: React.ReactNode;
  initialValue: Partial<TrinoValue>;
}) {
  const [tables, setTables] = useState<string[] | null>(
    initialValue.tables || [
      'sample_table_1_with_name',
      'sample_table_2_with_name',
      'sample_table_3_with_name',
      'sample_table_4_with_name',
      'sample_table_5_with_name',
      'sample_table_6_with_name',
      'sample_table_7_with_name',
      'sample_table_8_with_name',
      'sample_table_9_with_name',
      'sample_table_10_with_name',
    ],
  );

  const value: TrinoValue = { setTables, tables };

  return (
    <TrinoContext.Provider value={value}>{children}</TrinoContext.Provider>
  );
}
