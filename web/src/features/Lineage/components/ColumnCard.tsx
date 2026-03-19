import { makeClassName } from '@web';
import DataTypeBadge from '@web/features/DataModeling/components/DataTypeBadge';
import React, { useMemo } from 'react';

export interface ColumnCardProps {
  /** Column name */
  name: string;
  /** Data type (e.g., "TEXT", "INTEGER") */
  dataType?: string;
  /** Column description */
  description?: string;
  /** Whether this card is currently selected */
  isSelected?: boolean;
  /** Click handler */
  onClick?: () => void;
}

/** Map of data type patterns to icons */
const DATA_TYPE_ICONS: Array<{ patterns: string[]; icon: string }> = [
  { patterns: ['STRING', 'TEXT', 'VARCHAR', 'CHAR'], icon: 'A' },
  {
    patterns: [
      'INT',
      'FLOAT',
      'DOUBLE',
      'DECIMAL',
      'NUMERIC',
      'NUMBER',
      'REAL',
    ],
    icon: '#',
  },
  { patterns: ['BOOL'], icon: '◉' },
  { patterns: ['TIMESTAMP', 'DATETIME'], icon: '⏱' },
  { patterns: ['DATE'], icon: '📅' },
  { patterns: ['JSON'], icon: '{}' },
  { patterns: ['ARRAY'], icon: '[]' },
  { patterns: ['ROW', 'STRUCT'], icon: '{}' },
  { patterns: ['BINARY', 'BYTES', 'BLOB'], icon: '01' },
  { patterns: ['GEO', 'GEOGRAPHY', 'GEOMETRY'], icon: '🌐' },
];

/** Get the icon character to display based on the column data type. */
function getDataTypeIcon(dataType?: string): string {
  if (!dataType) return '?';

  const type = dataType.toUpperCase();
  const match = DATA_TYPE_ICONS.find(({ patterns }) =>
    patterns.some((p) => type.includes(p)),
  );

  return match?.icon ?? '?';
}

/**
 * A card component for displaying column information in a selection list.
 * Used in column lineage and model lineage features.
 */
export const ColumnCard: React.FC<ColumnCardProps> = ({
  name,
  dataType,
  description,
  isSelected = false,
  onClick,
}) => {
  const typeIcon = useMemo(() => getDataTypeIcon(dataType), [dataType]);

  return (
    <div
      onClick={onClick}
      className={makeClassName(
        'flex flex-col gap-2',
        'p-3 rounded cursor-pointer transition-colors duration-150',
        isSelected
          ? 'bg-primary/20 border border-primary'
          : 'bg-card border border-neutral hover:bg-surface',
      )}
    >
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-2 overflow-hidden">
          {/* Column type icon */}
          <div className="w-6 h-6 rounded bg-surface flex items-center justify-center text-xs font-semibold text-foreground">
            {typeIcon}
          </div>
          {/* Column name */}
          <span
            className={makeClassName(
              'text-[13px] font-medium truncate',
              isSelected ? 'text-primary' : 'text-foreground',
            )}
            title={name}
          >
            {name}
          </span>
        </div>
        {/* Data type badge */}
        <DataTypeBadge dataType={dataType} />
      </div>
      {/* Description */}
      {description && (
        <div className="text-xs text-surface-contrast leading-relaxed">
          {description}
        </div>
      )}
    </div>
  );
};
