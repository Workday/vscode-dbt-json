import { Tooltip } from '@web/elements';
import React from 'react';

import { getDisplayDataType, isComplexDataType } from '../types';

export interface DataTypeBadgeProps {
  /** The full data type string */
  dataType?: string;
  /** Additional CSS classes */
  className?: string;
  /** Whether to show the info icon for complex types */
  showInfoIcon?: boolean;
}

/**
 * A badge component for displaying data types.
 * For complex types (row, array, struct, map), shows a truncated version
 * with a tooltip showing the full definition on hover.
 */
export const DataTypeBadge: React.FC<DataTypeBadgeProps> = ({
  dataType,
  className = '',
  showInfoIcon = true,
}) => {
  if (!dataType) return null;

  const displayType = getDisplayDataType(dataType);
  const isComplex = isComplexDataType(dataType);

  return (
    <div className={`flex items-center gap-1 flex-shrink-0 ${className}`}>
      <span
        className="text-[11px] font-medium text-surface-contrast uppercase tracking-wider"
        title={!isComplex ? dataType : undefined}
      >
        {displayType}
      </span>
      {/* Info icon for complex types with tooltip */}
      {isComplex && showInfoIcon && (
        <Tooltip
          content={
            <div className="font-mono text-xs break-all whitespace-pre-wrap max-w-[300px]">
              {dataType}
            </div>
          }
        />
      )}
    </div>
  );
};

export default DataTypeBadge;
