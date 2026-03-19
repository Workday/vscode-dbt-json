import { Table } from '@web/elements';

import DataTypeBadge from '../DataModeling/components/DataTypeBadge';

interface ColumnMetadata {
  name: string;
  description: string;
  type: 'dim' | 'fct';
  dataType: string;
}

interface MetaInformationTableProps {
  columns: ColumnMetadata[];
}

export function MetaInformationTable({ columns }: MetaInformationTableProps) {
  // Handle empty state
  if (!columns || columns.length === 0) {
    return (
      <div className="flex items-center justify-center h-40 text-gray-500">
        <p>No column information available</p>
      </div>
    );
  }

  // Define table columns
  const tableColumns = [
    { id: 'name', label: 'Column Name' },
    { id: 'description', label: 'Description' },
    { id: 'type', label: 'Column Type' },
    { id: 'dataType', label: 'Data Type' },
  ];

  // Transform column data into table rows
  const tableRows = columns.map((column, index) => ({
    items: [
      {
        id: `name-${index}`,
        element: (
          <span className="font-mono text-sm font-medium">{column.name}</span>
        ),
      },
      {
        id: `description-${index}`,
        element: <span className="text-sm">{column.description || '-'}</span>,
      },
      {
        id: `type-${index}`,
        element: (
          <span
            className={`inline-flex items-center px-2.5 py-0.5 rounded-md text-xs font-medium ${
              column.type === 'fct'
                ? 'bg-green-100 text-green-800'
                : 'bg-blue-100 text-blue-800'
            }`}
          >
            {column.type}
          </span>
        ),
      },
      {
        id: `dataType-${index}`,
        element: (
          <DataTypeBadge dataType={column.dataType} className="text-sm" />
        ),
      },
    ],
  }));

  return (
    <div className="overflow-auto h-full">
      <Table columns={tableColumns} rows={tableRows} />
    </div>
  );
}
