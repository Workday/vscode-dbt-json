export type TableProps = {
  columns: { id: string; label: React.ReactNode }[];
  rows: { items: { element: React.ReactNode; id: string }[] }[];
};

export function Table({ columns, rows }: TableProps) {
  return (
    <table className="min-w-full">
      <thead>
        <tr className="border-b-2 border-b-background-contrast">
          {columns.map((column, index) => (
            <th
              key={index}
              scope="col"
              className="py-3.5 pl-4 pr-3 text-left text-sm font-semibold text-background-contrast sm:pl-0"
            >
              {column.label}
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="">
        {rows.map((row, index) => (
          <tr key={index}>
            {row.items.map((item, index) => {
              return (
                <td
                  key={index}
                  className="whitespace-nowrap py-4 pl-4 pr-3 text-sm font-medium text-background-contrast sm:pl-0"
                >
                  {item.element}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
