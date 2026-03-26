import { Field, Label } from '@headlessui/react';
import type { TableProps } from '@web/elements';
import { Button, Table } from '@web/elements';
import React, { forwardRef, useMemo } from 'react';
import type {
  ArrayPath,
  Control,
  // ControllerRenderProps,
  FieldArray,
  FieldValues,
  Path,
} from 'react-hook-form';
import { Controller, useFieldArray } from 'react-hook-form';

import { FieldInputText } from './FieldInputText';
import { FieldSelectSingle } from './FieldSelectSingle';

type Column<T extends FieldValues> =
  | {
      label: string;
      name: string | ArrayPath<T>;
      type: 'input_text';
    }
  | {
      label: string;
      name: string | ArrayPath<T>;
      options: Option[];
      type: 'select_single';
    };

type Option = { label: string; value: string };

export type FieldArrayTableProps<T extends FieldValues = FieldValues> = {
  columns: Column<T>[];
  control: Control<T>;
  label?: string;
  labelAdd?: string;
  name: string;
};

const _FieldArrayTable = <T extends FieldValues>(
  { columns, control, label, labelAdd, name }: FieldArrayTableProps<T>,
  _ref: React.ForwardedRef<HTMLElement>,
) => {
  const _name = name as ArrayPath<T>;
  const { append, fields } = useFieldArray({
    control,
    name: _name,
  });

  const DEFAULT = { column: '' } as FieldArray<T, ArrayPath<T>>;

  const rows: TableProps['rows'] = useMemo(
    () =>
      fields.map((field, index) => {
        return {
          items: columns.map((column) => {
            switch (column.type) {
              case 'input_text': {
                return {
                  element: (
                    <Controller
                      control={control}
                      key={field.id}
                      name={`${name}.${index}.${column.name}` as Path<T>}
                      render={({ field }) => (
                        <FieldInputText {...field} label={column.label} />
                      )}
                    />
                  ),
                  id: column.label,
                };
              }
              case 'select_single': {
                return {
                  element: (
                    <Controller
                      control={control}
                      key={field.id}
                      name={`${name}.${index}.${column.name}` as Path<T>}
                      render={({ field }) => (
                        <FieldSelectSingle
                          {...field}
                          options={column.options}
                        />
                      )}
                    />
                  ),
                  id: column.label,
                };
              }
            }
          }),
        };
      }),
    [columns, control, fields, name],
  );

  return (
    <Field className="flex-col">
      <Label className="block text-md leading-6 mt-2">{label}</Label>
      <Table
        columns={columns.map((c) => ({ label: c.label, id: c.name }))}
        rows={rows}
      />
      <Button
        fullWidth
        label={labelAdd || 'Add Row'}
        onClick={() => {
          append(DEFAULT);
        }}
        variant="secondary"
      />
    </Field>
  );
};

export const FieldArrayTable = forwardRef(_FieldArrayTable) as <
  T extends FieldValues,
>(
  props: FieldArrayTableProps<T> & { ref?: React.Ref<HTMLElement> },
) => ReturnType<typeof _FieldArrayTable>;
