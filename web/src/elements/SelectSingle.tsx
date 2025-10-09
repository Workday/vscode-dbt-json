import {
  Combobox,
  ComboboxButton,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
  Label,
} from '@headlessui/react';
import {
  CheckIcon,
  ChevronUpDownIcon,
  XMarkIcon,
} from '@heroicons/react/20/solid';
import { makeClassName } from '@web';
import { Tooltip } from '@web/elements';
import { useState } from 'react';

type Option = { label: string; value: string };

export type SelectSingleProps = {
  disabled?: boolean;
  error?: boolean | string;
  innerRef?: React.Ref<HTMLSelectElement>;
  label?: string;
  onBlur: () => void;
  onChange: (value: Option | null) => void;
  options: Option[];
  value: Option | null;
  tooltipText?: string;
};

export function SelectSingle({
  disabled,
  error,
  label,
  options,
  onBlur,
  onChange,
  value = null,
  tooltipText,
}: SelectSingleProps) {
  const [query, setQuery] = useState('');

  const filteredOptions =
    query === ''
      ? options
      : options.filter((o) => {
          return o.label.toLowerCase().includes(query.toLowerCase());
        });

  return (
    <Combobox<Option | null>
      disabled={disabled}
      immediate
      onChange={(o) => {
        setQuery('');
        onChange(o);
      }}
      value={value}
    >
      {label && (
        <Label className="block text-sm/6 leading-6 mt-2 text-background-contrast flex gap-1 items-center">
          {label}
          {tooltipText && <Tooltip content={tooltipText} />}
        </Label>
      )}

      <div className="relative mt-2">
        <ComboboxInput<Option | null>
          autoComplete="one-time-code" // Should disable autofill in browser
          className={makeClassName(
            'w-full rounded-md border-0 bg-background py-1.5 pl-3 pr-10 text-background-contrast text-sm shadow-sm ring-1 ring-inset ring-background-contrast',
            'focus:ring-2 focus:ring-inset focus:ring-primary',
            error && 'ring-error',
          )}
          displayValue={(o) => o?.label || ''}
          onChange={(e) => setQuery(e.target.value)}
          onBlur={() => {
            setQuery('');
            onBlur();
          }}
        />
        <XMarkIcon
          aria-hidden="true"
          className={makeClassName(
            'absolute cursor-pointer inset-y-0 right-7 flex items-center rounded-r-md top-1',
            'focus:outline-none',
            'h-6 stroke-background-contrast text-background-contrast',
          )}
          onClick={() => {
            setQuery('');
            onChange(null);
          }}
        />
        <ComboboxButton
          className={makeClassName(
            'absolute inset-y-0 right-0 flex items-center rounded-r-md px-2',
            'focus:outline-none',
          )}
        >
          <ChevronUpDownIcon
            className="h-5 w-5 text-background-contrast"
            aria-hidden="true"
          />
        </ComboboxButton>
        {filteredOptions.length > 0 && (
          <ComboboxOptions
            className={makeClassName(
              'absolute z-10 mt-1 max-h-96 w-full overflow-auto rounded-md bg-background py-1 text-base shadow-lg ring-1 ring-background-contrast ring-opacity-5',
            )}
          >
            {filteredOptions.map((o) => (
              <ComboboxOption
                key={o.value}
                value={o}
                className={({ focus }) =>
                  makeClassName(
                    'relative cursor-default select-none py-2 pl-3 pr-9 text-background-contrast text-sm',
                    focus
                      ? 'bg-primary text-primary-contrast'
                      : 'text-background-contrast',
                  )
                }
              >
                {({ focus, selected }) => (
                  <>
                    <span
                      className={makeClassName(
                        'block truncate',
                        selected && 'font-semibold',
                      )}
                    >
                      {o.label}
                    </span>
                    {selected && (
                      <span
                        className={makeClassName(
                          'absolute inset-y-0 right-0 flex items-center pr-4',
                          focus ? 'text-background-contrast' : 'text-primary',
                        )}
                      >
                        <CheckIcon className="h-5 w-5" aria-hidden="true" />
                      </span>
                    )}
                  </>
                )}
              </ComboboxOption>
            ))}
          </ComboboxOptions>
        )}
      </div>
    </Combobox>
  );
}
