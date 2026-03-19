import { Menu, MenuButton, MenuItem, MenuItems } from '@headlessui/react';
import { ArrowDownTrayIcon } from '@heroicons/react/24/outline';
import { makeClassName } from '@web';

/** A single export option in the dropdown */
export interface ExportOption {
  /** Unique identifier for the option */
  id: string;
  /** Display label for the option */
  label: string;
  /** Whether this option is disabled */
  disabled?: boolean;
  /** Called when this option is clicked */
  onClick: () => void;
}

interface ExportDropdownProps {
  /** List of export options to display */
  options: ExportOption[];
  /** Whether the entire dropdown is disabled */
  disabled?: boolean;
  /** Optional custom button label (defaults to "Export") */
  buttonLabel?: string;
}

export function ExportDropdown({
  options,
  disabled,
  buttonLabel = 'Export',
}: ExportDropdownProps) {
  return (
    <Menu as="div" className="relative">
      <MenuButton
        disabled={disabled}
        className={makeClassName(
          'flex items-center gap-1 text-xs text-surface-contrast',
          'disabled:opacity-50 disabled:cursor-not-allowed',
        )}
      >
        <ArrowDownTrayIcon className="w-4 h-4" />
        <span>{buttonLabel}</span>
      </MenuButton>

      <MenuItems
        anchor="bottom start"
        className={makeClassName(
          'z-50 mt-4 w-48 rounded border border-neutral bg-card shadow-lg',
          'focus:outline-none',
        )}
      >
        {options.map((option) => (
          <MenuItem key={option.id}>
            {({ focus }) => (
              <button
                type="button"
                onClick={option.onClick}
                disabled={option.disabled}
                className={makeClassName(
                  'w-full px-3 py-2 text-left text-sm',
                  focus ? 'bg-neutral' : '',
                  option.disabled
                    ? 'text-surface-contrast/50 cursor-not-allowed'
                    : 'text-surface-contrast',
                )}
              >
                {option.label}
              </button>
            )}
          </MenuItem>
        ))}
      </MenuItems>
    </Menu>
  );
}
