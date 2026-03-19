import { Popover, PopoverButton, PopoverPanel } from '@headlessui/react';
import { makeClassName } from '@web';
import HierarchyIcon from '@web/assets/icons/hierarchy.svg?react';
import { Button } from '@web/elements/Button';
import { ButtonGroup } from '@web/elements/ButtonGroup';
import { NumberStepper } from '@web/elements/NumberStepper';
import { useEffect, useState } from 'react';

import { MIN_LINEAGE_DEPTH } from '../constants';

const MODE_CUSTOM = 'Custom Levels';
const MODE_FULL = 'Full Lineage';

interface LineageDepthPopoverProps {
  /** Current upstream levels */
  upstreamLevels: number;
  /** Current downstream levels */
  downstreamLevels: number;
  /** Whether full lineage mode is active */
  isFullLineage: boolean;
  /** Whether the control is disabled */
  disabled?: boolean;
  /** Called when custom levels are applied */
  onApply: (upstream: number, downstream: number) => void;
  /** Called when full lineage is selected */
  onFullLineage: () => void;
}

export function LineageDepthPopover({
  upstreamLevels,
  downstreamLevels,
  isFullLineage,
  disabled,
  onApply,
  onFullLineage,
}: LineageDepthPopoverProps) {
  // Local state for editing (only used in Custom Levels mode)
  const [localUpstream, setLocalUpstream] = useState(upstreamLevels);
  const [localDownstream, setLocalDownstream] = useState(downstreamLevels);
  const [mode, setMode] = useState(isFullLineage ? MODE_FULL : MODE_CUSTOM);

  // Sync local state when props change
  useEffect(() => {
    setLocalUpstream(upstreamLevels);
    setLocalDownstream(downstreamLevels);
  }, [upstreamLevels, downstreamLevels]);

  useEffect(() => {
    setMode(isFullLineage ? MODE_FULL : MODE_CUSTOM);
  }, [isFullLineage]);

  // Button label
  const buttonLabel = isFullLineage
    ? 'Lineage depth (Full)'
    : `Lineage depth (${upstreamLevels} ↑ ${downstreamLevels} ↓)`;

  const handleModeChange = (newMode: string, close: () => void) => {
    setMode(newMode);
    if (newMode === MODE_FULL) {
      // Immediately compute full lineage and close
      onFullLineage();
      close();
    }
  };

  const handleApply = (close: () => void) => {
    onApply(localUpstream, localDownstream);
    close();
  };

  return (
    <Popover className="relative">
      {({ close }) => (
        <>
          <PopoverButton
            disabled={disabled}
            className={makeClassName(
              'flex items-center gap-2 text-sm',
              'text-surface-contrast hover:bg-neutral transition-colors',
              'disabled:opacity-50 disabled:cursor-not-allowed',
            )}
          >
            <HierarchyIcon className="w-4 h-4" />
            <span>{buttonLabel}</span>
          </PopoverButton>

          <PopoverPanel
            anchor="bottom start"
            className="mt-4 w-72 bg-card border border-neutral rounded-lg shadow-lg p-4 flex flex-col gap-4"
          >
            {/* Mode toggle */}
            <ButtonGroup
              options={[MODE_CUSTOM, MODE_FULL]}
              initialValue={mode}
              onSelect={(value) => handleModeChange(value, close)}
              className="w-full"
            />

            {/* Custom levels controls */}
            {mode === MODE_CUSTOM && (
              <>
                <div className="flex flex-col gap-3">
                  <NumberStepper
                    label="↑ Ancestors:"
                    value={localUpstream}
                    onChange={setLocalUpstream}
                    min={MIN_LINEAGE_DEPTH}
                  />
                  <NumberStepper
                    label="↓ Descendants:"
                    value={localDownstream}
                    onChange={setLocalDownstream}
                    min={MIN_LINEAGE_DEPTH}
                  />
                </div>

                <Button
                  variant="primary"
                  label="Apply"
                  onClick={() => handleApply(close)}
                  fullWidth
                  className="!py-2"
                />
              </>
            )}
          </PopoverPanel>
        </>
      )}
    </Popover>
  );
}
