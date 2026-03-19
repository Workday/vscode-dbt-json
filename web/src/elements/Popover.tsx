import {
  Popover as HeadlessPopover,
  PopoverButton,
  PopoverPanel,
} from '@headlessui/react';
import { ChevronDownIcon } from '@heroicons/react/24/outline';
import React from 'react';

interface PopoverProps {
  trigger: React.ReactNode;
  children: React.ReactNode | ((close: () => void) => React.ReactNode);
  className?: string;
  panelClassName?: string;
  showChevron?: boolean;
  placement?: 'left' | 'right' | 'center';
  anchor?: 'bottom' | 'top' | 'left' | 'right' | 'bottom start' | 'bottom end';
}

export const Popover: React.FC<PopoverProps> = ({
  trigger,
  children,
  className = '',
  panelClassName = '',
  showChevron = false,
  placement = 'right',
  anchor,
}) => {
  const getPlacementClass = () => {
    switch (placement) {
      case 'left':
        return 'left-0';
      case 'center':
        return 'left-1/2 -translate-x-1/2';
      case 'right':
      default:
        return 'right-0';
    }
  };

  return (
    <HeadlessPopover className={`relative ${className}`}>
      <PopoverButton className="flex items-center gap-1 focus:outline-none">
        {trigger}
        {showChevron && <ChevronDownIcon className="w-3 h-3" />}
      </PopoverButton>
      <PopoverPanel
        anchor={anchor}
        className={`${anchor ? '' : `absolute ${getPlacementClass()} top-full mt-2`} bg-background border border-border rounded-md shadow-lg z-50 ${panelClassName}`}
      >
        {({ close }) => (
          <>{typeof children === 'function' ? children(close) : children}</>
        )}
      </PopoverPanel>
    </HeadlessPopover>
  );
};
