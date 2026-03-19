import { Popover, PopoverButton, PopoverPanel } from '@headlessui/react';
import React, { useRef } from 'react';

interface HoverTooltipProps {
  children: React.ReactNode;
  content: string;
  description?: string; // Optional description text
  disabled?: boolean;
  placement?: 'right' | 'bottom' | 'top' | 'left';
  className?: string;
}

export const HoverTooltip: React.FC<HoverTooltipProps> = ({
  children,
  content,
  description,
  disabled = false,
  placement = 'right',
  className = '',
}) => {
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);
  const openTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);

  const onMouseEnter = (open: boolean) => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    if (openTimeoutRef.current) clearTimeout(openTimeoutRef.current);
    if (open || disabled) return;

    openTimeoutRef.current = setTimeout(() => {
      buttonRef.current?.click();
    }, 150);
  };

  const onMouseLeave = (close: () => void) => {
    if (openTimeoutRef.current) clearTimeout(openTimeoutRef.current);
    timeoutRef.current = setTimeout(() => {
      close();
    }, 200);
  };

  const getPositionClasses = () => {
    switch (placement) {
      case 'right':
        return {
          panel: 'left-full ml-2 top-1/2 -translate-y-1/2',
          arrow:
            'absolute right-full top-1/2 -translate-y-1/2 w-0 h-0 border-t-[6px] border-t-transparent border-b-[6px] border-b-transparent border-r-[6px] border-r-gray-800',
        };
      case 'left':
        return {
          panel: 'right-full mr-2 top-1/2 -translate-y-1/2',
          arrow:
            'absolute left-full top-1/2 -translate-y-1/2 w-0 h-0 border-t-[6px] border-t-transparent border-b-[6px] border-b-transparent border-l-[6px] border-l-gray-800',
        };
      case 'bottom':
        return {
          panel: 'top-full mt-2 left-1/2 -translate-x-1/2',
          arrow:
            'absolute bottom-full left-1/2 -translate-x-1/2 w-0 h-0 border-l-[6px] border-l-transparent border-r-[6px] border-r-transparent border-b-[6px] border-b-gray-800',
        };
      case 'top':
        return {
          panel: 'bottom-full mb-2 left-1/2 -translate-x-1/2',
          arrow:
            'absolute top-full left-1/2 -translate-x-1/2 w-0 h-0 border-l-[6px] border-l-transparent border-r-[6px] border-r-transparent border-t-[6px] border-t-gray-800',
        };
      default:
        return {
          panel: '',
          arrow: '',
        };
    }
  };

  const { panel, arrow } = getPositionClasses();

  return (
    <Popover as="div" className={`relative w-full ${className}`}>
      {({ open, close }) => (
        <div
          onMouseEnter={() => onMouseEnter(open)}
          onMouseLeave={() => onMouseLeave(close)}
          className="h-full w-full"
        >
          <PopoverButton
            ref={buttonRef}
            as="div"
            className="focus:outline-none h-full w-full"
          >
            {children}
          </PopoverButton>

          {open && !disabled && (
            <PopoverPanel static className={`absolute ${panel} z-50`}>
              <div className="relative">
                {/* Arrow */}
                <div className={arrow} />

                {/* Tooltip content */}
                <div className="bg-gray-800 text-white text-sm px-3 py-2 rounded-md shadow-lg min-w-max whitespace-nowrap">
                  <div className={description ? 'font-semibold mb-1' : ''}>
                    {content}
                  </div>
                  {description && (
                    <div className="text-xs text-gray-300 opacity-90 whitespace-normal max-w-xs">
                      {description}
                    </div>
                  )}
                </div>
              </div>
            </PopoverPanel>
          )}
        </div>
      )}
    </Popover>
  );
};
