import { Popover, PopoverButton, PopoverPanel } from '@headlessui/react';
import { InformationCircleIcon } from '@heroicons/react/24/outline';
import { useRef, type FC, type ReactNode } from 'react';

export type TooltipProps = {
  children?: ReactNode;
  content: ReactNode;
  align?: 'center' | 'start';
};

export const Tooltip: FC<TooltipProps> = ({
  children,
  content,
  align = 'start',
}) => {
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);
  const openTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Ref for the PopoverButton to programmatically control it
  const buttonRef = useRef<HTMLButtonElement>(null);

  const onMouseEnter = (open: boolean) => {
    // Clear any existing timeouts to prevent race conditions
    if (timeoutRef.current) clearTimeout(timeoutRef.current);

    if (openTimeoutRef.current) clearTimeout(openTimeoutRef.current);

    if (open) return;

    // Set a timeout to open the popover
    openTimeoutRef.current = setTimeout(() => {
      buttonRef.current?.click();
    }, 150); // Small delay before opening
  };

  const onMouseLeave = (close: () => void) => {
    if (openTimeoutRef.current) clearTimeout(openTimeoutRef.current);

    // set a timeout to close the popover
    timeoutRef.current = setTimeout(() => {
      close();
    }, 200); // small delay before closing allows moving mouse to the panel
  };

  const positionClasses = {
    center: 'left-1/2 -translate-x-1/2',
    start: '-left-8 sm:-left-12',
  };

  return (
    <Popover as="div" className="relative inline-block">
      {({ open, close }) => (
        <div
          onMouseEnter={() => onMouseEnter(open)}
          onMouseLeave={() => onMouseLeave(close)}
        >
          <PopoverButton ref={buttonRef} className="flex focus:outline-none">
            {children ? (
              children
            ) : (
              <InformationCircleIcon className="h-5 w-5" />
            )}
          </PopoverButton>

          {open && (
            <PopoverPanel
              static
              className={`absolute z-10 px-4 mt-2 sm:px-0 w-64 sm:w-64 md:w-80 lg:w-96 max-w-[calc(100vw-7.5rem)] ${positionClasses[align]}`}
            >
              <div className="rounded-lg shadow-lg ring-1 ring-black ring-opacity-5">
                <div className="bg-gray-800 text-white text-sm p-3 rounded-lg break-words">
                  {content}
                </div>
              </div>
            </PopoverPanel>
          )}
        </div>
      )}
    </Popover>
  );
};

export default Tooltip;
