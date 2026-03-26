import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react';
import {
  ChevronDownIcon,
  ChevronUpIcon,
  InformationCircleIcon,
} from '@heroicons/react/20/solid';
import { useEffect, useState } from 'react';

import { Button } from './Button';

export type DialogBoxProps = {
  open: boolean;
  description?: string;
  list?: string[];
  title?: string;
  caption?: string;
  confirmCTALabel?: string;
  discardCTALabel?: string;
  onConfirm?: () => void;
  onDiscard?: () => void;
  variant?: 'error' | 'warning' | 'info';
  showDetails?: boolean;
  detailsLabel?: string;
};

export function DialogBox({
  description,
  list,
  title,
  caption,
  confirmCTALabel,
  discardCTALabel,
  onConfirm,
  onDiscard,
  open = false,
  variant = 'error',
  showDetails = false,
  detailsLabel = 'View technical details',
}: DialogBoxProps) {
  const [isOpen, setIsOpen] = useState(open);
  const [isDetailsExpanded, setIsDetailsExpanded] = useState(false);

  useEffect(() => {
    setIsOpen(open);
  }, [open]);

  useEffect(() => {
    // Reset expanded state when dialog opens/closes
    if (!open) {
      setIsDetailsExpanded(false);
    }
  }, [open]);

  // Determine border color based on variant
  const variantBorderClass = {
    error: 'border-red-500/50',
    warning: 'border-yellow-500/50',
    info: 'border-blue-500/50',
  }[variant];

  return (
    <>
      <Dialog
        open={isOpen}
        onClose={() => {
          // When clicking outside, call onDiscard if available, otherwise onConfirm
          // This ensures parent state is properly reset
          if (onDiscard) {
            onDiscard();
          } else if (onConfirm) {
            onConfirm();
          } else {
            setIsOpen(false);
          }
        }}
        className="relative z-50"
      >
        {/* Backdrop with overlay */}
        <div className="fixed inset-0 bg-black/75" aria-hidden="true" />

        <div className="fixed inset-0 flex w-screen items-center justify-center p-4">
          <DialogPanel
            className={`max-w-lg space-y-6 border ${variantBorderClass} bg-card p-6 rounded-lg shadow-xl ring-1 ring-surface/20`}
          >
            {title && (
              <DialogTitle className="font-semibold text-background-contrast">
                {title}
              </DialogTitle>
            )}
            {caption && (
              <div className="text-sm text-background-contrast">{caption}</div>
            )}
            {description && (
              <p className="text-background-contrast">{description}</p>
            )}
            {/* Collapsible technical details section */}
            {showDetails && list && list.length > 0 && (
              <div className="space-y-2">
                <Button
                  variant="iconButton"
                  icon={
                    isDetailsExpanded ? (
                      <ChevronUpIcon className="h-4 w-4" />
                    ) : (
                      <ChevronDownIcon className="h-4 w-4" />
                    )
                  }
                  onClick={() => setIsDetailsExpanded(!isDetailsExpanded)}
                  className={`
                    flex items-center gap-2 text-sm text-blue-600 
                    pr-2 py-2 rounded-md
                    transition-all duration-200 ease-in-out
                    focus:outline-none
                    ${
                      isDetailsExpanded
                        ? 'bg-blue-50'
                        : 'bg-transparent border border-transparent hover:bg-blue-50'
                    }
                  `}
                  label={detailsLabel}
                />
                {isDetailsExpanded && (
                  <div className="text-background-contrast space-y-2 text-sm bg-gray-100 p-3 rounded border border-gray-200 animate-in fade-in slide-in-from-top-1 duration-200">
                    {list.map((item, index) => (
                      <p
                        key={index}
                        className="text-gray-700 flex items-center gap-2 text-sm"
                      >
                        <InformationCircleIcon className="w-4 h-4" />
                        {item}
                      </p>
                    ))}
                  </div>
                )}
              </div>
            )}
            {/* Legacy list display (for backward compatibility when showDetails is false) */}
            {!showDetails && list && list.length > 0 && (
              <ul className="list-disc list-inside text-background-contrast space-y-1">
                {list.map((item, index) => (
                  <li key={index}>{item}</li>
                ))}
              </ul>
            )}
            <div className="flex gap-4">
              {onDiscard && (
                <Button
                  label={discardCTALabel || 'Cancel'}
                  variant="neutral"
                  onClick={() => {
                    if (onDiscard) onDiscard();
                    else setIsOpen(false);
                  }}
                />
              )}

              <Button
                label={confirmCTALabel || 'Okay'}
                variant="error"
                onClick={() => {
                  if (onConfirm) onConfirm();
                  else setIsOpen(false);
                }}
              />
            </div>
          </DialogPanel>
        </div>
      </Dialog>
    </>
  );
}
