import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react';
import { useEffect, useState } from 'react';
import { Button } from './Button';

export type DialogBoxProps = {
  open: boolean;
  description?: string;
  title?: string;
  caption?: string;
  confirmCTALabel?: string;
  discardCTALabel?: string;
  onConfirm?: () => void;
  onDiscard?: () => void;
};

export function DialogBox({
  description,
  title,
  caption,
  confirmCTALabel,
  discardCTALabel,
  onConfirm,
  onDiscard,
  open = false,
}: DialogBoxProps) {
  const [isOpen, setIsOpen] = useState(open);

  useEffect(() => {
    setIsOpen(open);
  }, [open]);

  return (
    <>
      <Dialog
        open={isOpen}
        onClose={() => {
          if (onDiscard) onDiscard();
          else setIsOpen(false);
        }}
        className="relative z-50"
      >
        <div className="fixed inset-0 flex w-screen items-center justify-center p-4">
          <DialogPanel className="max-w-lg space-y-6 border border-[#202020] bg-background white p-6 rounded ring-1 ring-white/10">
            {title && (
              <DialogTitle className="font-bold text-background-contrast">
                {title}
              </DialogTitle>
            )}
            {caption && (
              <div className="text-sm text-background-contrast">{caption}</div>
            )}
            {description && (
              <p className="text-background-contrast">{description}</p>
            )}
            <div className="flex gap-4">
              <Button
                label={discardCTALabel || 'Cancel'}
                variant="neutral"
                onClick={() => {
                  if (onDiscard) onDiscard();
                  else setIsOpen(false);
                }}
              />

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
