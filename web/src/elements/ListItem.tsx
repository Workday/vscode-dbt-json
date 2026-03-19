import { XMarkIcon } from '@heroicons/react/20/solid';

export const ListItem = ({
  children,
  className,
  showRemoveIcon = false,
  onRemove,
}: {
  children: React.ReactNode;
  className?: string;
  showRemoveIcon?: boolean;
  onRemove?: () => void;
}) => {
  return (
    <div
      className={`p-4 bg-surface text-background-contrast rounded-md flex justify-between ${className}`}
    >
      {children}
      {showRemoveIcon && (
        <XMarkIcon className="w-4 h-4 cursor-pointer" onClick={onRemove} />
      )}
    </div>
  );
};
