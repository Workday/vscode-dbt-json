import { makeClassName } from '@web';

export type MessageProps = {
  children: React.ReactNode;
  variant?: 'info' | 'error' | 'success';
  className?: string;
};

export function Message({
  children,
  variant = 'info',
  className,
}: MessageProps) {
  const variantClasses = {
    info: 'bg-message-info border-message-info text-message-info-contrast',
    error: 'bg-message-error border-message-error text-message-error-contrast',
    success:
      'bg-message-success border-message-success text-message-success-contrast',
  };

  return (
    <div
      className={makeClassName(
        'border p-4 rounded-lg',
        variantClasses[variant],
        className,
      )}
    >
      {children}
    </div>
  );
}
