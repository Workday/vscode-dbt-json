import { useError } from '@web';
import { Alert, Button } from '@web/elements';
import { useCallback, useState } from 'react';
import type { FieldValues, UseFormHandleSubmit } from 'react-hook-form';

export type FormProps<T extends FieldValues> = Omit<
  React.FormHTMLAttributes<HTMLFormElement>,
  'handleSubmit' | 'onSubmit'
> & {
  disableSubmit?: boolean;
  handleSubmit: UseFormHandleSubmit<T>;
  hideSubmit?: boolean;
  labelSubmit?: string;
  onSubmit: (values: T) => Promise<void>;
  additionalButtons?: React.ReactNode;
};

export function Form<T extends FieldValues>({
  additionalButtons,
  children,
  disableSubmit,
  handleSubmit,
  hideSubmit,
  labelSubmit,
  onSubmit,
  ...props
}: FormProps<T>) {
  const { error, handleError } = useError();
  const [loading, setLoading] = useState(false);
  const handleOnSubmit: React.FormEventHandler<HTMLFormElement> = useCallback(
    (e) => {
      e.preventDefault();
      void handleSubmit(async (values: T) => {
        try {
          setLoading(true);
          await onSubmit(values);
        } catch (err) {
          handleError(err);
          setLoading(false);
        }
      })();
    },
    [handleError, handleSubmit, onSubmit],
  );

  return (
    <form className="p-3" {...props} onSubmit={handleOnSubmit}>
      {children}
      <div className="flex gap-2 mt-4">
        {additionalButtons}
        {!hideSubmit && (
          <Button
            disabled={disableSubmit}
            label={labelSubmit || 'Submit'}
            loading={loading}
            type="submit"
            variant="primary"
          />
        )}
      </div>
      {error && <Alert description={error.message} variant="error" />}
    </form>
  );
}
