export type ProgressProps = {
  caption?: string;
  label?: string;
  percent: number;
};

export function Progress({ caption, label, percent }: ProgressProps) {
  return (
    <div className="p-2">
      {label && <p className="label font-medium">{label}</p>}
      <div aria-hidden="true" className="mt-6">
        <div className="overflow-hidden rounded-full">
          <div
            style={{ width: `${percent}%` }}
            className="bg-primary-contrast h-2 rounded-full"
          />
        </div>
      </div>
      {caption && <p className="font-small">{caption}</p>}
    </div>
  );
}
