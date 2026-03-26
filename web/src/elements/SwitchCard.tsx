import { Switch } from '@web/elements/Switch';

interface SwitchCardProps<T = string> {
  checked: boolean;
  configKey: T;
  label: string;
  tooltipText: string;
  onChange: (key: T, value: boolean) => void;
  position?: 'left' | 'right';
}

export function SwitchCard<T = string>({
  checked,
  configKey,
  label,
  tooltipText,
  onChange,
  position = 'left',
}: SwitchCardProps<T>) {
  const handleChange = (
    checked: boolean | React.ChangeEvent<HTMLInputElement>,
  ) => {
    const value =
      typeof checked === 'boolean' ? checked : checked.target.checked;
    onChange(configKey, value);
  };

  return (
    <div className="p-6 border-2 border-neutral rounded-lg bg-background">
      <Switch
        checked={checked}
        onChange={handleChange}
        label={label}
        tooltipText={tooltipText}
        position={position}
      />
    </div>
  );
}
