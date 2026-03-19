import { PlusIcon, TrashIcon } from '@heroicons/react/24/outline';
import { useState } from 'react';

import { Button } from './Button';
import { InputText } from './InputText';

export interface EditableListProps {
  /** Array of items to display */
  items: string[];
  /** Callback when items change */
  onChange: (items: string[]) => void;
  /** Placeholder text for input fields */
  placeholder?: string;
  /** Text to show when list is empty */
  emptyText?: string;
  /** Label for the add button */
  addButtonLabel?: string;
  /** Icon size for remove button (default: 'w-5 h-5') */
  iconSize?: string;
  /** Maximum height for scrollable list (default: 'max-h-[200px]') */
  maxHeight?: string;
  /** Additional class for the container */
  className?: string;
}

export function EditableList({
  items,
  onChange,
  placeholder = 'Enter value',
  emptyText = 'No items added',
  addButtonLabel = 'Add',
  iconSize = 'w-5 h-5',
  maxHeight = 'max-h-[200px]',
  className = '',
}: EditableListProps) {
  const [inputValue, setInputValue] = useState('');

  const handleAdd = () => {
    if (inputValue.trim() === '') return;
    onChange([...items, inputValue.trim()]);
    setInputValue('');
  };

  const handleRemove = (index: number) => {
    onChange(items.filter((_, i) => i !== index));
  };

  const handleUpdate = (index: number, value: string) => {
    const newItems = items.map((item, i) => (i === index ? value : item));
    onChange(newItems);
  };

  return (
    <div className={`flex flex-col gap-2 ${className}`}>
      {/* Existing items */}
      <div className={`flex flex-col gap-2 ${maxHeight} overflow-y-auto p-1`}>
        {items.length === 0 && (
          <p className="text-sm text-muted-foreground text-center py-2">
            {emptyText}
          </p>
        )}
        {items.map((item, index) => (
          <div key={index} className="flex items-start gap-2">
            <InputText
              placeholder={placeholder}
              value={item}
              onChange={(e) => handleUpdate(index, e.target.value)}
              onBlur={(e) => {
                if (e.target.value.trim() === '') {
                  handleRemove(index);
                }
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                }
              }}
              className="flex-1"
            />
            <Button
              onClick={(e) => {
                e.stopPropagation();
                handleRemove(index);
              }}
              variant="iconButton"
              title="Remove item"
              label=""
              icon={<TrashIcon className={`${iconSize} text-error`} />}
            />
          </div>
        ))}
      </div>

      {/* Add new item */}
      <div className="flex items-center gap-2">
        <InputText
          placeholder={placeholder}
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault();
              handleAdd();
            }
          }}
          className="flex-1"
        />
        <Button
          onClick={handleAdd}
          label={addButtonLabel}
          title={`Add item`}
          variant="outlineIconButton"
          icon={<PlusIcon className={`${iconSize}`} />}
        />
      </div>
    </div>
  );
}
