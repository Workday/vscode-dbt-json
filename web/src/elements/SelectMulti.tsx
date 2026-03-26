import { ChevronDownIcon, XMarkIcon } from '@heroicons/react/24/outline';
import React, { useEffect, useRef, useState } from 'react';

import { InputText } from './InputText';

export interface SelectMultiOption {
  value: string;
  label: string;
}

export interface SelectMultiProps {
  options: SelectMultiOption[];
  value: string[];
  onChange: (selectedValues: string[]) => void;
  placeholder?: string;
  className?: string;
  searchable?: boolean;
  showSelectedTags?: boolean;
}

export const SelectMulti: React.FC<SelectMultiProps> = ({
  options,
  value,
  onChange,
  placeholder = 'Select options',
  className = '',
  searchable = false,
  showSelectedTags = true,
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isOpen]);

  const toggleOption = (optionValue: string) => {
    if (value.includes(optionValue)) {
      onChange(value.filter((v) => v !== optionValue));
    } else {
      onChange([...value, optionValue]);
    }
  };

  const removeOption = (optionValue: string, event: React.MouseEvent) => {
    event.stopPropagation();
    onChange(value.filter((v) => v !== optionValue));
  };

  const getSelectedLabels = () => {
    return value
      .map((v) => options.find((opt) => opt.value === v)?.label)
      .filter(Boolean);
  };

  // Filter options based on search query
  const filteredOptions = searchable
    ? options.filter((option) =>
        option.label.toLowerCase().includes(searchQuery.toLowerCase()),
      )
    : options;

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <div
        className="w-full min-h-[40px] px-3 py-2 border border-input rounded-md bg-background text-foreground cursor-pointer flex items-center gap-2 flex-wrap"
        onClick={() => setIsOpen(!isOpen)}
      >
        {value.length === 0 ? (
          <span className="text-muted-foreground">{placeholder}</span>
        ) : showSelectedTags ? (
          <div className="flex flex-wrap gap-1">
            {getSelectedLabels().map((label, index) => (
              <span
                key={value[index]}
                className="inline-flex items-center gap-1 px-2 py-1 bg-primary/10 text-primary rounded text-sm"
              >
                {label}
                <button
                  onClick={(e) => removeOption(value[index], e)}
                  className="hover:bg-primary/20 rounded-full p-0.5"
                >
                  <XMarkIcon className="w-3 h-3" />
                </button>
              </span>
            ))}
          </div>
        ) : (
          <span className="text-foreground">{value.length} selected</span>
        )}
        <ChevronDownIcon
          className={`w-4 h-4 ml-auto transition-transform ${
            isOpen ? 'transform rotate-180' : ''
          }`}
        />
      </div>

      {isOpen && (
        <div className="absolute z-50 w-full mt-1 bg-background border border-input rounded-md shadow-lg max-h-60 overflow-y-auto react-flow__node-scrollable">
          {searchable && (
            <div className="p-2 border-b border-input">
              <InputText
                placeholder="Search..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onClick={(e) => e.stopPropagation()}
                inputClassName="!mt-0"
              />
            </div>
          )}
          <div
            className={
              searchable
                ? 'max-h-48 overflow-y-auto react-flow__node-scrollable'
                : ''
            }
          >
            {filteredOptions.length === 0 ? (
              <div className="px-3 py-2 text-muted-foreground text-center">
                {searchQuery ? 'No options found' : 'No options available'}
              </div>
            ) : (
              filteredOptions.map((option) => (
                <div
                  key={option.value}
                  className={`px-3 py-2 cursor-pointer hover:bg-muted flex items-center gap-2 ${
                    value.includes(option.value) ? 'bg-muted' : ''
                  }`}
                  onClick={() => toggleOption(option.value)}
                >
                  <input
                    type="checkbox"
                    checked={value.includes(option.value)}
                    onChange={() => {}}
                    className="w-4 h-4 rounded border-gray-300"
                  />
                  <span className="text-foreground">{option.label}</span>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
};
