import { PlusIcon, TrashIcon, XMarkIcon } from '@heroicons/react/24/outline';
import type {
  SchemaLightdashRequiredAttributes,
  SchemaLightdashRequiredFilters,
} from '@shared/schema/types/lightdash.table.schema';
import type { SchemaModelLightdash } from '@shared/schema/types/model.schema';
import { Button, Checkbox, InputText, Popover, Tooltip } from '@web/elements';
import React, { useCallback, useEffect, useMemo, useState } from 'react';

interface LightdashTablePropertiesProps {
  lightdashConfig: SchemaModelLightdash;
  updateLightdashState: (state: SchemaModelLightdash) => void;
}

export const LightdashTableProperties: React.FC<
  LightdashTablePropertiesProps
> = ({ lightdashConfig, updateLightdashState }) => {
  // Local state for simple table properties to prevent focus loss
  const [tableValues, setTableValues] = useState<Record<string, string>>({});

  // UI state for Required Attributes
  const [currentAttrKey, setCurrentAttrKey] = useState<string>('');
  const [currentAttrValue, setCurrentAttrValue] = useState<string>('');

  // UI state for Required Filters (per filter set)
  const [filterInputs, setFilterInputs] = useState<
    Record<number, { key: string; value: string }>
  >({});

  // Initialize local state from lightdashConfig
  useEffect(() => {
    const initialValues: Record<string, string> = {};
    if (lightdashConfig.table) {
      // Only initialize for simple string properties
      const groupLabel = lightdashConfig.table.group_label;
      const label = lightdashConfig.table.label;

      if (groupLabel !== undefined) {
        initialValues.group_label =
          typeof groupLabel === 'string' ? groupLabel : '';
      }
      if (label !== undefined) {
        initialValues.label = typeof label === 'string' ? label : '';
      }

      // Handle other properties if they exist
      Object.entries(lightdashConfig.table).forEach(([key, value]) => {
        if (
          key !== 'group_label' &&
          key !== 'label' &&
          key !== 'required_attributes' &&
          key !== 'required_filters'
        ) {
          if (typeof value === 'string') {
            initialValues[key] = value;
          } else if (Array.isArray(value)) {
            initialValues[key] = value.join(', ');
          }
        }
      });
    }
    setTableValues(initialValues);
  }, [
    lightdashConfig.table?.group_label,
    lightdashConfig.table?.label,
    lightdashConfig.table,
  ]);

  // All table properties with their display names and default visibility
  const tableProperties = useMemo(
    () => [
      {
        key: 'group_label',
        label: 'Group Label',
        defaultVisible: true,
        type: 'string',
      },
      { key: 'label', label: 'Label', defaultVisible: true, type: 'string' },
      {
        key: 'ai_hint',
        label: 'AI Hint',
        defaultVisible: false,
        type: 'string_array',
      },
      {
        key: 'group_details',
        label: 'Group Details',
        defaultVisible: false,
        type: 'any',
      },
      {
        key: 'required_attributes',
        label: 'Required Attributes',
        defaultVisible: false,
        type: 'object_keyvalue',
      },
      {
        key: 'required_filters',
        label: 'Required Filters',
        defaultVisible: false,
        type: 'array_of_objects',
      },
      {
        key: 'sql_filter',
        label: 'SQL Filter',
        defaultVisible: false,
        type: 'string',
      },
      {
        key: 'sql_where',
        label: 'SQL Where',
        defaultVisible: false,
        type: 'string',
      },
    ],
    [],
  );

  // State for managing visible table properties - initialize with default visibility OR if values exist
  const [visibleProperties, setVisibleProperties] = useState<
    Record<string, boolean>
  >(() => {
    return tableProperties.reduce(
      (acc, prop) => {
        // Show property if it has a default visibility OR if it has a value in the config
        const hasValue =
          lightdashConfig.table &&
          lightdashConfig.table[
            prop.key as keyof typeof lightdashConfig.table
          ] !== undefined &&
          lightdashConfig.table[
            prop.key as keyof typeof lightdashConfig.table
          ] !== null &&
          lightdashConfig.table[
            prop.key as keyof typeof lightdashConfig.table
          ] !== '';
        acc[prop.key] = !!(prop.defaultVisible || hasValue);
        return acc;
      },
      {} as Record<string, boolean>,
    );
  });

  // Handler for local input changes (doesn't trigger parent update)
  const handleInputChange = useCallback((property: string, value: string) => {
    setTableValues((prev) => ({
      ...prev,
      [property]: value,
    }));
  }, []);

  const handleTablePropertyChange = useCallback(
    (property: string, value: string | string[]) => {
      updateLightdashState({
        ...lightdashConfig,
        table: {
          ...lightdashConfig.table,
          [property]: value,
        },
      });
    },
    [lightdashConfig, updateLightdashState],
  );

  // Handler for toggling property visibility
  const handlePropertyVisibilityChange = useCallback(
    (property: string, isVisible: boolean) => {
      setVisibleProperties((prev) => ({
        ...prev,
        [property]: isVisible,
      }));
    },
    [],
  );

  // Required Attributes handlers
  const handleAddAttribute = useCallback(() => {
    if (!currentAttrKey.trim()) return;

    // Auto-detect array: if value contains comma, split into array
    const processedValue = currentAttrValue.includes(',')
      ? currentAttrValue
          .split(',')
          .map((v) => v.trim())
          .filter((v) => v)
      : currentAttrValue;

    const newAttributes: SchemaLightdashRequiredAttributes = {
      ...(lightdashConfig.table?.required_attributes || {}),
      [currentAttrKey.trim()]: processedValue,
    };

    updateLightdashState({
      ...lightdashConfig,
      table: {
        ...lightdashConfig.table,
        required_attributes: newAttributes,
      },
    });

    // Clear inputs
    setCurrentAttrKey('');
    setCurrentAttrValue('');
  }, [currentAttrKey, currentAttrValue, lightdashConfig, updateLightdashState]);

  const handleRemoveAttribute = useCallback(
    (key: string) => {
      const currentAttrs = lightdashConfig.table?.required_attributes || {};

      const { [key]: _removed, ...rest } = currentAttrs;

      updateLightdashState({
        ...lightdashConfig,
        table: {
          ...lightdashConfig.table,
          required_attributes: rest,
        },
      });
    },
    [lightdashConfig, updateLightdashState],
  );

  // Required Filters handlers
  const handleAddFilterSet = useCallback(() => {
    const newFilterSets: SchemaLightdashRequiredFilters = [
      ...(lightdashConfig.table?.required_filters || []),
      {}, // Empty filter set
    ];

    updateLightdashState({
      ...lightdashConfig,
      table: {
        ...lightdashConfig.table,
        required_filters: newFilterSets,
      },
    });
  }, [lightdashConfig, updateLightdashState]);

  const handleAddFilterKeyValue = useCallback(
    (filterIndex: number) => {
      const input = filterInputs[filterIndex];
      if (!input || !input.key.trim() || !input.value.trim()) return;

      const updatedFilters: SchemaLightdashRequiredFilters = [
        ...(lightdashConfig.table?.required_filters || []),
      ];
      updatedFilters[filterIndex] = {
        ...updatedFilters[filterIndex],
        [input.key.trim()]: input.value.trim(),
      };

      updateLightdashState({
        ...lightdashConfig,
        table: {
          ...lightdashConfig.table,
          required_filters: updatedFilters,
        },
      });

      // Clear input for this filter set
      setFilterInputs((prev) => {
        const newInputs = { ...prev };
        delete newInputs[filterIndex];
        return newInputs;
      });
    },
    [filterInputs, lightdashConfig, updateLightdashState],
  );

  const handleRemoveFilterKeyValue = useCallback(
    (filterIndex: number, key: string) => {
      const updatedFilters: SchemaLightdashRequiredFilters = [
        ...(lightdashConfig.table?.required_filters || []),
      ];

      const { [key]: _removed, ...rest } = updatedFilters[filterIndex];
      updatedFilters[filterIndex] = rest;

      updateLightdashState({
        ...lightdashConfig,
        table: {
          ...lightdashConfig.table,
          required_filters: updatedFilters,
        },
      });
    },
    [lightdashConfig, updateLightdashState],
  );

  const handleRemoveFilterSet = useCallback(
    (filterIndex: number) => {
      const updatedFilters: SchemaLightdashRequiredFilters = (
        lightdashConfig.table?.required_filters || []
      ).filter((_, idx) => idx !== filterIndex);

      updateLightdashState({
        ...lightdashConfig,
        table: {
          ...lightdashConfig.table,
          required_filters: updatedFilters,
        },
      });

      // Clear input for this filter set
      setFilterInputs((prev) => {
        const newInputs = { ...prev };
        delete newInputs[filterIndex];
        return newInputs;
      });
    },
    [lightdashConfig, updateLightdashState],
  );

  const handleFilterInputChange = useCallback(
    (filterIndex: number, field: 'key' | 'value', value: string) => {
      setFilterInputs((prev) => ({
        ...prev,
        [filterIndex]: {
          key: field === 'key' ? value : prev[filterIndex]?.key || '',
          value: field === 'value' ? value : prev[filterIndex]?.value || '',
        },
      }));
    },
    [],
  );

  return (
    <div className="mb-4" data-tutorial-id="lightdash-table-properties">
      <div className="flex items-center justify-between mb-3">
        <div className="text-sm font-medium text-foreground flex items-center gap-2">
          Table
          <Tooltip
            content="Configure table-level properties for your Lightdash model, such as labels, filters, and AI hints"
            variant="outline"
          />
        </div>
        <Popover
          trigger={
            <span className="text-sm text-primary hover:text-primary/80 transition-colors cursor-pointer">
              + Choose properties
            </span>
          }
          showChevron
          panelClassName="w-72 p-4"
          placement="right"
        >
          <div className="text-sm font-medium text-foreground mb-3">
            Table Properties
          </div>
          <div className="space-y-3">
            {tableProperties.map((property) => (
              <div key={property.key} className="flex items-center space-x-2">
                <Checkbox
                  checked={visibleProperties[property.key] || false}
                  onChange={(checked) => {
                    const isChecked =
                      typeof checked === 'boolean'
                        ? checked
                        : checked.target.checked;
                    handlePropertyVisibilityChange(property.key, isChecked);
                  }}
                  className="size-4"
                />
                <span className="text-sm text-foreground">
                  {property.label}
                </span>
              </div>
            ))}
          </div>
        </Popover>
      </div>

      <div className="space-y-3">
        {tableProperties
          .filter((property) => visibleProperties[property.key])
          .map((property) => {
            // Skip special types - they're rendered separately below
            if (
              property.type === 'object_keyvalue' ||
              property.type === 'array_of_objects'
            ) {
              return null;
            }

            return (
              <div key={property.key}>
                <label className="block text-sm text-muted-foreground mb-1">
                  {property.label}
                </label>
                <InputText
                  value={tableValues[property.key] || ''}
                  onChange={(e) => {
                    handleInputChange(property.key, e.target.value);
                  }}
                  onBlur={(e) => {
                    let processedValue: string | string[] = e.target.value;

                    // ai_hint which can be string or string[]
                    if (
                      property.type === 'string_array' &&
                      e.target.value.includes(',')
                    ) {
                      processedValue = e.target.value
                        .split(',')
                        .map((v) => v.trim())
                        .filter((v) => v);
                    }

                    handleTablePropertyChange(property.key, processedValue);
                  }}
                  placeholder={`Enter ${property.label.toLowerCase()}${
                    property.type === 'string_array'
                      ? ' (separate multiple with commas)'
                      : ''
                  }`}
                  className="w-full"
                />
              </div>
            );
          })}

        {/* Required Attributes Section (conditional) */}
        {visibleProperties['required_attributes'] && (
          <div className="p-4 border border-border rounded-lg bg-muted/10">
            <div className="text-sm font-medium text-foreground mb-3">
              Required Attributes
            </div>

            {/* Display existing attributes */}
            {Object.keys(lightdashConfig.table?.required_attributes || {})
              .length > 0 && (
              <div className="space-y-2 mb-3">
                {Object.entries(
                  lightdashConfig.table?.required_attributes || {},
                ).map(([key, value]) => (
                  <div
                    key={key}
                    className="grid grid-cols-12 gap-2 p-2 border border-border rounded-md bg-background"
                  >
                    <div className="col-span-5">
                      <span className="text-sm font-medium text-foreground">
                        {key}
                      </span>
                    </div>
                    <div className="col-span-6">
                      <span className="text-sm text-muted-foreground">
                        {Array.isArray(value) ? value.join(', ') : value}
                      </span>
                      {Array.isArray(value) && (
                        <span className="ml-2 text-xs text-primary">
                          (array)
                        </span>
                      )}
                    </div>
                    <div className="col-span-1 flex justify-end">
                      <Button
                        onClick={(e) => {
                          e.stopPropagation();
                          handleRemoveAttribute(key);
                        }}
                        onMouseDown={(e) => e.stopPropagation()}
                        className="p-1 hover:bg-muted rounded transition-colors"
                        title="Remove attribute"
                        variant="iconButton"
                        label=""
                        icon={
                          <XMarkIcon className="w-4 h-4 text-muted-foreground hover:text-red-500" />
                        }
                      />
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* Add new attribute form */}
            <div className="grid grid-cols-12 gap-2">
              <div className="col-span-5">
                <InputText
                  value={currentAttrKey}
                  onChange={(e) => setCurrentAttrKey(e.target.value)}
                  placeholder="Key"
                  className="w-full"
                  onMouseDown={(e) => e.stopPropagation()}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                      handleAddAttribute();
                    }
                  }}
                />
              </div>
              <div className="col-span-5">
                <InputText
                  value={currentAttrValue}
                  onChange={(e) => setCurrentAttrValue(e.target.value)}
                  placeholder="Value (comma-separated for array)"
                  className="w-full"
                  onMouseDown={(e) => e.stopPropagation()}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                      handleAddAttribute();
                    }
                  }}
                />
              </div>
              <div className="col-span-2">
                <Button
                  label="+ Add"
                  className="w-full py-1 border border-primary rounded-md text-primary"
                  variant="link"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleAddAttribute();
                  }}
                  onMouseDown={(e) => e.stopPropagation()}
                  disabled={!currentAttrKey.trim()}
                />
              </div>
            </div>
          </div>
        )}

        {/* Required Filters Section (conditional) */}
        {visibleProperties['required_filters'] && (
          <div className="p-4 border border-border rounded-lg bg-muted/10">
            <div className="text-sm font-medium text-foreground mb-3">
              Required Filters
            </div>

            {/* Display existing filter sets */}
            {(lightdashConfig.table?.required_filters || []).length > 0 && (
              <div className="space-y-4 mb-3">
                {(lightdashConfig.table?.required_filters || []).map(
                  (filterSet, filterIndex) => (
                    <div
                      key={filterIndex}
                      className="border border-border rounded-md p-3 bg-background"
                    >
                      <div className="flex justify-between items-center mb-2">
                        <span className="text-sm font-medium text-foreground">
                          Filter Set {filterIndex + 1}
                        </span>
                        <Button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleRemoveFilterSet(filterIndex);
                          }}
                          onMouseDown={(e) => e.stopPropagation()}
                          className="p-1 hover:bg-muted rounded transition-colors"
                          title="Remove filter set"
                          variant="iconButton"
                          label=""
                          icon={
                            <TrashIcon className="w-4 h-4 text-muted-foreground hover:text-red-500" />
                          }
                        />
                      </div>

                      {/* Display key-value pairs in this filter set */}
                      {Object.keys(filterSet).length > 0 && (
                        <div className="space-y-2 mb-2">
                          {Object.entries(filterSet).map(([key, value]) => (
                            <div
                              key={key}
                              className="grid grid-cols-12 gap-2 p-2 border border-border rounded bg-muted/10"
                            >
                              <div className="col-span-5">
                                <span className="text-sm font-medium text-foreground">
                                  {key}
                                </span>
                              </div>
                              <div className="col-span-6">
                                <span className="text-sm text-muted-foreground">
                                  {value}
                                </span>
                              </div>
                              <div className="col-span-1 flex justify-end">
                                <Button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    handleRemoveFilterKeyValue(
                                      filterIndex,
                                      key,
                                    );
                                  }}
                                  onMouseDown={(e) => e.stopPropagation()}
                                  className="p-1 hover:bg-muted rounded transition-colors"
                                  title="Remove key-value pair"
                                  variant="iconButton"
                                  label=""
                                  icon={
                                    <XMarkIcon className="w-4 h-4 text-muted-foreground hover:text-red-500" />
                                  }
                                />
                              </div>
                            </div>
                          ))}
                        </div>
                      )}

                      {/* Add key-value to this filter set */}
                      <div className="grid grid-cols-12 gap-2">
                        <div className="col-span-5">
                          <InputText
                            value={filterInputs[filterIndex]?.key || ''}
                            onChange={(e) =>
                              handleFilterInputChange(
                                filterIndex,
                                'key',
                                e.target.value,
                              )
                            }
                            placeholder="Key"
                            className="w-full text-sm"
                            onMouseDown={(e) => e.stopPropagation()}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') {
                                e.preventDefault();
                                handleAddFilterKeyValue(filterIndex);
                              }
                            }}
                          />
                        </div>
                        <div className="col-span-5">
                          <InputText
                            value={filterInputs[filterIndex]?.value || ''}
                            onChange={(e) =>
                              handleFilterInputChange(
                                filterIndex,
                                'value',
                                e.target.value,
                              )
                            }
                            placeholder="Value"
                            className="w-full text-sm"
                            onMouseDown={(e) => e.stopPropagation()}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') {
                                e.preventDefault();
                                handleAddFilterKeyValue(filterIndex);
                              }
                            }}
                          />
                        </div>
                        <div className="col-span-2">
                          <Button
                            label="+ Add"
                            className="h-full w-full py-1 text-xs border border-primary rounded-md text-primary"
                            variant="link"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleAddFilterKeyValue(filterIndex);
                            }}
                            onMouseDown={(e) => e.stopPropagation()}
                            disabled={
                              !filterInputs[filterIndex]?.key?.trim() ||
                              !filterInputs[filterIndex]?.value?.trim()
                            }
                          />
                        </div>
                      </div>
                    </div>
                  ),
                )}
              </div>
            )}

            {/* Add new filter set button */}
            <Button
              label="+ Add Filter Set"
              className="w-full py-1 px-4 border border-primary font-bold text-primary text-sm rounded-md bg-transparent hover:bg-primary/5 transition-colors flex items-center justify-center gap-2"
              variant="link"
              onClick={(e) => {
                e.stopPropagation();
                handleAddFilterSet();
              }}
              onMouseDown={(e) => e.stopPropagation()}
              icon={<PlusIcon className="w-4 h-4" />}
            />
          </div>
        )}
      </div>
    </div>
  );
};
