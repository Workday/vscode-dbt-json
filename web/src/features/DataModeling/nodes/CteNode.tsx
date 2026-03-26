import {
  ChevronDownIcon,
  ChevronUpIcon,
  PlusIcon,
  TableCellsIcon,
  TrashIcon,
} from '@heroicons/react/24/outline';
import { Button } from '@web/elements';
import { type CteState, useModelStore } from '@web/stores/useModelStore';
import { Handle, type NodeProps, Position } from '@xyflow/react';
import React, { useCallback, useState } from 'react';

/**
 * ReactFlow node for managing CTE definitions within the data modeling canvas.
 * Renders a collapsible list of CTEs with controls for CRUD and reordering.
 */
export const CteNode: React.FC<NodeProps> = ({ data: _data }) => {
  const { ctes, addCte, updateCte, removeCte, moveCte } = useModelStore(
    (state) => ({
      ctes: state.ctes,
      addCte: state.addCte,
      updateCte: state.updateCte,
      removeCte: state.removeCte,
      moveCte: state.moveCte,
    }),
  );

  const [expandedIndex, setExpandedIndex] = useState<number | null>(null);
  const [newCteName, setNewCteName] = useState('');

  const handleAddCte = useCallback(() => {
    const name = newCteName.trim() || `cte_${(ctes?.length || 0) + 1}`;
    addCte({
      name,
      from: { model: '' },
      select: [],
    });
    setNewCteName('');
    setExpandedIndex(ctes?.length || 0);
  }, [addCte, ctes, newCteName]);

  const handleRemoveCte = useCallback(
    (index: number) => {
      removeCte(index);
      if (expandedIndex === index) setExpandedIndex(null);
    },
    [removeCte, expandedIndex],
  );

  const handleUpdateCteName = useCallback(
    (index: number, name: string) => {
      if (!ctes?.[index]) return;
      updateCte(index, { ...ctes[index], name });
    },
    [ctes, updateCte],
  );

  const handleUpdateCteFrom = useCallback(
    (index: number, fromType: string, ref: string) => {
      if (!ctes?.[index]) return;
      let from: CteState['from'];
      switch (fromType) {
        case 'model':
          from = { model: ref };
          break;
        case 'cte':
          from = { cte: ref };
          break;
        default:
          from = { model: ref };
      }
      updateCte(index, { ...ctes[index], from });
    },
    [ctes, updateCte],
  );

  const cteCount = ctes?.length || 0;

  return (
    <div
      className="cte-node"
      style={{
        background: 'var(--vscode-editor-background)',
        border: '1px solid var(--vscode-panel-border)',
        borderRadius: '8px',
        padding: '12px',
        minWidth: '320px',
        maxWidth: '400px',
      }}
    >
      <Handle type="target" position={Position.Top} id="input" />

      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: '8px',
          borderBottom: '1px solid var(--vscode-panel-border)',
          paddingBottom: '8px',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <TableCellsIcon
            style={{
              width: 16,
              height: 16,
              color: 'var(--vscode-charts-purple)',
            }}
          />
          <span style={{ fontWeight: 600, fontSize: '13px' }}>
            CTEs ({cteCount})
          </span>
        </div>
        <Button
          variant="secondary"
          onClick={handleAddCte}
          label="Add"
          icon={<PlusIcon style={{ width: 14, height: 14 }} />}
        />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
        {ctes?.map((cte, index) => (
          <div
            key={index}
            style={{
              border: '1px solid var(--vscode-panel-border)',
              borderRadius: '6px',
              overflow: 'hidden',
            }}
          >
            {/* CTE Header */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: '6px 8px',
                background:
                  expandedIndex === index
                    ? 'var(--vscode-list-activeSelectionBackground)'
                    : 'transparent',
                cursor: 'pointer',
              }}
              onClick={() =>
                setExpandedIndex(expandedIndex === index ? null : index)
              }
            >
              <div
                style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
              >
                <span
                  style={{
                    fontSize: '11px',
                    color: 'var(--vscode-descriptionForeground)',
                    minWidth: '16px',
                  }}
                >
                  {index + 1}.
                </span>
                <span style={{ fontSize: '12px', fontWeight: 500 }}>
                  {cte.name || `cte_${index + 1}`}
                </span>
              </div>
              <div
                style={{ display: 'flex', alignItems: 'center', gap: '2px' }}
              >
                {index > 0 && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      moveCte(index, index - 1);
                    }}
                    style={{
                      background: 'none',
                      border: 'none',
                      cursor: 'pointer',
                      padding: '2px',
                    }}
                    title="Move up"
                  >
                    <ChevronUpIcon
                      style={{
                        width: 12,
                        height: 12,
                        color: 'var(--vscode-foreground)',
                      }}
                    />
                  </button>
                )}
                {index < cteCount - 1 && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      moveCte(index, index + 1);
                    }}
                    style={{
                      background: 'none',
                      border: 'none',
                      cursor: 'pointer',
                      padding: '2px',
                    }}
                    title="Move down"
                  >
                    <ChevronDownIcon
                      style={{
                        width: 12,
                        height: 12,
                        color: 'var(--vscode-foreground)',
                      }}
                    />
                  </button>
                )}
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    handleRemoveCte(index);
                  }}
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    padding: '2px',
                  }}
                  title="Remove CTE"
                >
                  <TrashIcon
                    style={{
                      width: 12,
                      height: 12,
                      color: 'var(--vscode-errorForeground)',
                    }}
                  />
                </button>
              </div>
            </div>

            {/* CTE Expanded Editor */}
            {expandedIndex === index && (
              <div
                style={{
                  padding: '8px',
                  borderTop: '1px solid var(--vscode-panel-border)',
                }}
              >
                {/* Name */}
                <label
                  style={{
                    fontSize: '11px',
                    color: 'var(--vscode-descriptionForeground)',
                    display: 'block',
                    marginBottom: '2px',
                  }}
                >
                  Name
                </label>
                <input
                  value={cte.name || ''}
                  onChange={(e) => handleUpdateCteName(index, e.target.value)}
                  placeholder="cte_name"
                  style={{
                    width: '100%',
                    padding: '4px 8px',
                    fontSize: '12px',
                    border: '1px solid var(--vscode-input-border)',
                    background: 'var(--vscode-input-background)',
                    color: 'var(--vscode-input-foreground)',
                    borderRadius: '4px',
                    marginBottom: '8px',
                    boxSizing: 'border-box',
                  }}
                />

                {/* From Type */}
                <label
                  style={{
                    fontSize: '11px',
                    color: 'var(--vscode-descriptionForeground)',
                    display: 'block',
                    marginBottom: '2px',
                  }}
                >
                  From
                </label>
                <div
                  style={{ display: 'flex', gap: '4px', marginBottom: '4px' }}
                >
                  <select
                    value={'cte' in cte.from ? 'cte' : 'model'}
                    onChange={(e) => {
                      const from = cte.from as Record<string, string>;
                      const ref = from.model ?? from.cte ?? '';
                      handleUpdateCteFrom(index, e.target.value, ref);
                    }}
                    style={{
                      padding: '4px',
                      fontSize: '11px',
                      border: '1px solid var(--vscode-input-border)',
                      background: 'var(--vscode-input-background)',
                      color: 'var(--vscode-input-foreground)',
                      borderRadius: '4px',
                      minWidth: '70px',
                    }}
                  >
                    <option value="model">Model</option>
                    <option value="cte">CTE</option>
                  </select>
                  <input
                    value={
                      ('model' in cte.from
                        ? typeof (cte.from as Record<string, unknown>).model ===
                          'string'
                          ? (cte.from as Record<string, unknown>).model
                          : ''
                        : 'cte' in cte.from
                          ? typeof (cte.from as Record<string, unknown>).cte ===
                            'string'
                            ? (cte.from as Record<string, unknown>).cte
                            : ''
                          : '') as string
                    }
                    onChange={(e) => {
                      const fromType = 'cte' in cte.from ? 'cte' : 'model';
                      handleUpdateCteFrom(index, fromType, e.target.value);
                    }}
                    placeholder="reference name"
                    style={{
                      flex: 1,
                      padding: '4px 8px',
                      fontSize: '12px',
                      border: '1px solid var(--vscode-input-border)',
                      background: 'var(--vscode-input-background)',
                      color: 'var(--vscode-input-foreground)',
                      borderRadius: '4px',
                    }}
                  />
                </div>

                {/* From summary */}
                <div
                  style={{
                    fontSize: '11px',
                    color: 'var(--vscode-descriptionForeground)',
                    marginTop: '4px',
                  }}
                >
                  {'cte' in cte.from
                    ? `→ CTE: ${(cte.from as { cte: string }).cte}`
                    : `→ Model: ${(cte.from as { model: string }).model}`}
                </div>
              </div>
            )}
          </div>
        ))}

        {cteCount === 0 && (
          <div
            style={{
              textAlign: 'center',
              padding: '16px',
              fontSize: '12px',
              color: 'var(--vscode-descriptionForeground)',
            }}
          >
            No CTEs defined. Click &quot;Add&quot; to create one.
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Bottom} id="output" />
    </div>
  );
};
