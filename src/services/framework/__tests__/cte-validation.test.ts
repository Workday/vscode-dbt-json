import { describe, expect, test } from '@jest/globals';
import { frameworkBuildCteColumnRegistry } from '@services/framework/utils';
import {
  validateCteColumnReferences,
  validateCteGroupBy,
  validateCtes,
} from '@services/modelValidation';

import { createTestProject } from './helpers';

const project = createTestProject();

describe('CTE Validation', () => {
  test('validateCtes catches duplicate CTE names', () => {
    const errors = validateCtes({
      ctes: [
        { name: 'cte_a', from: { model: 'm' } },
        { name: 'cte_a', from: { model: 'm' } },
      ],
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('duplicate CTE name');
  });

  test('validateCtes catches forward references', () => {
    const errors = validateCtes({
      ctes: [
        { name: 'cte_a', from: { cte: 'cte_b' } },
        { name: 'cte_b', from: { model: 'm' } },
      ],
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('not defined earlier');
  });

  test('validateCtes passes for valid CTE chain', () => {
    const errors = validateCtes({
      ctes: [
        { name: 'cte_a', from: { model: 'm' } },
        { name: 'cte_b', from: { cte: 'cte_a' } },
      ],
      from: { cte: 'cte_b' },
    });
    expect(errors.length).toBe(0);
  });

  test('validateCtes rejects where on union CTE', () => {
    const errors = validateCtes({
      ctes: [
        { name: 'a', from: { model: 'm1' }, select: ['col1'] },
        { name: 'b', from: { model: 'm2' }, select: ['col1'] },
        {
          name: 'combined',
          from: { cte: 'a', union: { type: 'all', ctes: ['b'] } },
          where: { and: [{ expr: 'col1 IS NOT NULL' }] },
        },
      ],
      from: { cte: 'combined' },
    });
    expect(errors.length).toBe(1);
    expect(errors[0]).toContain('not supported on union CTEs');
  });

  test('validateCtes catches invalid main model from.cte reference', () => {
    const errors = validateCtes({
      ctes: [{ name: 'cte_a', from: { model: 'm' } }],
      from: { cte: 'nonexistent' },
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('not defined in the ctes array');
  });

  test('validateCtes catches forward CTE reference in join', () => {
    const errors = validateCtes({
      ctes: [
        {
          name: 'cte_a',
          from: {
            model: 'm1',
            join: [{ cte: 'cte_b', on: { and: ['col'] }, type: 'left' }],
          },
        },
        { name: 'cte_b', from: { model: 'm2' } },
      ],
      from: { cte: 'cte_a' },
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('join references CTE "cte_b"');
    expect(errors[0]).toContain('not defined earlier');
  });

  test('validateCtes passes valid CTE join reference', () => {
    const errors = validateCtes({
      ctes: [
        { name: 'cte_a', from: { model: 'm1' } },
        {
          name: 'cte_b',
          from: {
            cte: 'cte_a',
            join: [{ cte: 'cte_a', on: { and: ['col'] }, type: 'left' }],
          },
        },
      ],
      from: { cte: 'cte_b' },
    });
    expect(errors.length).toBe(0);
  });

  test('validateCtes catches invalid CTE reference in main model from.join', () => {
    const errors = validateCtes({
      ctes: [{ name: 'cte_a', from: { model: 'm1' } }],
      from: {
        cte: 'cte_a',
        join: [{ cte: 'nonexistent', on: { and: ['col'] }, type: 'left' }],
      },
    });
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('from.join: references CTE "nonexistent"');
  });
});

describe('validateCteColumnReferences', () => {
  const baseCtes: any[] = [
    {
      name: 'source_cte',
      from: { model: 'model_a' },
      select: [
        { name: 'dim_a', type: 'dim' },
        { name: 'dim_b', type: 'dim' },
        { name: 'dim_c', type: 'dim' },
        { name: 'fct_x', type: 'fct' },
        { name: 'fct_y', type: 'fct' },
      ],
    },
  ];

  function buildRegistryForBaseCtes() {
    return frameworkBuildCteColumnRegistry({
      ctes: baseCtes,
      project,
    });
  }

  test('reports error for non-existent column in exclude', () => {
    const registry = buildRegistryForBaseCtes();
    const modelJson: any = {
      ctes: [
        {
          name: 'derived',
          from: { cte: 'source_cte' },
          select: [
            {
              type: 'all_from_cte',
              cte: 'source_cte',
              exclude: ['nonexistent_col'],
            },
          ],
        },
      ],
      from: { cte: 'derived' },
      select: [{ type: 'all_from_cte', cte: 'derived' }],
    };
    const errors = validateCteColumnReferences(modelJson, registry);
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('nonexistent_col');
    expect(errors[0]).toContain('does not exist');
  });

  test('reports error for non-existent column in include', () => {
    const registry = buildRegistryForBaseCtes();
    const modelJson: any = {
      ctes: [
        {
          name: 'derived',
          from: { cte: 'source_cte' },
          select: [
            {
              type: 'all_from_cte',
              cte: 'source_cte',
              include: ['ghost_col'],
            },
          ],
        },
      ],
      from: { cte: 'derived' },
      select: [{ type: 'all_from_cte', cte: 'derived' }],
    };
    const errors = validateCteColumnReferences(modelJson, registry);
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('ghost_col');
    expect(errors[0]).toContain('does not exist');
  });

  test('reports error when exclude removes all columns', () => {
    const registry = buildRegistryForBaseCtes();
    const modelJson: any = {
      ctes: [
        {
          name: 'derived',
          from: { cte: 'source_cte' },
          select: [
            {
              type: 'all_from_cte',
              cte: 'source_cte',
              exclude: ['dim_a', 'dim_b', 'dim_c', 'fct_x', 'fct_y'],
            },
          ],
        },
      ],
      from: { cte: 'derived' },
      select: [{ type: 'all_from_cte', cte: 'derived' }],
    };
    const errors = validateCteColumnReferences(modelJson, registry);
    expect(errors.some((e: string) => e.includes('zero columns'))).toBe(true);
  });

  test('validates main model select exclude/include references', () => {
    const registry = buildRegistryForBaseCtes();
    const modelJson: any = {
      ctes: baseCtes,
      from: { cte: 'source_cte' },
      select: [
        {
          type: 'all_from_cte',
          cte: 'source_cte',
          exclude: ['nonexistent'],
        },
      ],
    };
    const errors = validateCteColumnReferences(modelJson, registry);
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('nonexistent');
  });

  test('no errors for valid exclude/include', () => {
    const registry = buildRegistryForBaseCtes();
    const modelJson: any = {
      ctes: [
        {
          name: 'derived',
          from: { cte: 'source_cte' },
          select: [
            {
              type: 'all_from_cte',
              cte: 'source_cte',
              exclude: ['dim_b'],
            },
          ],
        },
      ],
      from: { cte: 'derived' },
      select: [{ type: 'all_from_cte', cte: 'derived' }],
    };
    const errors = validateCteColumnReferences(modelJson, registry);
    expect(errors).toEqual([]);
  });

  test('fcts_from_cte exclude referencing dim column reports error (type narrowing)', () => {
    const registry = buildRegistryForBaseCtes();
    const modelJson: any = {
      ctes: [
        {
          name: 'derived',
          from: { cte: 'source_cte' },
          select: [
            {
              type: 'fcts_from_cte',
              cte: 'source_cte',
              exclude: ['dim_a'],
            },
          ],
        },
      ],
      from: { cte: 'derived' },
      select: [{ type: 'all_from_cte', cte: 'derived' }],
    };
    const errors = validateCteColumnReferences(modelJson, registry);
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('dim_a');
    expect(errors[0]).toContain('does not exist');
  });

  test('dims_from_cte include referencing fct column reports error (type narrowing)', () => {
    const registry = buildRegistryForBaseCtes();
    const modelJson: any = {
      ctes: [
        {
          name: 'derived',
          from: { cte: 'source_cte' },
          select: [
            {
              type: 'dims_from_cte',
              cte: 'source_cte',
              include: ['fct_x'],
            },
          ],
        },
      ],
      from: { cte: 'derived' },
      select: [{ type: 'all_from_cte', cte: 'derived' }],
    };
    const errors = validateCteColumnReferences(modelJson, registry);
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toContain('fct_x');
    expect(errors[0]).toContain('does not exist');
  });
});

describe('validateCteGroupBy', () => {
  test('rejects string alias for computed column', () => {
    const cte = {
      name: 'agg_data',
      from: { model: 'stg_events' },
      select: [
        {
          name: 'month',
          expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
          type: 'dim',
        },
        { name: 'total', type: 'fct', agg: 'sum' },
      ],
      group_by: ['month'],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('string alias "month"');
    expect(errors[0]).toContain('computed expression');
    expect(errors[0]).toContain('DATE_TRUNC');
  });

  test('allows string alias for non-computed column', () => {
    const cte = {
      name: 'agg_data',
      from: { model: 'stg_events' },
      select: [
        { name: 'service', type: 'dim' },
        { name: 'total', type: 'fct', agg: 'sum' },
      ],
      group_by: ['service'],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });

  test('allows { type: "dims" } with computed columns', () => {
    const cte = {
      name: 'agg_data',
      from: { model: 'stg_events' },
      select: [
        {
          name: 'month',
          expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
          type: 'dim',
        },
        { name: 'total', type: 'fct', agg: 'sum' },
      ],
      group_by: [{ type: 'dims' }],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });

  test('allows { expr: "..." } for computed expressions', () => {
    const cte = {
      name: 'agg_data',
      from: { model: 'stg_events' },
      select: [
        {
          name: 'month',
          expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
          type: 'dim',
        },
        { name: 'total', type: 'fct', agg: 'sum' },
      ],
      group_by: [{ expr: "DATE_TRUNC('MONTH', portal_partition_daily)" }],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });

  test('mixed group_by: errors only for computed string aliases', () => {
    const cte = {
      name: 'agg_data',
      from: { model: 'stg_events' },
      select: [
        {
          name: 'month',
          expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
          type: 'dim',
        },
        { name: 'service', type: 'dim' },
        { name: 'total', type: 'fct', agg: 'sum' },
      ],
      group_by: ['month', 'service'],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('"month"');
    expect(errors[0]).not.toContain('"service"');
  });

  test('no error when CTE has no select (passthrough)', () => {
    const cte = {
      name: 'passthrough',
      from: { model: 'stg_events' },
      group_by: ['some_col'],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });

  test('no error when CTE has no group_by', () => {
    const cte = {
      name: 'no_group',
      from: { model: 'stg_events' },
      select: [
        {
          name: 'month',
          expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
          type: 'dim',
        },
      ],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });

  test('no error when select has no computed columns', () => {
    const cte = {
      name: 'simple',
      from: { model: 'stg_events' },
      select: [
        { name: 'service', type: 'dim' },
        { name: 'region', type: 'dim' },
      ],
      group_by: ['service', 'region'],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });

  test('reports errors for multiple computed aliases', () => {
    const cte = {
      name: 'multi_computed',
      from: { model: 'stg_events' },
      select: [
        {
          name: 'month',
          expr: "DATE_TRUNC('MONTH', event_date)",
          type: 'dim',
        },
        { name: 'year', expr: "DATE_TRUNC('YEAR', event_date)", type: 'dim' },
        { name: 'total', type: 'fct', agg: 'sum' },
      ],
      group_by: ['month', 'year'],
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(2);
    expect(errors[0]).toContain('"month"');
    expect(errors[1]).toContain('"year"');
  });
});

describe('validateCteGroupBy with "dims" string shorthand', () => {
  test('allows "dims" shorthand with computed columns', () => {
    const cte = {
      name: 'agg_data',
      from: { model: 'stg_events' },
      select: [
        {
          name: 'month',
          expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
          type: 'dim',
        },
        { name: 'total', type: 'fct', agg: 'sum' },
      ],
      group_by: 'dims',
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });

  test('allows "dims" shorthand without computed columns', () => {
    const cte = {
      name: 'simple',
      from: { model: 'stg_events' },
      select: [
        { name: 'service', type: 'dim' },
        { name: 'region', type: 'dim' },
      ],
      group_by: 'dims',
    };
    const errors = validateCteGroupBy(cte, 0);
    expect(errors).toHaveLength(0);
  });
});

describe('validateCtes with group_by checks', () => {
  test('validateCtes reports computed alias in CTE group_by', () => {
    const errors = validateCtes({
      ctes: [
        {
          name: 'agg_data',
          from: { model: 'stg_events' },
          select: [
            {
              name: 'month',
              expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
              type: 'dim',
            },
            { name: 'service', type: 'dim' },
            { name: 'total', type: 'fct', agg: 'sum' },
          ],
          group_by: ['month', 'service'],
        },
      ],
      from: { cte: 'agg_data' },
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('string alias "month"');
    expect(errors[0]).toContain('computed expression');
  });

  test('validateCtes passes when CTE uses { type: "dims" }', () => {
    const errors = validateCtes({
      ctes: [
        {
          name: 'agg_data',
          from: { model: 'stg_events' },
          select: [
            {
              name: 'month',
              expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
              type: 'dim',
            },
            { name: 'total', type: 'fct', agg: 'sum' },
          ],
          group_by: [{ type: 'dims' }],
        },
      ],
      from: { cte: 'agg_data' },
    });
    expect(errors).toHaveLength(0);
  });

  test('validateCtes only flags the CTE with the issue in multi-CTE model', () => {
    const errors = validateCtes({
      ctes: [
        {
          name: 'clean_cte',
          from: { model: 'stg_events' },
          select: [{ name: 'region', type: 'dim' }],
          group_by: ['region'],
        },
        {
          name: 'bad_cte',
          from: { cte: 'clean_cte' },
          select: [
            {
              name: 'quarter',
              expr: "DATE_TRUNC('QUARTER', event_date)",
              type: 'dim',
            },
            { name: 'count_val', type: 'fct', agg: 'count' },
          ],
          group_by: ['quarter'],
        },
      ],
      from: { cte: 'bad_cte' },
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toContain('ctes[1]');
    expect(errors[0]).toContain('"bad_cte"');
    expect(errors[0]).toContain('"quarter"');
  });

  test('validateCtes passes when CTE uses "dims" string shorthand', () => {
    const errors = validateCtes({
      ctes: [
        {
          name: 'agg_data',
          from: { model: 'stg_events' },
          select: [
            {
              name: 'month',
              expr: "DATE_TRUNC('MONTH', portal_partition_daily)",
              type: 'dim',
            },
            { name: 'total', type: 'fct', agg: 'sum' },
          ],
          group_by: 'dims',
        },
      ],
      from: { cte: 'agg_data' },
    });
    expect(errors).toHaveLength(0);
  });
});
