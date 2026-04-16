import { describe, expect, test } from '@jest/globals';
import {
  frameworkBuildCteColumnRegistry,
  frameworkGenerateCteSql,
} from '@services/framework/utils';
import { resolveJoinOnSql } from '@services/framework/utils/sql-utils';
import { JOIN_ON_DIMS } from '@shared/framework/constants';

import { createTestProject } from './helpers';

const project = createTestProject({
  nodes: {
    ['model.project.base_model']: {
      columns: {
        tenant_name: { name: 'tenant_name', meta: { type: 'dim' } },
        region: { name: 'region', meta: { type: 'dim' } },
        revenue: { name: 'revenue', meta: { type: 'fct' } },
      },
    },
    ['model.project.join_model']: {
      columns: {
        tenant_name: { name: 'tenant_name', meta: { type: 'dim' } },
        service: { name: 'service', meta: { type: 'dim' } },
        region: { name: 'region', meta: { type: 'dim' } },
        cost: { name: 'cost', meta: { type: 'fct' } },
      },
    },
    ['model.project.no_overlap_model']: {
      columns: {
        product_id: { name: 'product_id', meta: { type: 'dim' } },
        amount: { name: 'amount', meta: { type: 'fct' } },
      },
    },
  },
});

describe('resolveJoinOnSql', () => {
  test('on: "dims" produces equi-join for shared dimension columns', () => {
    const result = resolveJoinOnSql({
      on: JOIN_ON_DIMS,
      baseAlias: 'base_model',
      joinAlias: 'join_model',
      project,
      baseRef: { model: 'base_model' },
      joinRef: { model: 'join_model' },
    });
    expect(result).toEqual([
      'base_model.tenant_name=join_model.tenant_name',
      'base_model.region=join_model.region',
    ]);
  });

  test('on: "dims" returns empty array when no shared dimensions', () => {
    const result = resolveJoinOnSql({
      on: JOIN_ON_DIMS,
      baseAlias: 'base_model',
      joinAlias: 'no_overlap_model',
      project,
      baseRef: { model: 'base_model' },
      joinRef: { model: 'no_overlap_model' },
    });
    expect(result).toEqual([]);
  });

  test('on: { and: [...] } with string conditions', () => {
    const result = resolveJoinOnSql({
      on: { and: ['tenant_name', 'region'] },
      baseAlias: 'base_model',
      joinAlias: 'join_model',
      project,
      baseRef: { model: 'base_model' },
      joinRef: { model: 'join_model' },
    });
    expect(result).toEqual([
      'base_model.tenant_name=join_model.tenant_name',
      'base_model.region=join_model.region',
    ]);
  });

  test('on: { and: [...] } with expr conditions', () => {
    const result = resolveJoinOnSql({
      on: { and: [{ expr: 'a.id = b.id' }] },
      baseAlias: 'a',
      joinAlias: 'b',
      project,
      baseRef: { model: 'base_model' },
      joinRef: { model: 'join_model' },
    });
    expect(result).toEqual(['a.id = b.id']);
  });

  test('on: "dims" with CTE base resolves from cteRegistry', () => {
    const cteRegistry = new Map([
      [
        'cte_base',
        [
          { name: 'tenant_name', meta: { type: 'dim' as const } },
          { name: 'region', meta: { type: 'dim' as const } },
          { name: 'revenue', meta: { type: 'fct' as const } },
        ] as any,
      ],
    ]);
    const result = resolveJoinOnSql({
      on: JOIN_ON_DIMS,
      baseAlias: 'cte_base',
      joinAlias: 'join_model',
      project,
      cteRegistry,
      baseRef: { cte: 'cte_base' },
      joinRef: { model: 'join_model' },
    });
    expect(result).toEqual([
      'cte_base.tenant_name=join_model.tenant_name',
      'cte_base.region=join_model.region',
    ]);
  });
});

describe('CTE join with on: "dims"', () => {
  test('generates equi-join SQL for shared dims in CTE joins', () => {
    const ctes = [
      {
        name: 'cte_a',
        from: { model: 'base_model' },
        select: [
          { name: 'tenant_name', type: 'dim' as const },
          { name: 'region', type: 'dim' as const },
          { name: 'revenue', type: 'fct' as const },
        ],
      },
      {
        name: 'cte_b',
        from: {
          cte: 'cte_a',
          join: [{ model: 'join_model', on: 'dims' as const }],
        },
        select: [{ name: 'tenant_name', type: 'dim' as const }],
      },
    ];
    const registry = frameworkBuildCteColumnRegistry({
      ctes: ctes as any,
      project,
    });
    const sql = frameworkGenerateCteSql({
      cte: ctes[1] as any,
      cteRegistry: registry,
      project,
    });
    expect(sql).toContain('inner join');
    expect(sql).toContain('cte_a.tenant_name=join_model.tenant_name');
    expect(sql).toContain('cte_a.region=join_model.region');
  });
});
