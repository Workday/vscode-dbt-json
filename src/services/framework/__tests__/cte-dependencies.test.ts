import { describe, expect, test } from '@jest/globals';
import { extractFrameworkDependencies } from '@services/sync/dependencyGraph';
import type { FrameworkModel } from '@shared/framework/types';

describe('CTE Dependencies', () => {
  test('extractFrameworkDependencies extracts CTE model dependencies', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'test',
      topic: 'cte',
      name: 'result',
      ctes: [
        { name: 'cte_a', from: { model: 'external_model_1' } },
        { name: 'cte_b', from: { cte: 'cte_a' } },
      ],
      from: { cte: 'cte_b' },
      select: [{ type: 'all_from_cte' as any, cte: 'cte_b' }],
    } as any;

    const deps = extractFrameworkDependencies(modelJson);
    expect(deps).toContain('external_model_1');
    expect(deps.length).toBe(1);
  });

  test('extractFrameworkDependencies extracts CTE join dependencies', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'test',
      topic: 'cte',
      name: 'result',
      ctes: [
        {
          name: 'cte_a',
          from: {
            model: 'base_model',
            join: [{ model: 'join_model', on: { and: ['col_a'] } }],
          },
        },
      ],
      from: { cte: 'cte_a' },
      select: [{ type: 'all_from_cte' as any, cte: 'cte_a' }],
    } as any;

    const deps = extractFrameworkDependencies(modelJson);
    expect(deps).toContain('base_model');
    expect(deps).toContain('join_model');
  });

  test('extractFrameworkDependencies handles CTE-based union models', () => {
    const modelJson: FrameworkModel = {
      type: 'int_union_models',
      group: 'test',
      topic: 'cte',
      name: 'union_deps',
      ctes: [
        { name: 'cte_a', from: { model: 'ext_model_a' } },
        { name: 'cte_b', from: { model: 'ext_model_b' } },
      ],
      from: {
        cte: 'cte_a',
        union: { type: 'all', ctes: ['cte_b'] },
      },
      select: [{ type: 'all_from_cte' as any, cte: 'cte_a' }],
    } as any;

    const deps = extractFrameworkDependencies(modelJson);
    expect(deps).toContain('ext_model_a');
    expect(deps).toContain('ext_model_b');
  });

  test('extractFrameworkDependencies ignores CTE join refs (no false external deps)', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'test',
      topic: 'cte',
      name: 'cte_join_deps',
      ctes: [
        { name: 'cte_a', from: { model: 'ext_model' } },
        {
          name: 'cte_b',
          from: {
            model: 'ext_model_2',
            join: [{ cte: 'cte_a', on: { and: ['col'] }, type: 'left' }],
          },
        },
      ],
      from: { cte: 'cte_b' },
      select: [{ type: 'all_from_cte' as any, cte: 'cte_b' }],
    } as any;

    const deps = extractFrameworkDependencies(modelJson);
    expect(deps).toContain('ext_model');
    expect(deps).toContain('ext_model_2');
    expect(deps).not.toContain('cte_a');
    expect(deps).not.toContain('cte_b');
  });
});
