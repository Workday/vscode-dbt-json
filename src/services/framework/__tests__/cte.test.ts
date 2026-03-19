import { describe, expect, test } from '@jest/globals';
import {
  frameworkBuildCteColumnRegistry,
  frameworkGenerateCteSql,
  frameworkGenerateModelOutput,
  frameworkInferCteColumns,
} from '@services/framework/utils';
import { extractFrameworkDependencies } from '@services/sync/dependencyGraph';
import { validateCtes } from '@services/validationErrors';
import type { DJ } from '@shared';
import type { DbtProject } from '@shared/dbt/types';
import type { FrameworkCTE, FrameworkModel } from '@shared/framework/types';

// Mock config to avoid vscode dependency (matches index.test.ts pattern)
const mockConfig: any = {
  aiHintTag: 'ai',
  airflowGenerateDags: false,
  dbtMacroPath: '_ext_',
  airflowDagsPath: 'dags/_ext_',
};

const createTestDJ = (): DJ => ({
  config: mockConfig,
});

const project: DbtProject = {
  name: 'project',
  macroPaths: ['macros'],
  manifest: {
    child_map: {},
    disabled: {},
    docs: {},
    exposures: {},
    group_map: {},
    groups: {},
    macros: {},
    metadata: { project_name: 'project' },
    metrics: {},
    nodes: {
      ['model.project.model_a']: {
        columns: {
          col_a: { name: 'col_a', data_type: 'varchar', meta: { type: 'dim' } },
          col_b: { name: 'col_b', data_type: 'bigint', meta: { type: 'fct' } },
        },
      },
    },
    parent_map: {},
    saved_queries: {},
    selectors: {},
    semantic_models: {},
    sources: {},
  },
  modelPaths: ['models'],
  packagePath: '',
  pathRelative: '',
  pathSystem: '',
  properties: { vars: { event_dates: '2024-07-01' } },
  targetPath: 'target',
  variables: {},
};

describe('CTE support', () => {
  describe('CTE Column Registry', () => {
    test('CTE column registry infers columns from select items', () => {
      const ctes = [
        {
          name: 'filtered',
          from: { model: 'model_a' },
          select: [
            { name: 'col_a', type: 'dim' as const },
            { name: 'total', type: 'fct' as const, expr: 'SUM(col_b)' },
          ],
        },
        {
          name: 'final',
          from: { cte: 'filtered' },
          select: [{ type: 'all_from_cte' as any, cte: 'filtered' }],
        },
      ];

      const registry = frameworkBuildCteColumnRegistry({
        ctes: ctes as any,
        project,
      });

      expect(registry.get('filtered')).toEqual([
        { name: 'col_a', meta: { type: 'dim' } },
        { name: 'total', meta: { type: 'fct', expr: 'SUM(col_b)' } },
      ]);

      const finalCols = registry.get('final');
      expect(finalCols?.length).toBe(2);
      expect(finalCols?.[0].name).toBe('col_a');
      expect(finalCols?.[1].name).toBe('total');
    });

    test('CTE column inference includes joined CTE columns', () => {
      const registry: Map<string, any[]> = new Map();
      registry.set('data_cte', [
        { name: 'id', meta: { type: 'dim' } },
        { name: 'value', meta: { type: 'fct' } },
      ]);
      registry.set('lookup_cte', [
        { name: 'id', meta: { type: 'dim' } },
        { name: 'label', meta: { type: 'dim' } },
      ]);

      const cte: FrameworkCTE = {
        name: 'joined',
        from: {
          cte: 'data_cte',
          join: [{ cte: 'lookup_cte', on: { and: ['id'] }, type: 'left' }],
        },
      } as any;

      const columns = frameworkInferCteColumns({
        cte,
        cteRegistry: registry,
        project,
      });
      const colNames = columns.map((c: any) => c.name);
      expect(colNames).toContain('id');
      expect(colNames).toContain('value');
      expect(colNames).toContain('label');
    });
  });

  describe('CTE SQL Generation', () => {
    test('CTE SQL generation produces correct SELECT/FROM/WHERE', () => {
      const cte = {
        name: 'filtered',
        from: { model: 'model_a' },
        select: [
          { name: 'col_a', type: 'dim' as const },
          { name: 'total', type: 'fct' as const, expr: 'SUM(col_b)' },
        ],
        where: { and: [{ expr: 'col_a > 0' }] },
      };

      const registry = frameworkBuildCteColumnRegistry({
        ctes: [cte as any],
        project,
      });
      const sql = frameworkGenerateCteSql({
        cte: cte as any,
        cteRegistry: registry,
        project,
      });

      expect(sql).toContain('select col_a, SUM(col_b) as total');
      expect(sql).toContain("from {{ ref('model_a') }}");
      expect(sql).toContain('where col_a > 0');
    });

    test('CTE SQL generation for CTE-to-CTE reference', () => {
      const cte = {
        name: 'aggregated',
        from: { cte: 'filtered' },
        select: [{ name: 'col_a', type: 'dim' as const }],
      };

      const registry = frameworkBuildCteColumnRegistry({
        ctes: [cte as any],
        project,
      });
      const sql = frameworkGenerateCteSql({
        cte: cte as any,
        cteRegistry: registry,
        project,
      });

      expect(sql).toContain('select col_a');
      expect(sql).toContain('from filtered');
      expect(sql).not.toContain("ref('");
    });

    test('CTE SQL generation for UNION between CTEs', () => {
      const ctes = [
        {
          name: 'aws_raw',
          from: { model: 'model_a' },
          select: [{ name: 'cost', type: 'fct' as const }],
        },
        {
          name: 'gcp_raw',
          from: { model: 'model_a' },
          select: [{ name: 'cost', type: 'fct' as const }],
        },
        {
          name: 'combined',
          from: {
            cte: 'aws_raw',
            union: { type: 'all' as const, ctes: ['gcp_raw'] },
          },
        },
      ];

      const registry = frameworkBuildCteColumnRegistry({
        ctes: ctes as any,
        project,
      });
      const sql = frameworkGenerateCteSql({
        cte: ctes[2] as any,
        cteRegistry: registry,
        project,
      });

      expect(sql).toContain('select * from aws_raw');
      expect(sql).toContain('union all');
      expect(sql).toContain('select * from gcp_raw');
    });

    test('CTE SQL generation with CTE-to-CTE join uses bare name (no ref)', () => {
      const cteA: FrameworkCTE = {
        name: 'base_data',
        from: { model: 'model_a' },
        select: ['col_a', 'col_b'],
      } as any;
      const cteB: FrameworkCTE = {
        name: 'lookup',
        from: { model: 'model_b' },
        select: ['col_b', 'col_c'],
      } as any;
      const cteJoined: FrameworkCTE = {
        name: 'joined',
        from: {
          cte: 'base_data',
          join: [{ cte: 'lookup', on: { and: ['col_b'] }, type: 'left' }],
        },
        select: ['col_a', 'col_c'],
      } as any;

      const registry: Map<string, any[]> = new Map();
      registry.set('base_data', [
        { name: 'col_a', meta: { type: 'dim' } },
        { name: 'col_b', meta: { type: 'dim' } },
      ]);
      registry.set('lookup', [
        { name: 'col_b', meta: { type: 'dim' } },
        { name: 'col_c', meta: { type: 'dim' } },
      ]);

      const sql = frameworkGenerateCteSql({
        cte: cteJoined,
        project,
        cteRegistry: registry,
      });
      expect(sql).toContain('from base_data');
      expect(sql).toContain('left join lookup');
      expect(sql).toContain('base_data.col_b = lookup.col_b');
      expect(sql).not.toContain("ref('base_data')");
      expect(sql).not.toContain("ref('lookup')");
    });

    test('model with CTEs generates SQL with WITH clause prepended', () => {
      const modelJson: FrameworkModel = {
        type: 'int_select_model',
        group: 'test',
        topic: 'cte',
        name: 'result',
        ctes: [
          {
            name: 'filtered',
            from: { model: 'model_a' },
            select: [{ name: 'col_a', type: 'dim' as const }],
          },
        ],
        from: { cte: 'filtered' },
        select: [{ type: 'all_from_cte' as any, cte: 'filtered' }],
      } as any;

      const { sql } = frameworkGenerateModelOutput({
        dj: createTestDJ(),
        modelJson,
        project,
      });

      expect(sql).toContain('filtered AS (');
      expect(sql).toContain("{{ ref('model_a') }}");
      expect(sql).toContain('int__test__cte__result AS (');
      expect(sql).toContain('filtered');
    });

    test('main model from.cte + join[{ cte }] generates correct SQL', () => {
      const joinProject: DbtProject = {
        ...project,
        manifest: {
          ...project.manifest,
          nodes: {
            ['model.project.model_x']: {
              columns: {
                key: {
                  name: 'key',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
                val: {
                  name: 'val',
                  data_type: 'bigint',
                  meta: { type: 'fct' },
                },
              },
            },
            ['model.project.model_y']: {
              columns: {
                key: {
                  name: 'key',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
                info: {
                  name: 'info',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
              },
            },
          },
        },
      };

      const modelJson: FrameworkModel = {
        type: 'mart_join_models',
        group: 'test',
        topic: 'cte',
        name: 'main_cte_join',
        ctes: [
          {
            name: 'cte_base',
            from: { model: 'model_x' },
            select: ['key', 'val'],
          },
          {
            name: 'cte_ref',
            from: { model: 'model_y' },
            select: ['key', 'info'],
          },
        ],
        from: {
          cte: 'cte_base',
          join: [{ cte: 'cte_ref', on: { and: ['key'] }, type: 'left' }],
        },
        select: [
          { type: 'all_from_cte', cte: 'cte_base' },
          { cte: 'cte_ref', name: 'info' },
        ],
      } as any;

      const result = frameworkGenerateModelOutput({
        dj: createTestDJ(),
        modelJson,
        project: joinProject,
      });

      expect(result.sql.toUpperCase()).toContain('LEFT JOIN CTE_REF');
      expect(result.sql).toContain('cte_base.key = cte_ref.key');
      expect(result.sql).not.toContain("ref('cte_base')");
      expect(result.sql).not.toContain("ref('cte_ref')");
    });
  });

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

  describe('CTE Integration / Full Model', () => {
    test('int_union_models with CTEs generates CTE-based UNION ALL SQL', () => {
      const unionProject: DbtProject = {
        ...project,
        manifest: {
          ...project.manifest,
          nodes: {
            ['model.project.model_a']: {
              columns: {
                col_a: {
                  name: 'col_a',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
                col_b: {
                  name: 'col_b',
                  data_type: 'bigint',
                  meta: { type: 'fct' },
                },
              },
            },
            ['model.project.model_b']: {
              columns: {
                col_x: {
                  name: 'col_x',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
                col_y: {
                  name: 'col_y',
                  data_type: 'bigint',
                  meta: { type: 'fct' },
                },
              },
            },
          },
        },
      };

      const modelJson: FrameworkModel = {
        type: 'int_union_models',
        group: 'test',
        topic: 'cte',
        name: 'union_normalized',
        ctes: [
          {
            name: 'normalized_a',
            from: { model: 'model_a' },
            select: [
              { name: 'id', expr: 'col_a' },
              { name: 'value', expr: 'col_b' },
            ],
          },
          {
            name: 'normalized_b',
            from: { model: 'model_b' },
            select: [
              { name: 'id', expr: 'col_x' },
              { name: 'value', expr: 'col_y' },
            ],
          },
        ],
        from: {
          cte: 'normalized_a',
          union: { type: 'all', ctes: ['normalized_b'] },
        },
        select: [{ type: 'all_from_cte', cte: 'normalized_a' }],
      } as any;

      const result = frameworkGenerateModelOutput({
        dj: createTestDJ(),
        modelJson,
        project: unionProject,
      });

      expect(result.sql).toContain('normalized_a');
      expect(result.sql).toContain('normalized_b');
      expect(result.sql.toUpperCase()).toContain('UNION ALL');
      expect(result.sql).toContain("ref('model_a')");
      expect(result.sql).toContain("ref('model_b')");
      expect(result.sql).toContain('normalized_a');
      expect(result.sql).toContain('normalized_b');
      const mainCteSql =
        result.sql.split('int__test__cte__union_normalized')[1] || '';
      expect(mainCteSql).not.toContain("ref('normalized_a')");
      expect(mainCteSql).not.toContain("ref('normalized_b')");
    });

    test('int_union_models with CTEs produces correct YML columns', () => {
      const unionProject: DbtProject = {
        ...project,
        manifest: {
          ...project.manifest,
          nodes: {
            ['model.project.model_a']: {
              columns: {
                col_a: {
                  name: 'col_a',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
              },
            },
            ['model.project.model_b']: {
              columns: {
                col_x: {
                  name: 'col_x',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
              },
            },
          },
        },
      };

      const modelJson: FrameworkModel = {
        type: 'int_union_models',
        group: 'test',
        topic: 'cte',
        name: 'union_yml',
        ctes: [
          {
            name: 'norm_a',
            from: { model: 'model_a' },
            select: [{ name: 'id', expr: 'col_a' }],
          },
          {
            name: 'norm_b',
            from: { model: 'model_b' },
            select: [{ name: 'id', expr: 'col_x' }],
          },
        ],
        from: {
          cte: 'norm_a',
          union: { type: 'all', ctes: ['norm_b'] },
        },
        select: [{ type: 'all_from_cte', cte: 'norm_a' }],
      } as any;

      const result = frameworkGenerateModelOutput({
        dj: createTestDJ(),
        modelJson,
        project: unionProject,
      });

      expect(result.yml).toContain('id');
      expect(result.properties.columns.length).toBeGreaterThan(0);
      expect(result.properties.columns.some((c: any) => c.name === 'id')).toBe(
        true,
      );
    });

    test('int_union_models without CTEs still works (regression)', () => {
      const unionProject: DbtProject = {
        ...project,
        manifest: {
          ...project.manifest,
          nodes: {
            ['model.project.model_a']: {
              columns: {
                shared_col: {
                  name: 'shared_col',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
              },
            },
            ['model.project.model_b']: {
              columns: {
                shared_col: {
                  name: 'shared_col',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
              },
            },
          },
        },
      };

      const modelJson: FrameworkModel = {
        type: 'int_union_models',
        group: 'test',
        topic: 'union',
        name: 'classic',
        from: {
          model: 'model_a',
          union: { models: ['model_b'] },
        },
      } as any;

      const result = frameworkGenerateModelOutput({
        dj: createTestDJ(),
        modelJson,
        project: unionProject,
      });

      expect(result.sql).toContain("ref('model_a')");
      expect(result.sql).toContain("ref('model_b')");
      expect(result.sql.toUpperCase()).toContain('UNION ALL');
      expect(result.sql).not.toContain('normalized_');
    });

    test('full model with CTE-to-CTE join generates correct SQL', () => {
      const joinProject: DbtProject = {
        ...project,
        manifest: {
          ...project.manifest,
          nodes: {
            ['model.project.ext_model_a']: {
              columns: {
                id: { name: 'id', data_type: 'varchar', meta: { type: 'dim' } },
                value: {
                  name: 'value',
                  data_type: 'bigint',
                  meta: { type: 'fct' },
                },
              },
            },
            ['model.project.ext_model_b']: {
              columns: {
                id: { name: 'id', data_type: 'varchar', meta: { type: 'dim' } },
                label: {
                  name: 'label',
                  data_type: 'varchar',
                  meta: { type: 'dim' },
                },
              },
            },
          },
        },
      };

      const modelJson: FrameworkModel = {
        type: 'mart_select_model',
        group: 'test',
        topic: 'cte',
        name: 'cte_join',
        tags: ['lightdash'],
        ctes: [
          {
            name: 'data_cte',
            from: { model: 'ext_model_a' },
            select: ['id', 'value'],
          },
          {
            name: 'lookup_cte',
            from: { model: 'ext_model_b' },
            select: ['id', 'label'],
          },
          {
            name: 'final_joined',
            from: {
              cte: 'data_cte',
              join: [{ cte: 'lookup_cte', on: { and: ['id'] }, type: 'left' }],
            },
            select: [
              { type: 'all_from_cte', cte: 'data_cte' },
              { cte: 'lookup_cte', name: 'label' },
            ],
          },
        ],
        from: { cte: 'final_joined' },
        select: [{ type: 'all_from_cte', cte: 'final_joined' }],
      } as any;

      const result = frameworkGenerateModelOutput({
        dj: createTestDJ(),
        modelJson,
        project: joinProject,
      });

      expect(result.sql).toContain('data_cte AS (');
      expect(result.sql).toContain('lookup_cte AS (');
      expect(result.sql).toContain('final_joined AS (');
      expect(result.sql.toUpperCase()).toContain('LEFT JOIN LOOKUP_CTE');
      expect(result.sql).toContain('data_cte.id = lookup_cte.id');
      expect(result.sql).not.toContain("ref('data_cte')");
      expect(result.sql).not.toContain("ref('lookup_cte')");
      expect(result.sql).toContain("ref('ext_model_a')");
      expect(result.sql).toContain("ref('ext_model_b')");
    });
  });
});
