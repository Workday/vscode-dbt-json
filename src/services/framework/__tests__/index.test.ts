import { describe, expect, test } from '@jest/globals';
import {
  frameworkBuildColumns,
  frameworkGenerateModelOutput,
  frameworkGetModelName,
} from '@services/framework/utils';
import { sqlFormat } from '@services/utils/sql';
import type { DJ } from '@shared';
import { jsonParse } from '@shared';
import type { DbtProject } from '@shared/dbt/types';
import type { FrameworkModel } from '@shared/framework/types';
import * as fs from 'fs';
import { glob } from 'glob';
import * as path from 'path';

import { createTestDJ } from './helpers';

describe('framework fixtures', () => {
  test('test that all .sql & .yml match when generated from .model.json', () => {
    const pathRelative = '../../../../tests/fixtures';
    const pathSystem = path.join(__dirname, pathRelative);
    const pathManifest = path.join(pathSystem, 'manifest.json');
    const projectManifest = jsonParse(fs.readFileSync(pathManifest, 'utf8'));

    const project: DbtProject = {
      macroPaths: ['macros'],
      manifest: projectManifest,
      modelPaths: ['models'],
      name: projectManifest.metadata.project_name,
      pathRelative,
      pathSystem,
      properties: { vars: { event_dates: '2024-01-01' } },
      targetPath: 'target',
    };

    const pathsModelJson = glob.sync(`${pathSystem}/*.model.json`);
    for (const pathModelJson of pathsModelJson) {
      const pathSql = pathModelJson.replace(/\.model\.json$/, '.sql');
      const pathYml = pathModelJson.replace(/\.model\.json$/, '.yml');

      const fileModelJson = jsonParse(fs.readFileSync(pathModelJson, 'utf8'));
      const fileSql = fs.readFileSync(pathSql, 'utf8');
      const fileYml = fs.readFileSync(pathYml, 'utf8');

      const { sql: generatedSql, yml: generatedYml } =
        frameworkGenerateModelOutput({
          dj: createTestDJ(),
          modelJson: fileModelJson,
          project,
        });

      expect(generatedSql).toBe(fileSql);
      expect(generatedYml).toBe(fileYml);
    }
  });
});

describe('framework unit tests', () => {
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
      metadata: {
        project_name: 'project',
      },
      metrics: {},
      nodes: {
        ['model.project.model_a']: {
          columns: {
            dim_a: {
              name: 'dim_a',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
            dim_b: {
              name: 'dim_b',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
          },
        },
        ['model.project.model_b']: {
          columns: {
            dim_a: {
              name: 'dim_a',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
            dim_b: {
              name: 'dim_b',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
            amount: {
              name: 'amount',
              data_type: 'double',
              meta: { type: 'fct' },
            },
          },
        },
        ['model.project.model_with_aggs']: {
          columns: {
            datetime: {
              name: 'datetime',
              data_type: 'timestamp',
              meta: { type: 'dim' },
            },
            store_id: {
              name: 'store_id',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
            store_name: {
              name: 'store_name',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
            revenue_sum: {
              name: 'revenue_sum',
              data_type: 'double',
              meta: { type: 'fct' },
            },
            events_count: {
              name: 'events_count',
              data_type: 'bigint',
              meta: { type: 'fct' },
            },
            price_min: {
              name: 'price_min',
              data_type: 'double',
              meta: { type: 'fct' },
            },
            price_max: {
              name: 'price_max',
              data_type: 'double',
              meta: { type: 'fct' },
            },
          },
        },
      },
      parent_map: {},
      saved_queries: {},
      selectors: {},
      semantic_models: {},
      sources: {
        source_a: {
          name: 'source_a',
        },
      },
    },
    modelPaths: ['models'],
    packagePath: '',
    pathRelative: '',
    pathSystem: '',
    properties: { vars: { event_dates: '2024-07-01' } },
    targetPath: 'target',
    variables: {},
  };
  test('staging model sql and yml files are generated correctly', () => {
    const modelJson: FrameworkModel = {
      type: 'stg_select_source',
      group: 'ml',
      topic: 'test',
      name: 'model_memory_requested',
      select: ['column_a'],
      from: { source: 'source_a.table_a' },
    };
    const modelName = frameworkGetModelName(modelJson);

    expect(
      frameworkGenerateModelOutput({ dj: createTestDJ(), modelJson, project })
        .sql,
    ).toBe(
      `
{{
  config(
    materialized="ephemeral"
  )
}}
`.trim() +
        '\n\n' +
        sqlFormat(`
with ${modelName} as (
  select
    column_a,
    1 as portal_source_count
  from
    {{ source('source_a','table_a') }}
)

select * from ${modelName}
`).trim(),
    );

    expect(
      frameworkGenerateModelOutput({ dj: createTestDJ(), modelJson, project })
        .yml,
    ).toBe(
      `version: "2"
models:
  - name: ${modelName}
    group: ${modelJson.group}
    description: ""
    docs:
      node_color: "#B6AB33"
      show: true
    private: false
    contract:
      enforced: false
    config:
      tags:
        - json
        - staging
    columns:
      - name: column_a
        data_type: varchar
        description: Column A
        meta:
          type: dim
      - name: portal_source_count
        data_type: bigint
        description: Portal Source Count
        meta:
          type: fct
          dimension:
            label: Portal Source Count
            hidden: true
          metrics:
            metric_portal_source_count:
              type: sum
              label: Portal Source Count
`,
    );
  });

  test('join model sql is generated correctly', () => {
    const modelJson: FrameworkModel = {
      type: 'int_join_models',
      group: 'ml',
      name: 'model_c',
      select: [
        { type: 'dims_from_model', model: 'model_a' },
        { type: 'dims_from_model', model: 'model_b' },
      ],
      from: {
        model: 'model_a',
        join: [
          {
            model: 'model_b',
            on: { and: ['dim_a', 'dim_b'] },
          },
        ],
      },
    };
    const modelName = frameworkGetModelName(modelJson);

    expect(
      frameworkGenerateModelOutput({
        dj: createTestDJ(),
        modelJson,
        project,
      }).sql,
    ).toBe(
      `
{{
  config(
    materialized="ephemeral"
  )
}}
`.trim() +
        '\n\n' +
        sqlFormat(`
with ${modelName} as (
  select
      model_a.dim_a,
      model_a.dim_b
  from
      {{ ref('model_a') }} model_a
  inner join
      {{ ref('model_b') }} model_b
  on model_a.dim_a = model_b.dim_a and model_a.dim_b = model_b.dim_b
)
select * from ${modelName}
        `),
    );
  });

  test('join model with aggregation generates correct sql', () => {
    const modelJson: FrameworkModel = {
      type: 'int_join_models',
      group: 'ml',
      name: 'model_c_agg',
      select: [
        { type: 'dims_from_model', model: 'model_a' },
        { model: 'model_b', name: 'amount', type: 'fct', agg: 'sum' },
      ],
      from: {
        model: 'model_a',
        join: [
          {
            model: 'model_b',
            on: { and: ['dim_a'] },
          },
        ],
      },
    };
    const modelName = frameworkGetModelName(modelJson);

    const result = frameworkGenerateModelOutput({
      dj: createTestDJ(),
      modelJson,
      project,
    });

    expect(result.sql).toBe(
      `
{{
  config(
    materialized="ephemeral"
  )
}}
`.trim() +
        '\n\n' +
        sqlFormat(`
with ${modelName} as (
  select
      sum(model_b.amount) as amount_sum,
      model_a.dim_a,
      model_a.dim_b
  from
      {{ ref('model_a') }} model_a
  inner join
      {{ ref('model_b') }} model_b
  on model_a.dim_a = model_b.dim_a
  group by
      model_a.dim_a,
      model_a.dim_b
)
select * from ${modelName}
        `),
    );
  });

  test('builds columns correctly', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'analytics',
      topic: 'billing',
      name: 'cost_aggregated',
      from: {
        model: 'stg__analytics__billing__raw_data',
      },
      select: [
        {
          name: 'count_star',
          type: 'fct',
          agg: 'sum',
        },
        {
          name: 'csp_ondemand_cost',
          expr: 'csp_ondemand_cost',
          type: 'fct',
          aggs: ['sum', 'min', 'max'],
          description:
            'On Demand Cost if resource were purchased without any discounts',
        },
      ],
    };
    expect(
      frameworkBuildColumns({ dj: createTestDJ(), modelJson, project }).columns,
    ).toEqual([
      {
        name: 'count_star',
        data_type: 'number',
        meta: {
          type: 'fct',
          origin: {
            id: 'model.project.int__analytics__billing__cost_aggregated',
          },
          agg: 'sum',
        },
      },
      {
        name: 'csp_ondemand_cost',
        description:
          'On Demand Cost if resource were purchased without any discounts',
        data_type: 'number',
        meta: {
          expr: 'csp_ondemand_cost',
          type: 'fct',
          agg: 'sum',
        },
      },
      {
        name: 'csp_ondemand_cost',
        description:
          'On Demand Cost if resource were purchased without any discounts',
        data_type: 'number',
        meta: {
          expr: 'csp_ondemand_cost',
          type: 'fct',
          agg: 'min',
        },
      },
      {
        name: 'csp_ondemand_cost',
        description:
          'On Demand Cost if resource were purchased without any discounts',
        data_type: 'number',
        meta: {
          expr: 'csp_ondemand_cost',
          type: 'fct',
          agg: 'max',
        },
      },
    ]);
  });

  test('int_select_model with group_by does not re-aggregate without from.rollup', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'analytics',
      topic: 'reporting',
      name: 'store_summary',
      from: {
        model: 'model_with_aggs',
      },
      select: [
        'store_id',
        'store_name',
        { name: 'revenue_sum', type: 'fct' },
        { name: 'events_count', type: 'fct' },
        { name: 'price_min', type: 'fct' },
        { name: 'price_max', type: 'fct' },
        {
          name: 'total_days',
          expr: 'COUNT(DISTINCT event_date)',
          type: 'fct',
        },
      ],
      group_by: [{ type: 'dims' }],
    };
    const modelName = frameworkGetModelName(modelJson);

    const result = frameworkGenerateModelOutput({
      dj: createTestDJ(),
      modelJson,
      project,
    });

    expect(result.sql).toBe(
      `
{{
  config(
    materialized="ephemeral"
  )
}}
`.trim() +
        '\n\n' +
        sqlFormat(`
with ${modelName} as (
  select
      datetime,
      events_count,
      price_max,
      price_min,
      revenue_sum,
      store_id,
      store_name,
      COUNT(DISTINCT event_date) as total_days
  from
      {{ ref('model_with_aggs') }}
  group by
      datetime,
      store_id,
      store_name
)
select * from ${modelName}
        `),
    );
  });

  test('int_select_model with from.rollup + explicit select generates rollup SQL', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'analytics',
      topic: 'reporting',
      name: 'store_daily',
      from: {
        model: 'model_with_aggs',
        rollup: { interval: 'day' },
      },
      select: [
        'store_id',
        { name: 'revenue_sum', type: 'fct' },
        { name: 'events_count', type: 'fct' },
        {
          name: 'custom_metric',
          expr: 'COUNT(DISTINCT store_name)',
          type: 'fct',
        },
      ],
    };
    const modelName = frameworkGetModelName(modelJson);

    const result = frameworkGenerateModelOutput({
      dj: createTestDJ(),
      modelJson,
      project,
    });

    expect(result.sql).toBe(
      `
{{
  config(
    materialized="ephemeral"
  )
}}
`.trim() +
        '\n\n' +
        sqlFormat(`
with ${modelName} as (
  select
      COUNT(DISTINCT store_name) as custom_metric,
      date_trunc('day', datetime) as datetime,
      sum(events_count) as events_count,
      sum(revenue_sum) as revenue_sum,
      store_id
  from
      {{ ref('model_with_aggs') }}
  group by
      date_trunc('day', datetime),
      store_id
)
select * from ${modelName}
        `),
    );
  });
});

describe('materialization shorthand', () => {
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
            dim_a: {
              name: 'dim_a',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
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

  test('"materialization": "incremental" uses default strategy from config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'delete+insert',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('"materialization": "incremental" respects config override to merge', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('merge');
  });

  test('"materialization": "ephemeral" string shorthand generates ephemeral config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'ephemeral',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const { config } = frameworkGenerateModelOutput({
      dj: createTestDJ(),
      modelJson,
      project,
    });

    expect(config.materialized).toBe('ephemeral');
    expect(config.incremental_strategy).toBeUndefined();
  });

  test('materialization object with explicit strategy takes precedence over config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: {
        type: 'incremental',
        strategy: { type: 'delete+insert', unique_key: 'dim_a' },
      },
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
    expect(config.unique_key).toBe('dim_a');
  });

  test('"materialized" string without incremental_strategy uses config default', () => {
    const modelJson: FrameworkModel = {
      type: 'stg_select_source',
      group: 'sales',
      topic: 'orders',
      name: 'raw',
      materialized: 'incremental',
      select: ['dim_a'],
      from: { source: 'source_a.table_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'delete+insert',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('defaults to delete+insert when config is unset', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = { config: {} };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('materialization object without strategy uses config default', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: { type: 'incremental' },
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('merge');
  });

  test('overwrite_existing_partitions from config is applied', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy:
          'overwrite_existing_partitions',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('overwrite_existing_partitions');
  });
});

describe('materialization shorthand', () => {
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
            dim_a: {
              name: 'dim_a',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
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

  test('"materialization": "incremental" uses default strategy from config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'delete+insert',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('"materialization": "incremental" respects config override to merge', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('merge');
  });

  test('"materialization": "ephemeral" string shorthand generates ephemeral config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'ephemeral',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const { config } = frameworkGenerateModelOutput({
      dj: createTestDJ(),
      modelJson,
      project,
    });

    expect(config.materialized).toBe('ephemeral');
    expect(config.incremental_strategy).toBeUndefined();
  });

  test('materialization object with explicit strategy takes precedence over config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: {
        type: 'incremental',
        strategy: { type: 'delete+insert', unique_key: 'dim_a' },
      },
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
    expect(config.unique_key).toBe('dim_a');
  });

  test('"materialized" string without incremental_strategy uses config default', () => {
    const modelJson: FrameworkModel = {
      type: 'stg_select_source',
      group: 'sales',
      topic: 'orders',
      name: 'raw',
      materialized: 'incremental',
      select: ['dim_a'],
      from: { source: 'source_a.table_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'delete+insert',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('defaults to delete+insert when config is unset', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = { config: {} };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('materialization object without strategy uses config default', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: { type: 'incremental' },
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('merge');
  });

  test('overwrite_existing_partitions from config is applied', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy:
          'overwrite_existing_partitions',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('overwrite_existing_partitions');
  });
});

describe('materialization shorthand', () => {
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
            dim_a: {
              name: 'dim_a',
              data_type: 'varchar',
              meta: { type: 'dim' },
            },
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

  test('"materialization": "incremental" uses default strategy from config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'delete+insert',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('"materialization": "incremental" respects config override to merge', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('merge');
  });

  test('"materialization": "ephemeral" string shorthand generates ephemeral config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'ephemeral',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const { config } = frameworkGenerateModelOutput({
      dj: createTestDJ(),
      modelJson,
      project,
    });

    expect(config.materialized).toBe('ephemeral');
    expect(config.incremental_strategy).toBeUndefined();
  });

  test('materialization object with explicit strategy takes precedence over config', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: {
        type: 'incremental',
        strategy: { type: 'delete+insert', unique_key: 'dim_a' },
      },
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
    expect(config.unique_key).toBe('dim_a');
  });

  test('"materialized" string without incremental_strategy uses config default', () => {
    const modelJson: FrameworkModel = {
      type: 'stg_select_source',
      group: 'sales',
      topic: 'orders',
      name: 'raw',
      materialized: 'incremental',
      select: ['dim_a'],
      from: { source: 'source_a.table_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'delete+insert',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('defaults to delete+insert when config is unset', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = { config: {} };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('delete+insert');
  });

  test('materialization object without strategy uses config default', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: { type: 'incremental' },
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy: 'merge',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('merge');
  });

  test('overwrite_existing_partitions from config is applied', () => {
    const modelJson: FrameworkModel = {
      type: 'int_select_model',
      group: 'sales',
      topic: 'orders',
      name: 'enriched',
      materialization: 'incremental',
      select: [{ model: 'model_a', columns: ['dim_a'] }],
      from: { model: 'model_a' },
    } as unknown as FrameworkModel;

    const dj: DJ = {
      config: {
        materializationDefaultIncrementalStrategy:
          'overwrite_existing_partitions',
      },
    };

    const { config } = frameworkGenerateModelOutput({
      dj,
      modelJson,
      project,
    });

    expect(config.materialized).toBe('incremental');
    expect(config.incremental_strategy).toBe('overwrite_existing_partitions');
  });
});
