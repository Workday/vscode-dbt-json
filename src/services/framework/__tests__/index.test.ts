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

// Mock config directly to avoid vscode dependency
const mockConfig: any = {
  aiHintTag: 'ai',
  airflowGenerateDags: false,
  dbtMacroPath: '_ext_',
  airflowDagsPath: 'dags/_ext_',
};

// Test helper to create DJ object with config
const createTestDJ = (): DJ => ({
  config: mockConfig,
});

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
});
