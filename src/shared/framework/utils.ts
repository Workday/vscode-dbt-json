import {
  assertExhaustive,
  mergeDeep,
  orderKeys,
  removeEmpty,
  textToStartCase,
  yamlStringify,
} from '@shared';
import type { Api } from '@shared/api/types';
import {
  DbtModelConfig,
  DbtModelProperties,
  DbtModelPropertiesColumn,
  DbtProject,
  DbtProjectManifest,
  DbtProjectManifestNode,
  DbtProjectManifestSource,
  DbtProjectManifestSourceColumn,
  DbtProjectManifestSourceColumns,
  DbtResourceType,
  DbtSourceProperties,
  DbtSourceTableColumn,
} from '@shared/dbt/types';
import { getDbtModelId } from '@shared/dbt/utils';
import {
  FRAMEWORK_AGGS,
  FRAMEWORK_PARTITIONS,
} from '@shared/framework/constants';
import {
  FrameworkColumn,
  FrameworkColumnAgg,
  FrameworkDims,
  FrameworkInterval,
  FrameworkModel,
  FrameworkPartitionName,
  FrameworkSelected,
  FrameworkSource,
  FrameworkSourceMeta,
} from '@shared/framework/types';
import { LightdashDimension, LightdashMetrics } from '@shared/lightdash/types';
import {
  lightdashBuildMetrics,
  lightdashConvertDimensionType,
} from '@shared/lightdash/utils';
import {
  SchemaModelTypeIntJoinColumn,
  SchemaModelTypeIntJoinModels,
  SchemaModelTypeIntLookbackModel,
  SchemaModelTypeIntRollupModel,
  SchemaModelTypeIntSelectModel,
  SchemaModelTypeIntUnionModels,
  SchemaModelTypeMartJoinModels,
  SchemaModelTypeMartSelectModel,
  SchemaModelTypeStgSelectModel,
  SchemaModelTypeStgSelectSource,
  SchemaModelTypeStgUnionSources,
} from '@shared/schema/types/model.schema';
import { sqlCleanLine, sqlFormat } from '@shared/sql/utils';
import * as _ from 'lodash';
import * as path from 'path';

export function frameworkProcessSelected({
  existingColumns,
  datetimeInterval,
  fromColumn,
  modelJson,
  modelMetrics,
  prefix,
  project,
  selected,
}: {
  existingColumns: FrameworkColumn[];
  datetimeInterval: FrameworkInterval | null;
  fromColumn: FrameworkColumn | null;
  modelJson: FrameworkModel;
  modelMetrics: LightdashMetrics;
  prefix: string | null;
  project: DbtProject;
  selected: FrameworkSelected;
}): {
  columns: FrameworkColumn[];
  modelMetrics: LightdashMetrics;
} {
  const modelId = frameworkGetModelId({ modelJson, project });
  const newColumns: FrameworkColumn[] = [];

  // This function is how we prevent duplicate column names from being added
  function shouldAdd(n: FrameworkColumn) {
    return ![...existingColumns, ...newColumns].find(
      (e) =>
        // TODO: Avoid needing to reprocess the name each loop through
        frameworkColumnName({ column: e, modelJson }) ===
        frameworkColumnName({ column: n, modelJson }),
    );
  }

  // These are already processed outside of this function
  if (typeof selected === 'string') {
    const newColumn: FrameworkColumn = {
      name: selected,
      meta: { type: 'dim' },
    };
    if (shouldAdd(newColumn)) {
      newColumns.push(newColumn);
    }
  } else if (!('name' in selected)) {
    // If we don't have a name, we can't process this
  } else {
    // Building the new column properties that will override the inherited ones
    const selectedColumn: FrameworkColumn = {
      name: selected.name,
      meta: { type: selected.type || 'dim' },
    };
    if ('data_type' in selected && selected.data_type) {
      selectedColumn.data_type = selected.data_type;
    }
    if ('description' in selected && selected.description) {
      selectedColumn.description = selected.description;
    }
    if ('exclude_from_group_by' in selected && selected.exclude_from_group_by) {
      selectedColumn.meta.exclude_from_group_by =
        selected.exclude_from_group_by;
    }
    if ('expr' in selected && selected.expr) {
      selectedColumn.meta.expr = selected.expr;
    }
    if ('interval' in selected && selected.interval) {
      selectedColumn.meta.interval = selected.interval;
    }
    // We'll handle the lightdash metrics separately
    if ('lightdash' in selected && selected.lightdash?.dimension) {
      selectedColumn.meta.dimension = selected.lightdash.dimension;
    }
    if ('override_suffix_agg' in selected && selected.override_suffix_agg) {
      selectedColumn.meta.override_suffix_agg = !!selected.override_suffix_agg;
    }
    if ('data_tests' in selected && selected.data_tests) {
      selectedColumn.data_tests = selected.data_tests;
    }
    if (prefix) {
      selectedColumn.meta.prefix = prefix;
    }

    // In this scenario, we're creating a new column for each agg
    if ('aggs' in selected && selected.aggs) {
      let skipCustom = false;
      for (const agg of selected.aggs) {
        const aggColumn: FrameworkColumn = mergeDeep(selectedColumn, {
          data_type: 'number',
          meta: { agg }, // This agg key will append to the column name
        });
        const metrics = lightdashBuildMetrics({
          column: aggColumn,
          modelJson,
          selected,
          skipCustom,
        });
        modelMetrics = { ...modelMetrics, ...metrics.model };
        const fromColumnWithAgg: FrameworkColumn = mergeDeep(
          fromColumn,
          aggColumn,
        );
        const newColumn: FrameworkColumn = mergeDeep(fromColumnWithAgg, {
          meta: { metrics: metrics.column },
        });
        if (_.isEmpty(newColumn.meta.metrics)) {
          delete newColumn.meta.metrics;
        }
        if (shouldAdd(newColumn)) {
          // Only adding if the column doesn't already exist
          newColumns.push(newColumn);
          skipCustom = true; // Only attach custom metrics to the first column created from these aggs
        }
      }
    } else {
      // Special handing for datetime columns with interval specified
      if (
        selected.name === 'datetime' &&
        'interval' in selected &&
        selected.interval
      ) {
        const name = selected.name;
        datetimeInterval = selected.interval;
        selectedColumn.meta.interval = selected.interval;

        const timeIntervals: LightdashDimension['time_intervals'] = ['YEAR'];
        switch (datetimeInterval) {
          case 'hour': {
            timeIntervals.push('DAY');
            timeIntervals.push('DAY_OF_WEEK_NAME');
            timeIntervals.push('HOUR');
            timeIntervals.push('MONTH');
            timeIntervals.push('WEEK');
            break;
          }
          case 'day': {
            timeIntervals.push('DAY');
            timeIntervals.push('DAY_OF_WEEK_NAME');
            timeIntervals.push('MONTH');
            timeIntervals.push('WEEK');
            break;
          }
          case 'month': {
            timeIntervals.push('MONTH');
            break;
          }
        }
        timeIntervals.sort();
        selectedColumn.meta.dimension = {
          label: `Datetime`,
          time_intervals: timeIntervals,
          ...selected.lightdash?.dimension,
        };

        const fromInterval =
          fromColumn &&
          'interval' in fromColumn?.meta &&
          fromColumn?.meta.interval;
        const shouldTrunc =
          datetimeInterval && datetimeInterval !== fromInterval;

        if (shouldTrunc) {
          const prefixedName = prefix ? `${prefix}.${name}` : name;
          selectedColumn.meta.expr = `date_trunc('${datetimeInterval}', ${prefixedName})`;
        }
      } else {
        if ('agg' in selected && selected.agg) {
          selectedColumn.data_type = 'number';
          selectedColumn.meta.agg = selected.agg;
        }

        const metrics = lightdashBuildMetrics({
          column: selectedColumn,
          modelJson,
          selected,
        });
        selectedColumn.meta.metrics = metrics.column;
        modelMetrics = { ...modelMetrics, ...metrics.model };
      }
      let newColumn: FrameworkColumn = mergeDeep(fromColumn, {
        ...selectedColumn,
      });
      if (modelId && !newColumn?.meta?.origin?.id) {
        // If selection isn't inheriting an existing column, we'll establish the current model id as the origin
        newColumn = mergeDeep(newColumn, {
          meta: { origin: { id: modelId } },
        });
      }
      if (_.isEmpty(newColumn.meta.metrics)) {
        delete newColumn.meta.metrics;
      }
      if (shouldAdd(newColumn)) {
        newColumns.push(newColumn);
      }
    }
  }

  return {
    columns: newColumns,
    modelMetrics,
  };
}

export function frameworkBuildColumns({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): {
  columns: FrameworkColumn[];
  datetimeInterval: FrameworkInterval | null;
  dimensions: FrameworkColumn[];
  facts: FrameworkColumn[];
  modelMetrics: LightdashMetrics;
} {
  let columns: FrameworkColumn[] = [];

  const modelHasAgg = frameworkModelHasAgg({ modelJson });
  const modelLayer = frameworkGetModelLayer(modelJson);

  // Certain metrics can rise to the model level when declared on columns (e.g. avg)
  // We start with the inherited metrics as a baseline
  let modelMetrics: LightdashMetrics = frameworkInheritModels({
    modelJson,
    project,
  }).metrics;

  let datetimeInterval: 'hour' | 'day' | 'month' | 'year' | null = null;

  // HANDLE SELECTED COLUMNS
  if ('rollup' in modelJson.from && modelJson.from.rollup) {
    // HANDLE ROLLUP
    datetimeInterval = modelJson.from.rollup.interval;
    const from = frameworkGetNodeColumns({
      exclude: [
        ...columns,
        ...frameworkGetRollupInputs({
          ...modelJson.from,
          modelJson,
          project,
        }).exclude,
      ],
      from: modelJson.from,
      project,
    });
    columns.push(
      ...frameworkGetRollupInputs({
        ...modelJson.from,
        modelJson,
        project,
      }).columns,
    );
    columns.push(...from.dimensions);
    for (const f of from.facts) {
      if (frameworkSuffixAgg(f.name)) {
        // For rollups, we're only going to include the previously aggregated columns
        columns.push(f);
      }
    }
  } else if (
    'union' in modelJson.from &&
    modelJson.from.union &&
    !('select' in modelJson && modelJson.select)
  ) {
    // HANDLE UNION WITHOUT SELECT
    const from = frameworkGetNodeColumns({
      exclude: columns,
      from: {
        ...('model' in modelJson.from
          ? { model: modelJson.from.model }
          : { source: modelJson.from.source }),
      },
      project,
    });
    columns.push(...from.columns);
  } else if ('select' in modelJson && modelJson.select) {
    // HANDLE SELECT
    for (const selected of modelJson.select || []) {
      const fromModel =
        typeof selected === 'object' && 'model' in selected && selected.model
          ? selected.model
          : 'from' in modelJson &&
              'model' in modelJson.from &&
              modelJson.from.model
            ? modelJson.from.model
            : null;
      const fromSource =
        typeof selected === 'object' && 'source' in selected && selected.source
          ? selected.source
          : 'from' in modelJson &&
              'source' in modelJson.from &&
              modelJson.from.source
            ? modelJson.from.source
            : null;
      const exclude =
        typeof selected === 'object' &&
        'exclude' in selected &&
        selected.exclude
          ? selected.exclude
          : [];
      const include =
        typeof selected === 'object' &&
        'include' in selected &&
        selected.include
          ? selected.include
          : [];
      const from = fromModel
        ? frameworkGetNodeColumns({
            exclude: [...columns, ...exclude],
            from: { model: fromModel },
            include,
            project,
          })
        : fromSource
          ? frameworkGetNodeColumns({
              exclude: [...columns, ...exclude],
              from: { source: fromSource },
              include,
              project,
            })
          : null;
      const fromColumnName = !(
        typeof selected === 'object' &&
        'expr' in selected &&
        selected.expr
      ) // If expr is provided, we aren't inheriting anything
        ? typeof selected === 'string'
          ? selected
          : 'name' in selected && selected.name
            ? selected.name
            : null
        : null;
      const fromColumn =
        (fromColumnName &&
          from?.columns.find((c) => c.name === fromColumnName)) ||
        null;
      const overridePrefix =
        typeof selected === 'object' &&
        'override_prefix' in selected &&
        selected.override_prefix;
      const prefix =
        ('join' in modelJson.from &&
          modelJson.type !== 'int_join_column' &&
          modelJson.from.join?.length &&
          (overridePrefix || fromModel)) ||
        null;
      if (typeof selected === 'string') {
        columns.push(
          mergeDeep(fromColumn, {
            name: selected,
            meta: { type: 'dim' },
          }),
        );
      } else {
        switch (selected.type) {
          case 'all_from_model': {
            if (!from) continue;
            columns.push(
              ...frameworkInheritColumns(from.columns, {
                meta: { ...(prefix && { prefix }) },
              }),
            );
            break;
          }
          case 'all_from_source': {
            if (!from) continue;
            columns.push(
              ...frameworkInheritColumns(from.columns, {
                meta: { ...(prefix && { prefix }) },
              }),
            );
            break;
          }
          case 'dims_from_model': {
            if (!from) continue;
            columns.push(
              ...frameworkInheritColumns(from.dimensions, {
                meta: { ...(prefix && { prefix }) },
              }),
            );
            break;
          }
          case 'fcts_from_model': {
            if (!from) continue;
            columns.push(
              ...frameworkInheritColumns(from.facts, {
                meta: { ...(prefix && { prefix }) },
              }),
            );
            break;
          }
          // If single fact or dim, just add prefix
          case 'fct':
          case 'dim':
          default: {
            const processed = frameworkProcessSelected({
              datetimeInterval,
              existingColumns: columns,
              fromColumn,
              modelJson,
              modelMetrics,
              prefix,
              project,
              selected: selected as FrameworkSelected,
            });
            columns.push(...processed.columns);
            modelMetrics = { ...modelMetrics, ...processed.modelMetrics };
            break;
          }
        }
      }
    }
  }

  let baseModel: string | null = null;
  let basePrefix: string | null = null;
  let baseSource: string | null = null;

  // HANDLE ADDITIONAL PARTITION COLUMNS
  if (modelLayer === 'stg') {
    if ('from' in modelJson && 'model' in modelJson.from) {
      baseModel = modelJson.from.model;
    }
    if ('from' in modelJson && 'source' in modelJson.from) {
      baseSource = modelJson.from.source;
    }
  } else if (
    'from' in modelJson &&
    'model' in modelJson.from &&
    modelJson.from.model
  ) {
    baseModel = modelJson.from.model;
    if ('join' in modelJson.from && modelJson.type !== 'int_join_column') {
      basePrefix = baseModel;
    }
  }

  if (baseSource) {
    const sourceDateColumns = frameworkBuildSourceDateColumns({
      columns,
      project,
      source: baseSource,
    });
    columns.push(...sourceDateColumns);
  } else if (baseModel) {
    const exclude: FrameworkDims[] = [];
    if (!datetimeInterval) {
      // If we didn't select a datetime interval column, we'll inherit it from the base model
      const datetimeIntervalColumn = columns.find((c) =>
        'interval' in c.meta && c.name === 'datetime'
          ? c.meta.interval || undefined
          : null,
      );
      datetimeInterval =
        (datetimeIntervalColumn &&
          'interval' in datetimeIntervalColumn.meta &&
          datetimeIntervalColumn.meta.interval) ||
        null;
    }
    if (datetimeInterval) {
      exclude.push(
        ...frameworkGetRollupInputs({
          model: baseModel,
          modelJson,
          project,
          rollup: { interval: datetimeInterval },
        }).exclude,
      );
    }
    switch (datetimeInterval) {
      case 'day': {
        exclude.push('portal_partition_hourly');
        break;
      }
      case 'month': {
        exclude.push('portal_partition_daily');
        exclude.push('portal_partition_hourly');
        break;
      }
      case 'year': {
        exclude.push('portal_partition_monthly');
        exclude.push('portal_partition_hourly');
        exclude.push('portal_partition_daily');
        break;
      }
    }

    // By default, we include the datetime and partition columns when selecting from models, unless we're doing a lookback
    if (!('lookback' in modelJson.from && modelJson.from.lookback)) {
      const fromFrameworkDims = frameworkGetNodeColumns({
        exclude: [...columns, ...exclude], // Excluding if these were already added
        from: { model: baseModel },
        include: [
          'datetime',
          'portal_partition_monthly',
          'portal_partition_daily',
          'portal_partition_hourly',
        ],
        project,
      });
      const frameworkDims = frameworkInheritColumns(fromFrameworkDims.columns, {
        meta: { ...(basePrefix && { prefix: basePrefix }) },
      });
      columns.push(...frameworkDims);
    }

    // By default, we include portal_source_count column
    const fromFrameworkCounts = frameworkGetNodeColumns({
      exclude: [...columns, ...exclude], // Excluding if these were already added
      from: { model: baseModel },
      include: ['portal_source_count'],
      project,
    });
    const frameworkCounts = frameworkInheritColumns(
      fromFrameworkCounts.columns,
      {
        meta: {
          ...(basePrefix && { prefix: basePrefix }),
          ...(modelHasAgg && { agg: 'count' }),
        },
      },
    );
    columns.push(...frameworkCounts);
  }

  // Sort columns alphabetically
  columns = _.sortBy(columns, ['name']);

  // Remove portal_source_count if it's excluded
  if (!!modelJson.exclude_portal_source_count) {
    columns = columns.filter((c) => c.name !== 'portal_source_count');
  }

  // Pull out partition columns, and re-add them in order at the end (if not excluded)
  const partitionColumnNames = frameworkGetPartitionColumnNames({
    modelJson,
    project,
  });
  const partitionColumns: FrameworkColumn[] = columns.filter((c) =>
    partitionColumnNames.includes(c.name),
  );
  columns = columns.filter((c) => !partitionColumnNames.includes(c.name));

  // Unless we have explicity excluded them, we'll add back the partition columns
  if (!modelJson.exclude_portal_partition_columns) {
    for (const name of partitionColumnNames) {
      const partitionColumn = partitionColumns.find((c) => c.name === name);
      if (partitionColumn) {
        columns.push(partitionColumn);
      }
    }
  }

  if ('lookback' in modelJson.from && modelJson.from.lookback) {
    // Special handling for lookback models

    // Exclude the datetime and portal_source_count columns
    columns = columns.filter(
      (c) => !['datetime', 'portal_source_count'].includes(c.name),
    );
    // If portal_partition_daily exists, replace in current spot, otherwise add to end
    const portalPartitionDailyIndex = columns.findIndex(
      (c) => c.name === 'portal_partition_daily',
    );
    const portalPartitionDailyColumn: FrameworkColumn = {
      name: 'portal_partition_daily',
      data_type: 'date',
      meta: { expr: '_ext_event_date', type: 'dim' },
    };
    if (portalPartitionDailyIndex >= 0) {
      columns = [
        ...columns.slice(0, portalPartitionDailyIndex),
        portalPartitionDailyColumn,
        ...columns.slice(portalPartitionDailyIndex + 1),
      ];
    } else {
      columns.push(portalPartitionDailyColumn);
    }
  }

  return {
    columns,
    datetimeInterval,
    dimensions: columns.filter((c) => c.meta.type === 'dim'),
    facts: columns.filter((c) => c.meta.type === 'fct'),
    modelMetrics,
  };
}

export function frameworkBuildFilters({
  datetimeInterval,
  from,
  modelJson,
  prefix,
  project,
}: {
  datetimeInterval: 'hour' | 'day' | 'month' | 'year' | null;
  from: { model: string } | { source: string };
  modelJson: FrameworkModel;
  prefix?: string;
  project: DbtProject;
}): string[] {
  const sqlLines: string[] = [];
  // If exclude_date_filter set to true, we return no framework date filters
  if ('exclude_date_filter' in modelJson && modelJson.exclude_date_filter) {
    return sqlLines;
  }
  //
  const modelLayer = frameworkGetModelLayer(modelJson);

  // Model level inputs
  const includeFullMonth = !!(
    'include_full_month' in modelJson && modelJson.include_full_month
  );

  if (
    'model' in from &&
    modelLayer !== 'mart' // We don't add date filters in mart models
  ) {
    const partitions = frameworkGetModelPartitions({
      datetimeInterval,
      ...from,
      modelJson,
      project,
    });
    for (const p of partitions) {
      const expr = `${prefix ? `${prefix}.` : ''}${p.name}`;
      const args: string[] = [`"${expr}"`, `data_type="date"`];
      switch (p.name) {
        case 'portal_partition_monthly': {
          args.push(`interval="month"`);
          sqlLines.push(`{{ _ext_event_date_filter(${args.join(', ')}) }}`);
          break;
        }
        case 'portal_partition_daily': {
          if (
            !(
              'exclude_daily_filter' in modelJson &&
              modelJson.exclude_daily_filter
            ) &&
            !includeFullMonth
          ) {
            sqlLines.push(`{{ _ext_event_date_filter(${args.join(', ')}) }}`);
          }
          break;
        }
      }
    }
  } else if ('source' in from) {
    const sourceMeta = frameworkGetSourceMeta({ project, ...from });
    const sourceId = frameworkGetSourceId({ project, ...from });

    const tableFunction = sourceMeta?.table_function;
    const dialect = tableFunction?.dialect;

    // Handle partition date filters
    const partitionDateCompileDates =
      sourceMeta?.partition_date?.compile_dates !== false;
    const partitionDateDataType = sourceMeta?.partition_date?.data_type;
    const partitionDateExpr = sourceMeta?.partition_date?.expr;
    const partitionDateInterval = sourceMeta?.partition_date?.interval;
    const partitionDateUseEventDates =
      sourceMeta?.partition_date?.use_event_dates ||
      ('use_event_dates_for_partition_dates' in modelJson &&
        modelJson.use_event_dates_for_partition_dates);
    const partitionDateUseRange = sourceMeta?.partition_date?.use_range;

    // Compile the dates any time we're using a table function

    if (partitionDateExpr) {
      const args: string[] = [`"${partitionDateExpr.replaceAll('"', "'")}"`];
      if (partitionDateCompileDates) {
        args.push('compile_dates=true');
      }
      if (partitionDateDataType) {
        args.push(`data_type="${partitionDateDataType}"`);
      }
      if (dialect) {
        args.push(`dialect="${dialect}"`);
      }
      if (includeFullMonth) {
        args.push('include="month"');
      }
      if (partitionDateInterval) {
        args.push(`interval="${partitionDateInterval}"`);
      }
      args.push(`source_id="${sourceId}"`);
      if (partitionDateUseEventDates) {
        args.push('use_event_dates=true');
      }
      if (partitionDateUseRange) {
        args.push('use_range=true');
      }
      sqlLines.push(`{{ _ext_partition_date_filter(${args.join(', ')}) }}`);
    }

    // Handle additional partition filters
    const partitions = sourceMeta?.partitions;
    for (const partition of partitions || []) {
      switch (partition.type) {
        case 'event_dates': {
          const args: string[] = [
            `"${partition.expr.replaceAll('"', "'")}"`,
            `data_type="${partition.data_type || 'date'}"`,
          ];
          if (includeFullMonth) {
            args.push('include="month"');
          }
          if (partition.interval) {
            args.push(`interval="${partition.interval}"`);
          }
          if (partition.use_range) {
            args.push(`use_range=true`);
          }
          sqlLines.push(`{{ _ext_event_date_filter(${args.join(', ')}) }}`);
          break;
        }
        case 'eq':
          sqlLines.push(`${partition.expr} = '${partition.value}'`);
          break;
        case 'gt':
          sqlLines.push(`${partition.expr} > '${partition.value}'`);
          break;
        case 'gte':
          sqlLines.push(`${partition.expr} >= '${partition.value}'`);
          break;
        case 'lt':
          sqlLines.push(`${partition.expr} < '${partition.value}'`);
          break;
        case 'lte':
          sqlLines.push(`${partition.expr} <= '${partition.value}'`);
          break;
        case 'neq':
          sqlLines.push(`${partition.expr} <> '${partition.value}'`);
          break;
        default:
          assertExhaustive(partition);
      }
    }

    // Handle event datetime filter
    // const eventDatetimeDataType = sourceMeta?.event_datetime?.data_type;
    const eventDatetimeExpr = sourceMeta?.event_datetime?.expr;
    const eventDatetimeInterval = sourceMeta?.event_datetime?.interval;
    const eventDatetimeUseRange = !!sourceMeta?.event_datetime?.use_range;
    if (eventDatetimeExpr) {
      const args: string[] = [
        `"${eventDatetimeExpr}"`,
        // `data_type="${eventDatetimeDataType || 'timestamp'}"`,
      ];
      if (dialect) {
        args.push(`dialect="${dialect}"`);
      }
      if (includeFullMonth) {
        args.push('include="month"');
      }
      if (eventDatetimeInterval) {
        args.push(`interval="${eventDatetimeInterval}"`);
      }
      if (eventDatetimeUseRange) {
        args.push(`use_range=true`);
      }
      sqlLines.push(`{{ _ext_event_datetime_filter(${args.join(', ')}) }}`);
    }
    const whereExpr = sourceMeta?.where?.expr;
    if (whereExpr) {
      sqlLines.push(whereExpr);
    }
  }
  return sqlLines;
}

export function frameworkBuildModelTags({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): {
  local: string[];
  model: string[];
} {
  const modelLayer = frameworkGetModelLayer(modelJson);
  const tagsExclude = [];
  const tagsDefault = ['json'];
  const tagsInherited: string[] = [];
  const tagsLocal: string[] = [];
  const tagsNew: string[] = [];
  const parentNodes = frameworkGetParentNodes({ modelJson, project });

  switch (modelLayer) {
    case 'stg': {
      tagsDefault.push('staging');
      tagsExclude.push('intermediate');
      tagsExclude.push('lightdash');
      tagsExclude.push('lightdash-explore');
      tagsExclude.push('mart');
      break;
    }
    case 'int': {
      tagsDefault.push('intermediate');
      tagsExclude.push('lightdash');
      tagsExclude.push('lightdash-explore');
      tagsExclude.push('staging');
      tagsExclude.push('mart');
      break;
    }
    case 'mart': {
      tagsDefault.push('mart');
      tagsExclude.push('staging');
      tagsExclude.push('intermediate');
      break;
    }
  }

  for (const node of parentNodes) {
    const nodeTags = node?.tags || [];
    const nodeLocalTags = node?.meta?.local_tags || [];
    tagsInherited.push(
      ..._.union(
        tagsInherited,
        _.difference(nodeTags, nodeLocalTags, tagsExclude),
      ),
    );
  }

  if ('tags' in modelJson) {
    for (const t of modelJson.tags || []) {
      if (typeof t === 'string') {
        tagsNew.push(t);
      } else {
        switch (t.type) {
          case 'exclude': {
            tagsExclude.push(t.tag);
            break;
          }
          case 'local': {
            tagsLocal.push(t.tag);
            tagsNew.push(t.tag);
            break;
          }
          default: {
            tagsNew.push(t.tag);
            break;
          }
        }
      }
    }
  }

  // Combine all the tags for the model
  let tagsModel = _.union(tagsDefault, tagsInherited, tagsNew);
  // Remove any tags that are excluded
  tagsModel = _.difference(tagsModel, tagsExclude);

  tagsLocal.sort();
  tagsModel.sort();

  return {
    local: tagsLocal,
    model: tagsModel,
  };
}

export function frameworkBuildSourceDateColumns({
  columns,
  project,
  source,
}: {
  columns: FrameworkColumn[];
  project: DbtProject;
  source: string;
}) {
  const sourceDateColumns: FrameworkColumn[] = [];
  const sourceMeta = frameworkGetSourceMeta({
    project,
    source,
  });

  const eventDatetimeExpr = sourceMeta?.event_datetime?.expr;
  if (eventDatetimeExpr) {
    if (!columns.find((c) => c.name === 'datetime')) {
      sourceDateColumns.push({
        name: 'datetime',
        data_type: 'timestamp(6)',
        description: 'Event Datetime Column',
        meta: {
          type: 'dim',
          expr: `cast(${eventDatetimeExpr} as timestamp(6))`,
          dimension: { label: 'Datetime', type: 'timestamp' },
        },
      });
    }
    if (!columns.find((c) => c.name === 'portal_partition_daily')) {
      sourceDateColumns.push({
        name: 'portal_partition_daily',
        data_type: 'date',
        description: 'Daily Partition Column',
        meta: {
          type: 'dim',
          expr: `date_trunc('day', cast(${eventDatetimeExpr} as date))`,
          dimension: { label: 'Portal Partition Daily' },
        },
      });
    }
    if (!columns.find((c) => c.name === 'portal_partition_hourly')) {
      sourceDateColumns.push({
        name: 'portal_partition_hourly',
        data_type: 'timestamp(6)',
        description: 'Hourly Partition Column',
        meta: {
          type: 'dim',
          expr: `date_trunc('hour', cast(${eventDatetimeExpr} as timestamp(6)))`,
          dimension: { label: 'Portal Partition Hourly' },
        },
      });
    }
    if (!columns.find((c) => c.name === 'portal_partition_monthly')) {
      sourceDateColumns.push({
        name: 'portal_partition_monthly',
        data_type: 'date',
        description: 'Monthly Partition Column',
        meta: {
          type: 'dim',
          expr: `date_trunc('month', cast(${eventDatetimeExpr} as date))`,
          dimension: { label: 'Portal Partition Monthly' },
        },
      });
    }
  }

  const portalSourceCount = sourceMeta?.portal_source_count;
  if (!portalSourceCount?.exclude) {
    sourceDateColumns.push({
      name: 'portal_source_count',
      data_type: 'bigint',
      meta: {
        type: 'fct',
        expr: '1',
        dimension: { label: 'Portal Source Count', hidden: true },
        metrics: {
          metric_portal_source_count: {
            type: 'sum',
            label: portalSourceCount?.metric_label || 'Portal Source Count',
          },
        },
      },
    });
  }

  return sourceDateColumns;
}

export function frameworkColumnSelect(column: FrameworkColumn) {
  if (column.meta.expr) {
    return `${column.meta.expr} as ${column.name}`;
  }
  if (column.meta.prefix) {
    return `${column.meta.prefix}.${column.name} as ${column.name}`;
  }
  return sqlCleanLine(column.name);
}

export function frameworkColumnName({
  column,
  modelJson,
}: {
  column: FrameworkColumn;
  modelJson: FrameworkModel;
}) {
  const suffixAgg = frameworkSuffixAgg(column.name) || null;
  const metaAgg = ('agg' in column.meta && column.meta.agg) || null;
  const overrideSuffixAgg = !!(
    'override_suffix_agg' in column.meta && column.meta.override_suffix_agg
  );
  const rollupAgg =
    ('rollup' in modelJson.from && column.meta.type === 'fct' && suffixAgg) ||
    null;
  const newAgg = metaAgg || rollupAgg;
  return newAgg && (newAgg !== suffixAgg || overrideSuffixAgg)
    ? `${column.name}_${newAgg}`
    : column.name;
}

export function frameworkGetModelMeta({
  project,
  model,
}: {
  project: DbtProject;
  model: string;
}): Partial<DbtProjectManifestNode['meta']> | null {
  const modelId = `model.${project.name}.${model}`;
  return project.manifest.nodes?.[modelId]?.meta || null;
}

export function frameworkGetPathJson({
  pathResource,
  type,
}: {
  pathResource: string;
  type: DbtResourceType;
}): string {
  return pathResource.replace(/\.(?:csv|sql|yaml|yml)$/, `.${type}.json`);
}

/**
 * Gets the meta from the base parent node for a model (first model or source selected), whether it is a model or source
 */
export function frameworkGetParentMeta({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): Partial<
  | DbtProjectManifestNode['meta']
  | DbtProjectManifestSource['meta']
  | DbtProjectManifestSource['source_meta']
> | null {
  if ('source' in modelJson.from) {
    return frameworkGetSourceMeta({ project, ...modelJson.from });
  } else {
    return frameworkGetModelMeta({ project, ...modelJson.from });
  }
}

/**
 * Gets the base parent node for a model (first model or source selected), whether it is a model or source
 */
export function frameworkGetParentNode({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): Partial<DbtProjectManifestNode | DbtProjectManifestSource> | null {
  if ('from' in modelJson) {
    const baseNode = frameworkGetNode({ project, ...modelJson.from });
    return baseNode;
  }
  return null;
}

/**
 * Gets all parent nodes for a model, whether they are models or sources
 */
export function frameworkGetParentNodes({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): Partial<DbtProjectManifestNode | DbtProjectManifestSource>[] {
  const parentNodes: Partial<
    DbtProjectManifestNode | DbtProjectManifestSource
  >[] = [];
  if (!('from' in modelJson && modelJson.from)) return parentNodes;
  const baseNode = frameworkGetNode({ project, ...modelJson.from });
  if (baseNode) parentNodes.push(baseNode);

  if ('join' in modelJson.from && modelJson.type !== 'int_join_column') {
    for (const join of modelJson.from.join || []) {
      const joinNode = frameworkGetNode({ project, ...join });
      if (joinNode) parentNodes.push(joinNode);
    }
  }

  if ('union' in modelJson.from) {
    if ('models' in modelJson.from.union) {
      for (const model of modelJson.from.union.models || []) {
        const unionNode = frameworkGetNode({ project, model });
        if (unionNode) parentNodes.push(unionNode);
      }
    } else if ('sources' in modelJson.from.union) {
      for (const source of modelJson.from.union.sources || []) {
        const unionNode = frameworkGetNode({ project, source });
        if (unionNode) parentNodes.push(unionNode);
      }
    }
  }

  return parentNodes;
}

export function frameworkGetPartitionColumnNames({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): string[] {
  const parentMeta = frameworkGetParentMeta({ modelJson, project });
  const partitionColumnsParent = parentMeta?.portal_partition_columns;
  const partitionColumnsModel =
    ('materialization' in modelJson &&
      modelJson.materialization &&
      'partitions' in modelJson.materialization &&
      modelJson.materialization?.partitions) ||
    ('partitioned_by' in modelJson && modelJson.partitioned_by);
  const partitionColumnsDefault: FrameworkPartitionName[] = [
    'portal_partition_monthly',
    'portal_partition_daily',
    'portal_partition_hourly',
  ];
  // Set in order of priority
  const partitionColumnNames =
    partitionColumnsModel || partitionColumnsParent || partitionColumnsDefault;
  return partitionColumnNames;
}

export function frameworkGetMacro({
  macro,
  project,
}: {
  macro: string;
  project: DbtProject;
}) {
  return project.manifest.macros[`macro.${project.name}.${macro}`] || null;
}

export function frameworkGetModelChildMap({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): string[] {
  const modelId = frameworkGetModelId({ modelJson, project });
  if (!modelId) return [];
  return project.manifest.child_map[modelId] || [];
}

export function frameworkGetModelId({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): string | null {
  const modelName = frameworkGetModelName(modelJson);
  const modelId = getDbtModelId({ modelName, projectName: project.name });
  return modelId;
}

export function frameworkGetModelPartitions({
  datetimeInterval,
  project,
  model,
  modelJson,
}: {
  datetimeInterval: 'hour' | 'day' | 'month' | 'year' | null;
  project: DbtProject;
  model: string;
  modelJson: FrameworkModel;
}): FrameworkColumn[] {
  const exclude: FrameworkDims[] = [];
  if (datetimeInterval) {
    exclude.push(
      ...frameworkGetRollupInputs({
        model,
        modelJson,
        project,
        rollup: { interval: datetimeInterval },
      }).exclude,
    );
  }
  const from = frameworkGetNodeColumns({
    exclude,
    from: { model },
    include: [
      'portal_partition_monthly',
      'portal_partition_daily',
      'portal_partition_hourly',
    ],
    project,
  });
  return from.columns;
}

export function frameworkGetModelName(
  modelJson: Pick<FrameworkModel, 'group' | 'name' | 'topic' | 'type'>,
): string {
  const modelLayer = frameworkGetModelLayer(modelJson);
  return modelLayer && modelJson.group && modelJson.topic && modelJson.name
    ? `${modelLayer}__${modelJson.group}__${modelJson.topic.replaceAll('/', '__')}__${modelJson.name}`
    : '';
}

export function frameworkGetModelPrefix({
  project,
  modelJson,
}: {
  project: DbtProject;
  modelJson: Pick<FrameworkModel, 'group' | 'name' | 'topic' | 'type'>;
}): string | null {
  const modelLayer = frameworkGetModelLayer(modelJson);
  const modelName = frameworkGetModelName(modelJson);
  if (!modelName) return null;
  let modelPath = path.join(
    project.pathSystem,
    project.modelPaths[0] || 'models',
  );
  switch (modelLayer) {
    case 'int': {
      modelPath = path.join(modelPath, 'intermediate');
      break;
    }
    case 'mart': {
      modelPath = path.join(modelPath, 'marts');
      break;
    }
    case 'stg': {
      modelPath = path.join(modelPath, 'staging');
      break;
    }
  }
  modelPath = path.join(modelPath, modelJson.group);
  if (modelJson.topic) {
    modelPath = path.join(modelPath, modelJson.topic);
  }
  return path.join(modelPath, modelName);
}

export function frameworkGetModelLayer(
  modelJson: Pick<FrameworkModel, 'type'>,
): 'stg' | 'int' | 'mart' {
  return modelJson.type.split('_')[0] as 'stg' | 'int' | 'mart';
}

export function frameworkGetNode(
  payload: {
    project: DbtProject;
  } & ({ model: string } | { source: string }),
): Partial<DbtProjectManifestNode | DbtProjectManifestSource> | null {
  const { project } = payload;
  if ('model' in payload) {
    // May be either model node or seed node
    return (
      project.manifest.nodes?.[`model.${project.name}.${payload.model}`] ||
      project.manifest.nodes?.[`seed.${project.name}.${payload.model}`] ||
      null
    );
  }
  if ('source' in payload) {
    const sourceId = frameworkGetSourceId({ ...payload });
    return project.manifest.sources?.[sourceId] || null;
  }
  return null;
}

export function frameworkGetNodeColumns({
  exclude,
  from,
  include,
  project,
}: {
  exclude?: (string | FrameworkColumn)[];
  from: { model: string; alias?: string } | { source: string };
  include?: (string | FrameworkColumn)[];
  project: DbtProject;
}): {
  columns: FrameworkColumn[];
  dimensions: FrameworkColumn[];
  facts: FrameworkColumn[];
} {
  exclude = exclude?.map((e) => (typeof e === 'string' ? e : e.name)) || [];
  include = include?.map((e) => (typeof e === 'string' ? e : e.name)) || [];

  const node = frameworkGetNode({ project, ...from });

  const columns: FrameworkColumn[] = [];

  for (const [name, c] of Object.entries(node?.columns || {})) {
    if (exclude.length && exclude.includes(name)) continue;
    if (include.length && !include.includes(name)) continue;
    if (!c?.meta?.type) {
      // Default columns to dims unless specified as fct
      c.meta = { ...c.meta, type: 'dim' };
    }
    // Pick off the meta properties we don't want to inherit
    const { agg, aggs, prefix, ...meta } = c.meta;
    columns.push({
      name,
      data_type: c.data_type,
      description: c.description,
      tags: c.tags || [],
      meta,
    });
  }

  return {
    columns,
    dimensions: columns.filter((c) => c.meta.type === 'dim'),
    facts: columns.filter((c) => c.meta.type === 'fct'),
  };
}

export function frameworkGetRollupInputs({
  model,
  modelJson,
  project,
  rollup,
}: {
  model: string;
  modelJson: FrameworkModel;
  project: DbtProject;
  rollup: { interval: 'hour' | 'day' | 'month' | 'year' };
}): {
  columns: FrameworkColumn[];
  exclude: FrameworkDims[];
  include: FrameworkPartitionName[];
} {
  const fromDatetime = frameworkGetNodeColumns({
    from: { model },
    include: ['datetime'],
    project,
  }).columns[0];
  const columns: FrameworkColumn[] = [];
  const exclude: ('datetime' | FrameworkPartitionName)[] = ['datetime'];
  const partitions: FrameworkPartitionName[] = [
    'portal_partition_monthly',
    'portal_partition_daily',
    'portal_partition_hourly',
  ];

  let newDatetimeExpr = '';

  switch (rollup.interval) {
    case 'hour':
      newDatetimeExpr = "date_trunc('hour', datetime)";
      break;
    case 'day':
      exclude.push('portal_partition_hourly');
      newDatetimeExpr = "date_trunc('day', datetime)";
      break;
    case 'month':
      exclude.push('portal_partition_daily');
      exclude.push('portal_partition_hourly');
      newDatetimeExpr = "date_trunc('month', datetime)";
      break;
    case 'year':
      exclude.push('portal_partition_daily');
      exclude.push('portal_partition_hourly');
      exclude.push('portal_partition_monthly');
      newDatetimeExpr = "date_trunc('year', datetime)";
      break;
  }
  if (newDatetimeExpr) {
    columns.push(
      ...frameworkProcessSelected({
        existingColumns: columns,
        datetimeInterval: rollup.interval,
        fromColumn: fromDatetime,
        modelJson,
        modelMetrics: {},
        prefix: null,
        project,
        selected: { name: 'datetime', interval: rollup.interval },
      }).columns,
    );
  }
  return {
    columns,
    exclude,
    include: partitions.filter((p) => !exclude.includes(p)),
  };
}

export function frameworkGetSource({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): Partial<DbtProjectManifestSource> | null {
  const sourceId = frameworkGetSourceId({ project, source });
  return project.manifest.sources?.[sourceId] || null;
}

export function frameworkGetSourceId({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): string {
  return `source.${project.name}.${_.chain(source).split('.').takeRight(2).join('.').value()}`;
}

/**
 * Returns a list of all source ids from a given source.json file
 */
export function frameworkGetSourceIds({
  project,
  sourceJson,
}: {
  project: DbtProject;
  sourceJson: FrameworkSource;
}): string[] {
  const sourceIds: string[] = [];
  for (const table of sourceJson?.tables || []) {
    const sourceId = frameworkMakeSourceId({
      ...sourceJson,
      table: table.name,
      project,
    });
    sourceIds.push(sourceId);
  }
  return sourceIds;
}

export function frameworkGetSourceMeta({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): Partial<
  DbtProjectManifestSource['source_meta'] & DbtProjectManifestSource['meta']
> {
  const sourceId = frameworkGetSourceId({ project, source });
  return mergeDeep(
    project.manifest.sources?.[sourceId]?.source_meta,
    project.manifest.sources?.[sourceId]?.meta,
  );
}

export function frameworkGetSourceProperties({
  project,
  source,
}: {
  project: DbtProject;
  source: string;
}): Partial<DbtProjectManifestSource> | null {
  const sourceId = frameworkGetSourceId({ project, source });
  return project.manifest.sources?.[sourceId] || null;
}

// Will only return the first source ref
export function frameworkGetSourceRef(modelJson: FrameworkModel) {
  return ('source' in modelJson.from && modelJson.from.source) || null;
}

export function frameworkGenerateModelOutput({
  project,
  modelJson,
}: {
  project: DbtProject;
  modelJson: FrameworkModel;
}): {
  config: DbtModelConfig;
  modelId: string;
  project: DbtProject;
  properties: DbtModelProperties;
  sql: string;
  yml: string;
} {
  const { columns, datetimeInterval } = frameworkBuildColumns({
    modelJson,
    project,
  });

  const modelName = frameworkGetModelName(modelJson);
  const projectName = project.name;
  const modelId = getDbtModelId({ modelName, projectName });

  const partitionColumnNames = frameworkGetPartitionColumnNames({
    modelJson,
    project,
  });

  const modelProperties = frameworkModelProperties({
    modelJson,
    project,
  });

  const modelFrom = frameworkModelFrom({
    datetimeInterval,
    modelJson,
    project,
  });
  const modelGroupBy = frameworkModelGroupBy({
    columns,
    modelJson,
  });
  const modelHaving = frameworkModelHaving({
    modelJson,
  });
  const modelSelect = frameworkModelSelect({
    columns,
    datetimeInterval,
    modelJson,
    project,
  });
  const modelWhere = frameworkModelWhere({
    datetimeInterval,
    modelJson,
    project,
  });

  const modelTableFunction = frameworkModelTableFunction({
    modelJson,
    project,
  });

  let sql = '';

  // Append comments
  const modelComments = [
    ...modelFrom.comments,
    ...modelGroupBy.comments,
    ...modelHaving.comments,
    ...modelSelect.comments,
    ...modelWhere.comments,
  ];
  if (modelComments.length) {
    sql += `-- ${modelComments.join('\n-- ')}\n\n`;
  }
  //

  // Append config block
  const modelConfig: DbtModelConfig = {};
  const modelLayer = frameworkGetModelLayer(modelJson);
  let materialized: DbtModelConfig['materialized'];
  switch (modelLayer) {
    case 'int': {
      // modelConfig.incremental_strategy = 'overwrite_existing_partitions';
      materialized =
        ('materialization' in modelJson &&
          modelJson.materialization &&
          modelJson.materialization.type) ||
        ('materialized' in modelJson && modelJson.materialized) ||
        'ephemeral';
      break;
    }
    case 'stg': {
      materialized =
        ('materialization' in modelJson &&
          modelJson.materialization &&
          modelJson.materialization.type) ||
        ('materialized' in modelJson && modelJson.materialized) ||
        'ephemeral';
      break;
    }
    case 'mart': {
      materialized = 'view';
      break;
    }
  }
  if (materialized) {
    modelConfig.materialized = materialized;
    switch (materialized) {
      case 'incremental': {
        const database =
          ('materialization' in modelJson &&
            modelJson.materialization &&
            'database' in modelJson.materialization &&
            modelJson.materialization.database) ||
          null;
        if (database) {
          modelConfig.database = database;
        }
        const strategy =
          ('materialization' in modelJson &&
            modelJson.materialization &&
            'strategy' in modelJson.materialization &&
            modelJson.materialization.strategy) ||
          ('incremental_strategy' in modelJson &&
            modelJson.incremental_strategy) ||
          null;
        switch (strategy?.type) {
          case 'delete+insert': {
            modelConfig.incremental_strategy = 'delete+insert';
            if (strategy.unique_key) {
              // If unique key is set, we use it for the delete+insert strategy
              modelConfig.unique_key = strategy.unique_key;
            } else {
              // Otherwise, default the appropriate partition column
              if (partitionColumnNames.includes('portal_partition_daily')) {
                modelConfig.unique_key = 'portal_partition_daily';
              } else if (
                partitionColumnNames.includes('portal_partition_monthly')
              ) {
                modelConfig.unique_key = 'portal_partition_monthly';
              } else {
                modelConfig.unique_key = partitionColumnNames;
              }
            }
            break;
          }
          case 'merge': {
            modelConfig.incremental_strategy = 'merge';
            modelConfig.unique_key = strategy.unique_key;
            if (strategy.merge_update_columns) {
              modelConfig.merge_update_columns = strategy.merge_update_columns;
            } else if (strategy.merge_exclude_columns) {
              modelConfig.merge_exclude_columns =
                strategy.merge_exclude_columns;
            }
            break;
          }
          default:
            modelConfig.incremental_strategy = 'overwrite_existing_partitions';
        }
        modelConfig.pre_hook =
          "set session iterative_optimizer_timeout='60m'; set session query_max_planning_time='60m'";
        const partitions: string[] = [];
        for (const p of partitionColumnNames) {
          if (columns.find((c) => c.name === p)) {
            partitions.push(p);
          }
        }
        if (partitions.length) {
          const format =
            ('materialization' in modelJson &&
              modelJson.materialization &&
              'format' in modelJson.materialization &&
              modelJson.materialization.format) ||
            null;
          switch (format) {
            case 'iceberg':
              modelConfig.properties = {
                partitions: `ARRAY['${partitions.join("', '")}']`,
              };
              break;
            default:
              modelConfig.properties = {
                partitioned_by: `ARRAY['${partitions.join("', '")}']`,
              };
          }
        }
        break;
      }
      default:
      // No additional config needed
    }
  }

  if ('sql_hooks' in modelJson && modelJson.sql_hooks) {
    if (modelJson.sql_hooks.post) {
      modelConfig.post_hook = modelJson.sql_hooks.post;
    }
    if (modelJson.sql_hooks.pre) {
      modelConfig.pre_hook = modelJson.sql_hooks.pre;
    }
  }

  const modelConfigArgs: string[] = [];
  for (const [k, v] of Object.entries(modelConfig)) {
    try {
      switch (typeof v) {
        case 'object': {
          modelConfigArgs.push(`${k}=${JSON.stringify(v)}`);
          break;
        }
        default: {
          modelConfigArgs.push(`${k}="${v}"`);
          break;
        }
      }
    } catch {}
  }
  if (modelConfigArgs.length) {
    sql += `{{
  config(
    ${modelConfigArgs.join(',\n    ')}
  )
}}\n\n`;
  }
  //

  // Append model SQL
  const modelSql = `
with ${modelName} as (
${modelSelect.sql}
${modelFrom.sql}
${
  modelTableFunction
    ? '' // If the model has a table function, where filters are applied inside it
    : modelWhere.sql
}
${modelGroupBy.sql}
${modelHaving.sql}
)
select * from ${modelName}
`;
  sql += sqlFormat(modelSql);
  //

  return {
    config: modelConfig,
    modelId,
    project: {
      ...project,
      manifest: frameworkModelManifestMerge({
        modelJson,
        project,
      }),
    },
    properties: modelProperties,
    sql: sql.trim(),
    yml: yamlStringify({
      version: '2',
      models: [modelProperties],
      // semantic_models: [],
    }),
  };
}

export function frameworkGenerateSourceOutput({
  project,
  sourceJson,
}: {
  project: DbtProject;
  sourceJson: FrameworkSource;
}): {
  project: DbtProject;
  yml: string;
} {
  const sourceProperties = frameworkSourceProperties(sourceJson);

  // Merge new properties into the source manifest

  return {
    project: {
      ...project,
      manifest: frameworkSourceManifestMerge({
        project,
        sourceJson,
      }),
    },
    yml: yamlStringify({
      version: '2',
      sources: [sourceProperties],
    }),
  };
}

export function isAggregateExpr(expr?: string): boolean {
  return FRAMEWORK_AGGS.some((agg) => {
    const aggRegex = new RegExp(`\\b${agg}\\([^)]+\\)(?!\\s*over\\s*\\()`, 'i');
    return expr && aggRegex.test(expr.toLowerCase().trim());
  });
}

export function frameworkModelHasAgg({
  modelJson,
}: {
  modelJson: FrameworkModel;
}): boolean {
  return !!(
    ('group_by' in modelJson && modelJson.group_by?.length) ||
    ('rollup' in modelJson.from && modelJson.from.rollup) ||
    ('lookback' in modelJson.from && modelJson.from.lookback) ||
    ('select' in modelJson &&
      modelJson.select &&
      modelJson.select.some(
        (c) =>
          typeof c === 'object' &&
          !!(
            ('agg' in c && c.agg) ||
            ('aggs' in c && c.aggs) ||
            ('expr' in c && c.expr && isAggregateExpr(c.expr))
          ),
      ))
  );
}

export function frameworkModelTableFunction({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): FrameworkSourceMeta['table_function'] | null {
  if ('from' in modelJson && 'source' in modelJson.from) {
    const sourceMeta = frameworkGetSourceMeta({
      project,
      source: modelJson.from.source,
    });
    return sourceMeta?.table_function || null;
  }
  return null;
}

export function frameworkInheritColumn(
  col: FrameworkColumn,
  merge?: Partial<Omit<FrameworkColumn, 'meta'>> & {
    meta?: Partial<FrameworkColumn['meta']>;
    tags?: string[];
  },
): FrameworkColumn {
  col.name = merge?.name || col.name;
  col.data_type = merge?.data_type || col.data_type;
  col.description = merge?.description || col.description;
  col.tags = _.union(col.tags, merge?.tags);
  // We never inherit these properties
  if ('agg' in col.meta) delete col.meta.agg;
  if ('aggs' in col.meta) delete col.meta.aggs;
  if ('description' in col.meta) delete col.meta.description; // Don't need to inherit on the meta
  if ('exclude_from_group_by' in col.meta)
    delete col.meta.exclude_from_group_by;
  if ('expr' in col.meta) delete col.meta.expr;
  if ('prefix' in col.meta) delete col.meta.prefix;
  //
  col.meta = { ...col.meta, ...merge?.meta };
  return col;
}

export function frameworkInheritColumns(
  cols: FrameworkColumn[],
  merge?: Partial<Omit<FrameworkColumn, 'meta'>> & {
    meta?: Partial<FrameworkColumn['meta']>;
  },
): FrameworkColumn[] {
  return cols.map((col) => frameworkInheritColumn(col, merge));
}

export function frameworkInheritModel({
  model,
  project,
}: {
  model: string;
  project: DbtProject;
}): { metrics: LightdashMetrics } {
  const node = frameworkGetNode({ project, model });
  if (node?.resource_type !== 'model') return { metrics: {} };
  return { metrics: node.meta?.metrics || {} };
}

export function frameworkInheritModels({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): {
  metrics: LightdashMetrics;
} {
  let metrics: LightdashMetrics = {};

  if ('from' in modelJson && 'model' in modelJson.from) {
    const baseModel = modelJson.from.model;
    metrics = {
      ...metrics,
      ...frameworkInheritModel({ model: baseModel, project }).metrics,
    };
    if ('join' in modelJson.from && modelJson.type !== 'int_join_column') {
      for (const join of modelJson.from.join || []) {
        metrics = {
          ...metrics,
          ...frameworkInheritModel({ model: join.model, project }).metrics,
        };
      }
    }
    if ('union' in modelJson.from) {
      for (const model of modelJson.from.union.models || []) {
        metrics = {
          ...metrics,
          ...frameworkInheritModel({ model, project }).metrics,
        };
      }
    }
  }

  return { metrics };
}

/**
 * Function to convert the basic model creation inputs to a starting model json
 * We intentionally leave some of the arrays empty to ensure users are forced to fill them in
 * This requires typescript overrides where a minimum number of items are defined
 */
export function frameworkMakeModelTemplate({
  type,
  group,
  name,
  topic,
}: Api<'framework-model-create'>['request']): FrameworkModel {
  const base = { group, topic, name };
  switch (type) {
    case 'int_join_column':
      return {
        ...base,
        type,
        from: {
          model: '',
          join: {
            column: '',
            fields:
              [] as unknown as SchemaModelTypeIntJoinColumn['from']['join']['fields'],
            type: 'cross_join_unnest',
          },
        },
        select: [] as unknown as SchemaModelTypeIntJoinColumn['select'],
      };
    case 'int_join_models':
      return {
        ...base,
        type,
        from: {
          model: '',
          join: [] as unknown as SchemaModelTypeIntJoinModels['from']['join'],
        },
        select: [] as unknown as SchemaModelTypeIntJoinModels['select'],
      };
    case 'int_lookback_model':
      return {
        ...base,
        type,
        from: {
          model: '',
          lookback: { days: 8 },
        },
        select: [] as unknown as SchemaModelTypeIntLookbackModel['select'],
      };
    case 'int_rollup_model':
      return {
        ...base,
        type,
        from: {
          model: '',
          rollup: {
            interval:
              '' as unknown as SchemaModelTypeIntRollupModel['from']['rollup']['interval'],
          },
        },
      };
    case 'int_select_model':
      return {
        ...base,
        type,
        from: {
          model: '',
        },
        select: [] as unknown as SchemaModelTypeIntSelectModel['select'],
      };
    case 'int_union_models':
      return {
        ...base,
        type,
        from: {
          model: '',
          union: {
            models:
              [] as unknown as SchemaModelTypeIntUnionModels['from']['union']['models'],
          },
        },
        select: [] as unknown as SchemaModelTypeIntUnionModels['select'],
      };
    case 'mart_join_models':
      return {
        type,
        ...base,
        tags: ['lightdash', 'lightdash-explore'],
        from: {
          model: '',
          join: [] as unknown as SchemaModelTypeMartJoinModels['from']['join'],
        },
        select: [] as unknown as SchemaModelTypeMartJoinModels['select'],
      };
    case 'mart_select_model':
      return {
        type,
        ...base,
        tags: ['lightdash', 'lightdash-explore'],
        from: {
          model: '',
        },
        select: [] as unknown as SchemaModelTypeMartSelectModel['select'],
      };
    case 'stg_select_model':
      return {
        ...base,
        type,
        from: {
          model: '',
        },
        select: [] as unknown as SchemaModelTypeStgSelectModel['select'],
      };
    case 'stg_select_source':
      return {
        ...base,
        type,
        from: {
          source: '',
        },
        select: [] as unknown as SchemaModelTypeStgSelectSource['select'],
      };
    case 'stg_union_sources':
      return {
        ...base,
        type,
        from: {
          source: '',
          union: {
            sources:
              [] as unknown as SchemaModelTypeStgUnionSources['from']['union']['sources'],
          },
        },
        select: [] as unknown as SchemaModelTypeStgUnionSources['select'],
      };
    default:
      return assertExhaustive<FrameworkModel>(type);
  }
}

export function frameworkMakeSourceId({
  database,
  project,
  schema,
  table,
}: {
  database: string;
  project: { name: string };
  schema: string;
  table: string;
}): string {
  const sourceName = frameworkMakeSourceName({ database, schema });
  return `source.${project.name}.${sourceName}.${table}`;
}

export function frameworkMakeSourceName({
  database,
  schema,
}: {
  database: string;
  schema: string;
}): string {
  return `${database}__${schema}`;
}

export function frameworkMakeSourcePrefix({
  database,
  project,
  schema,
}: {
  database: string;
  project: DbtProject;
  schema: string;
}): string | null {
  const sourceName = frameworkMakeSourceName({ database, schema });
  const sourcePath = path.join(
    project.pathSystem,
    project.modelPaths[0] || 'models',
    'sources',
    database,
    sourceName,
  );
  return sourcePath;
}

export function frameworkMakeSourceRef({
  database,
  schema,
  table,
}: {
  database: string;
  schema: string;
  table: string;
}): string {
  const sourceName = frameworkMakeSourceName({ database, schema });
  return `${sourceName}.${table}`;
}

export function frameworkModelManifestMerge({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): DbtProjectManifest {
  let manifest = { ...project.manifest };
  const modelProperties = frameworkModelProperties({
    modelJson,
    project,
  });
  const modelId = `model.${project.name}.${modelProperties.name}`;
  const existingModel = manifest.nodes[modelId];
  const columns: DbtProjectManifestNode['columns'] = {};
  for (const column of modelProperties.columns) {
    const existingModelColumn = existingModel?.columns?.[column.name];
    columns[column.name] = {
      ...existingModelColumn,
      ...column,
    };
  }
  manifest = {
    ...manifest,
    nodes: {
      ...manifest.nodes,
      [modelId]: {
        ...existingModel,
        // Only setting the properties that are needed for a temporary in-memory merge, re-parsing the project will add the remaining
        columns,
        config: modelProperties.config,
        resource_type: 'model',
        meta: modelProperties.meta,
        ...(modelProperties.config?.tags && {
          tags: modelProperties.config?.tags,
        }),
        unique_id: `model.${project.name}.${modelProperties.name}`,
      },
    },
  };
  return manifest;
}

function frameworkModelFrom({
  datetimeInterval,
  modelJson,
  project,
}: {
  datetimeInterval: FrameworkInterval | null;
  modelJson: FrameworkModel;
  project: DbtProject;
}): { comments: string[]; sql: string } {
  const comments: string[] = [];
  let sql: string = '';
  const appendSql = (line: string) =>
    sql ? (sql += `\n${line}`) : (sql = line);

  // We don't add a from block here for unions
  if ('union' in modelJson.from && modelJson.from.union) {
    return { comments, sql };
  }

  if (
    modelJson.type === 'int_join_column' &&
    'from' in modelJson &&
    'join' in modelJson.from &&
    'column' in modelJson.from.join &&
    'type' in modelJson.from.join
  ) {
    // Handle model join to a column
    const baseModel = modelJson.from.model;
    appendSql('from');
    appendSql(`{{ ref('${baseModel}') }} ${baseModel}`);
    switch (modelJson.from.join.type) {
      case 'cross_join_unnest': {
        appendSql(
          `cross join unnest(${modelJson.from.join.column}) as t(${modelJson.from.join.fields?.join(',')})`,
        );
        break;
      }
    }
  } else if (
    modelJson.type !== 'int_join_column' &&
    'from' in modelJson &&
    'join' in modelJson.from &&
    'model' in modelJson.from
  ) {
    // Handle joined model refs
    const baseModel = modelJson.from.model;
    appendSql('from');
    appendSql(`{{ ref('${baseModel}') }} ${baseModel}`);
    for (const joinTo of modelJson.from.join || []) {
      if (!joinTo.model) continue;
      const alias = joinTo.override_alias || joinTo.model;
      appendSql(
        `${joinTo.type || 'inner'} join {{ ref('${joinTo.model}') }} ${alias}`,
      );
      if ('on' in joinTo && 'and' in joinTo.on) {
        appendSql('on');
        const sqlJoinOnAnd = [];
        for (const on of joinTo.on?.and || []) {
          if (typeof on === 'string') {
            sqlJoinOnAnd.push(`${baseModel}.${on}=${joinTo.model}.${on}`);
          } else if ('expr' in on) {
            sqlJoinOnAnd.push(on.expr);
          }
        }
        appendSql(sqlJoinOnAnd.join(' and '));
      }
    }
  } else if ('lookback' in modelJson.from && modelJson.from.lookback) {
    // Handle lookback
    const baseModel = modelJson.from.model;
    const lookbackDays = modelJson.from.lookback.days || 0;
    appendSql('FROM {{ _ext_event_dates_table() }}');
    appendSql(`INNER JOIN {{ ref('${baseModel}') }}`);
    if (!!modelJson.from.lookback.exclude_event_date) {
      appendSql('ON portal_partition_daily < _ext_event_date');
    } else {
      appendSql('ON portal_partition_daily <= _ext_event_date');
    }
    appendSql(
      `AND portal_partition_daily >= date_add('day', -${lookbackDays}, _ext_event_date)`,
    );
  } else if ('from' in modelJson) {
    appendSql('from');
    if (
      'source' in modelJson.from &&
      typeof modelJson.from.source === 'string'
    ) {
      const sourceRef = frameworkGetSourceRef(modelJson);
      const sourceJinja = `{{ source('${sourceRef?.split('.').join("','")}') }}`;
      const tableFunction = frameworkModelTableFunction({ modelJson, project });
      if (tableFunction) {
        const tableFunctionSql = frameworkTableFunctionSql({
          datetimeInterval,
          modelJson,
          project,
        });
        appendSql(tableFunctionSql);
        comments.push(`depends_on: ${sourceJinja}`);
      } else {
        appendSql(sourceJinja);
      }
    } else if (
      'model' in modelJson.from &&
      typeof modelJson.from.model === 'string'
    ) {
      appendSql(`{{ ref('${modelJson.from.model}') }}`);
    }
  }

  return { comments, sql };
}

function frameworkModelGroupBy({
  columns,
  modelJson,
}: {
  columns: FrameworkColumn[];
  modelJson: FrameworkModel;
}): { comments: string[]; sql: string } {
  const comments: string[] = [];
  let sql: string = '';
  const appendSql = (line: string) =>
    sql ? (sql += `\n${line}`) : (sql = line);

  // We don't add a group by block for unions
  if ('union' in modelJson.from && modelJson.from.union) {
    return { comments, sql };
  }

  const facts = columns.filter((c) => c.meta.type === 'fct');
  const dimensions = columns.filter((c) => c.meta.type === 'dim');

  const hasAggFact =
    facts.some((f) => !!f.meta.agg || !!f.meta.aggs) ||
    !!('lookback' in modelJson.from && modelJson.from.lookback);
  const shouldGroupBy =
    hasAggFact ||
    ('lookback' in modelJson.from && dimensions.length) ||
    ('rollup' in modelJson.from && dimensions.length) ||
    ('group_by' in modelJson && modelJson.group_by?.length);
  if (!shouldGroupBy) {
    return { comments, sql };
  }

  appendSql('group by');
  const sqlGroupBy: string[] = [];
  if ('rollup' in modelJson.from) {
    for (const d of dimensions) {
      if (d.meta.expr) {
        sqlGroupBy.push(d.meta.expr);
      } else {
        sqlGroupBy.push(d.meta.prefix ? `${d.meta.prefix}.${d.name}` : d.name);
      }
    }
  } else if (hasAggFact) {
    for (const c of columns) {
      if (
        ('agg' in c.meta && c.meta.agg) ||
        (c.meta.type === 'fct' && frameworkSuffixAgg(c.name)) ||
        c.meta.type === 'fct' ||
        ('exclude_from_group_by' in c.meta && c.meta.exclude_from_group_by)
      ) {
        continue;
      }
      if (c.meta.expr) {
        sqlGroupBy.push(c.meta.expr);
      } else {
        sqlGroupBy.push(c.meta.prefix ? `${c.meta.prefix}.${c.name}` : c.name);
      }
    }
  }
  if ('group_by' in modelJson && modelJson.group_by?.length) {
    const groupBy = modelJson.group_by;
    for (const g of groupBy) {
      if (typeof g === 'string') {
        if (sqlGroupBy.includes(g)) continue;
        sqlGroupBy.push(g);
      } else if ('expr' in g && g.expr) {
        if (sqlGroupBy.includes(g.expr)) continue;
        sqlGroupBy.push(g.expr);
      } else if ('type' in g && g.type === 'dims') {
        for (const d of dimensions) {
          if (d.meta.exclude_from_group_by) continue;
          const line = d.meta.expr
            ? d.meta.expr
            : d.meta.prefix
              ? `${d.meta.prefix}.${d.name}`
              : d.name;
          if (sqlGroupBy.includes(line)) continue;
          sqlGroupBy.push(line);
        }
      }
    }
  }

  // We always need to group by the lookback date in lookback models
  if ('lookback' in modelJson.from && modelJson.from.lookback) {
    const lookbackDateColumn = '_ext_event_date';
    if (!sqlGroupBy.includes(lookbackDateColumn)) {
      sqlGroupBy.push(lookbackDateColumn);
    }
  }

  appendSql(sqlGroupBy.join(',\n'));

  return { comments, sql };
}

function frameworkModelHaving({ modelJson }: { modelJson: FrameworkModel }): {
  comments: string[];
  sql: string;
} {
  const comments: string[] = [];
  let sql: string = '';
  const appendSql = (line: string) =>
    sql ? (sql += `\n${line}`) : (sql = line);

  // We don't support having in unions
  if (
    ('union' in modelJson.from && modelJson.from.union) ||
    !('having' in modelJson && modelJson.having)
  ) {
    return { comments, sql };
  }

  const sqlAnd: string[] = [];
  const sqlOr: string[] = [];

  if (typeof modelJson.having === 'string') {
    sqlAnd.push(modelJson.having);
  } else {
    for (const havingAnd of modelJson.having.and || []) {
      if (typeof havingAnd === 'string') {
        sqlAnd.push(havingAnd);
      } else if ('expr' in havingAnd && havingAnd.expr) {
        sqlAnd.push(havingAnd.expr);
      }
    }
    for (const havingOr of modelJson.having.or || []) {
      if (typeof havingOr === 'string') {
        sqlOr.push(havingOr);
      } else if ('expr' in havingOr && havingOr.expr) {
        sqlOr.push(havingOr.expr);
      }
    }
  }

  if (![...sqlAnd, ...sqlOr].length) {
    return { comments, sql };
  }

  appendSql('having');
  if (sqlAnd.length && sqlOr.length) {
    appendSql([sqlAnd.join(' and '), sqlAnd.join(' or ')].join(' or '));
  } else if (sqlAnd.length) {
    appendSql(sqlAnd.join(' and '));
  } else if (sqlOr.length) {
    appendSql(sqlOr.join(' or '));
  }

  return { comments, sql };
}

export function frameworkModelNodeColor({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): string | null {
  const modelLayer = frameworkGetModelLayer(modelJson);
  switch (modelLayer) {
    case 'int': {
      const childMap = frameworkGetModelChildMap({ modelJson, project });
      const hasMartChild = _.some(childMap, (id) =>
        _.startsWith(id, `model.${project.name}.mart__`),
      );
      if (hasMartChild) {
        return '#DAA520';
      }
      return null;
    }
    case 'mart': {
      return '#059669';
    }
    case 'stg': {
      return '#B6AB33';
    }
  }
}

export function frameworkModelProperties({
  modelJson,
  project,
}: {
  modelJson: FrameworkModel;
  project: DbtProject;
}): DbtModelProperties {
  const { columns, modelMetrics } = frameworkBuildColumns({
    modelJson,
    project,
  });

  const modelName = frameworkGetModelName(modelJson);
  const modelLayer = frameworkGetModelLayer(modelJson);
  const tags = frameworkBuildModelTags({ modelJson, project });

  const modelProperties: DbtModelProperties = {
    name: modelName,
    group: modelJson.group,
    description: modelJson.description || '',
    docs: { show: true },
    private: false,
    contract: { enforced: false },
    config: {},
    meta: { metrics: modelMetrics },
    columns: [],
  };

  let portalPartitionColumns: string[] | null = null;
  if ('source' in modelJson.from) {
    const sourceMeta = frameworkGetSourceMeta({
      project,
      ...modelJson.from,
    });
    portalPartitionColumns = sourceMeta?.portal_partition_columns || null;
  } else {
    const modelMeta = frameworkGetModelMeta({
      project,
      ...modelJson.from,
    });
    portalPartitionColumns = modelMeta?.portal_partition_columns || null;
  }
  if (portalPartitionColumns) {
    // This property will continue to be inherited by direct child models
    modelProperties.meta = {
      ...modelProperties.meta,
      portal_partition_columns: portalPartitionColumns,
    };
  }

  // Apply tags to model
  if (tags.model.length) {
    modelProperties.config = { ...modelProperties.config, tags: tags.model };
  }
  // Specify tags which should stay local to this model
  if (tags.local.length) {
    modelProperties.meta = { ...modelProperties.meta, local_tags: tags.local };
  }
  // Add model level lightdash meta
  if ('lightdash' in modelJson) {
    modelProperties.meta = {
      ...modelProperties.meta,
      ...modelJson.lightdash?.table,
    };
    for (const { name: metricName, ...metric } of modelJson.lightdash
      ?.metrics || []) {
      modelProperties.meta.metrics = {
        ...modelProperties.meta.metrics,
        [metricName]: metric,
      };
    }
  }

  // Persist columns on the model properties
  const modelPropertiesColumns: DbtModelPropertiesColumn[] = [];
  for (const c of columns) {
    // Control ordering of column properties
    const column: DbtModelPropertiesColumn = {
      name: frameworkColumnName({ column: c, modelJson }),
      data_type: c.data_type || 'varchar',
      description: c.description || textToStartCase(c.name),
      tags: c.tags,
      // Switch to data_tests on the yml once dbt is updated to >=1.8
      // data_tests: c.data_tests,
      tests: c.data_tests,
      meta: c.meta,
    };

    if (
      'materialized' in modelJson &&
      modelJson.materialized === 'incremental'
    ) {
      // Add standard tests for these columns on incremental models
      switch (column.name) {
        case 'portal_partition_monthly':
        case 'portal_partition_daily':
        case 'portal_partition_hourly': {
          const dataTests = column.tests || [];
          if (!dataTests.includes('not_null')) {
            dataTests.push('not_null');
          }
          column.tests = dataTests;
          break;
        }
      }
    }

    // Setting lightdash dimension meta
    let dimension = { ...c.meta.dimension };
    if (typeof dimension.time_intervals !== 'string') {
      // If the time_intervals aren't a string, sort alphabetically
      dimension.time_intervals = _.chain([...(dimension.time_intervals || [])])
        .sort()
        .uniq()
        .value();
    }
    // Set defaults for column level properties at the mart layer
    if (modelLayer === 'mart') {
      if (dimension.hidden === undefined) {
        dimension.hidden =
          column.meta?.type === 'fct' ||
          FRAMEWORK_PARTITIONS.includes(column.name as FrameworkPartitionName);
      }
      if (!dimension.label) {
        dimension.label = textToStartCase(column.name);
      }
      if (!dimension.type) {
        dimension.type = lightdashConvertDimensionType(column.data_type);
      }
      if (column.name === 'datetime') {
        // Find a partitioned column to use for time intervals
        const partitionedColumn =
          columns.find((c) => c.name === 'portal_partition_hourly') ||
          columns.find((c) => c.name === 'portal_partition_daily') ||
          columns.find((c) => c.name === 'portal_partition_monthly');
        if (partitionedColumn) {
          dimension.sql = partitionedColumn.name;
        }
      }
    }

    // Control ordering of lightdash dimension properties
    dimension = orderKeys(dimension, [
      'ai_hint',
      'type',
      'label',
      'group_label',
      'groups',
    ]);

    // Order lightdash metric keys and remove empty properties
    const metrics = _.reduce(
      column.meta?.metrics || {},
      (m, metric, metricName) => ({
        ...m,
        [metricName]: removeEmpty(
          orderKeys(metric, [
            'ai_hint',
            'type',
            'label',
            'group_label',
            'groups',
          ]),
        ),
      }),
      {},
    );

    // Control ordering and only include certain meta properties
    column.meta = removeEmpty({
      type: column.meta?.type,
      origin: column.meta?.origin,
      dimension: removeEmpty(dimension),
      metrics,
    });

    // Remove any remaining empty column properties
    modelPropertiesColumns.push(removeEmpty(column));
  }

  // Set data_tests at model level
  if ('data_tests' in modelJson && modelJson.data_tests) {
    for (const data_test of modelJson.data_tests) {
      switch (data_test.type) {
        case 'unique':
          {
            modelProperties.tests = [
              ...(modelProperties.tests || []),
              { unique: { column_name: data_test.column_name } },
            ];
          }
          break;
      }
    }
  }

  modelProperties.columns = modelPropertiesColumns;

  // Look for specific metrics to keep (all others will be dropped)
  const metricsModelInclude =
    ('lightdash' in modelJson &&
      modelJson.lightdash &&
      'metrics_include' in modelJson.lightdash &&
      modelJson.lightdash.metrics_include) ||
    null;
  if (metricsModelInclude) {
    for (const metricName in modelProperties.meta?.metrics) {
      if (!metricsModelInclude.includes(metricName)) {
        delete modelProperties.meta.metrics[metricName];
      }
    }
  }

  // Look for excluded metrics to drop
  const metricsModelExclude =
    ('lightdash' in modelJson &&
      modelJson.lightdash &&
      'metrics_exclude' in modelJson.lightdash &&
      modelJson.lightdash.metrics_exclude) ||
    [];
  for (const metricName of metricsModelExclude) {
    if (modelProperties.meta?.metrics?.[metricName]) {
      delete modelProperties.meta.metrics[metricName];
    }
  }

  // Set node color for docs
  const nodeColor = frameworkModelNodeColor({ modelJson, project });
  if (nodeColor) {
    modelProperties.docs = orderKeys({
      ...modelProperties.docs,
      node_color: nodeColor,
    });
  }

  // Set addition defaults for model properties by layer
  switch (modelLayer) {
    case 'mart': {
      if (!modelProperties.meta?.required_filters) {
        if (
          _.find(
            modelProperties.columns,
            (c) => c.name === 'portal_partition_monthly',
          )
        ) {
          if (
            _.find(
              modelProperties.columns,
              (c) => c.name === 'portal_partition_daily',
            )
          ) {
            modelProperties.meta = {
              ...modelProperties.meta,
              required_filters: [{ datetime: 'inThePast 14 days' }],
            };
          } else {
            modelProperties.meta = {
              ...modelProperties.meta,
              required_filters: [{ datetime: 'inThePast 2 months' }],
            };
          }
        }
      }
      break;
    }
  }

  if (_.isEmpty(modelProperties.meta?.metrics)) {
    delete modelProperties.meta?.metrics;
  }
  if (_.isEmpty(modelProperties.meta)) {
    delete modelProperties.meta;
  }

  return modelProperties;
}

function frameworkModelSelect({
  columns,
  datetimeInterval,
  modelJson,
  project,
}: {
  columns: FrameworkColumn[];
  datetimeInterval: FrameworkInterval | null;
  modelJson: FrameworkModel;
  project: DbtProject;
}): { comments: string[]; sql: string } {
  const partitionColumnNames = frameworkGetPartitionColumnNames({
    modelJson,
    project,
  });

  const comments: string[] = [];
  let hasPartitionColumnsComment: boolean = false;
  let sql: string = '';
  const appendSql = (line: string) =>
    sql ? (sql += `\n${line}`) : (sql = line);

  const sqlLines: string[] = [];
  if ('union' in modelJson.from && modelJson.from.union) {
    // HANDLE UNION
    const { union } = modelJson.from;
    const sqlSelectLines: string[] = [];
    for (const c of columns) {
      sqlSelectLines.push(frameworkColumnSelect(c));
    }
    if ('model' in modelJson.from && 'models' in union && union.models.length) {
      const baseModel = modelJson.from.model;
      const modelRefs: string[] = [baseModel];
      for (const unionModel of union.models || []) {
        modelRefs.push(unionModel);
      }
      for (const modelRef of modelRefs) {
        let sqlLine = `select ${sqlSelectLines.join(', ')} from {{ ref('${modelRef}') }}`;
        const filters = frameworkBuildFilters({
          datetimeInterval,
          from: { model: modelRef },
          modelJson,
          project,
        });
        if (filters.length) {
          sqlLine += ` where ${filters.join(' and ')}`;
        }
        sqlLines.push(sqlLine);
      }
    } else if ('source' in modelJson.from && 'sources' in union) {
      const baseSource = modelJson.from.source;
      const sourceRefs: string[] = [baseSource];
      for (const unionSource of union.sources || []) {
        sourceRefs.push(unionSource);
      }
      for (const sourceRef of sourceRefs) {
        let sqlLine = `select ${sqlSelectLines.join(',')} from {{ source('${sourceRef.split('.').join("', '")}') }}`;
        const filters = frameworkBuildFilters({
          datetimeInterval,
          from: { source: sourceRef },
          modelJson,
          project,
        });
        if (filters.length) {
          sqlLine += ` where ${filters.join(' and ')}`;
        }
        sqlLines.push(sqlLine);
      }
    }
    appendSql(sqlLines.join(' union all '));
  } else {
    for (const c of columns) {
      const suffixAgg = frameworkSuffixAgg(c.name);
      const metaAgg = ('agg' in c.meta && c.meta.agg) || '';
      const newAgg =
        metaAgg ||
        ('rollup' in modelJson.from && c.meta.type === 'fct' && suffixAgg);
      const shouldAlias = !!c.meta.expr || !!newAgg;
      const prefix = !c.meta.expr && c.meta.prefix ? `${c.meta.prefix}.` : '';
      const nameWithSuffix = frameworkColumnName({ column: c, modelJson });
      // const newSuffixAgg = frameworkSuffixAgg(nameWithSuffix);

      let line = c.meta.expr || `${prefix}${c.name}`;
      if (newAgg) {
        switch (newAgg) {
          case 'count': {
            if (suffixAgg === 'count') {
              // If the input column is already a count agg, them going forward we'll sum it
              line = `sum(${line})`;
            } else {
              line = `count(${line})`;
            }
            break;
          }
          case 'hll': {
            if (suffixAgg === 'hll') {
              line = `cast(merge(cast(${line} as hyperloglog)) as varbinary)`;
            } else {
              line = `cast(approx_set(${line}) as varbinary)`;
            }
            break;
          }
          case 'tdigest': {
            if (suffixAgg === 'tdigest') {
              line = `cast(merge(cast(${line} as tdigest)) as varbinary)`;
            } else {
              line = `cast(tdigest_agg(${line}) as varbinary)`;
            }
            break;
          }
          default: {
            line = `${newAgg}(${line})`;
          }
        }
      }
      if (shouldAlias) {
        line = `${line} as ${nameWithSuffix}`;
      }

      sqlLines.push(line);
    }

    if (sqlLines.length) {
      // HANDLE SELECT
      appendSql('select');
      appendSql(
        '\n' +
          sqlLines
            .map((line) => {
              if (
                (partitionColumnNames.includes(line.split(' ').pop() || '') ||
                  partitionColumnNames.includes(line.split('.').pop() || '')) &&
                !hasPartitionColumnsComment
              ) {
                hasPartitionColumnsComment = true;
                return `-- partition columns\n${sqlCleanLine(line)} `;
              }
              return sqlCleanLine(line);
            })
            .join(',\n'),
      );
    }
  }

  return { comments, sql };
}

function frameworkModelWhere({
  datetimeInterval,
  modelJson,
  project,
}: {
  datetimeInterval: FrameworkInterval | null;
  modelJson: FrameworkModel;
  project: DbtProject;
}): { comments: string[]; sql: string } {
  const comments: string[] = [];
  let sql: string = '';
  const appendSql = (line: string) =>
    sql ? (sql += `\n${line}`) : (sql = line);

  // We don't add a from block here for unions
  if ('union' in modelJson.from && modelJson.from.union) {
    return { comments, sql };
  }

  const sqlAndFramework: string[] = [];
  const sqlOrFramework: string[] = [];

  if (
    'from' in modelJson &&
    'model' in modelJson.from &&
    // Lookback models are filtered in the join
    !('lookback' in modelJson.from && modelJson.from.lookback)
  ) {
    sqlAndFramework.push(
      ...frameworkBuildFilters({
        datetimeInterval,
        from: modelJson.from,
        modelJson,
        project,
        prefix:
          'join' in modelJson.from && modelJson.type !== 'int_join_column'
            ? modelJson.from.model
            : undefined,
      }),
    );
  } else if ('from' in modelJson && 'source' in modelJson.from) {
    sqlAndFramework.push(
      ...frameworkBuildFilters({
        datetimeInterval,
        from: modelJson.from,
        modelJson,
        project,
      }),
    );
  }

  const sqlAndUser: string[] = [];
  const sqlOrUser: string[] = [];

  if ('where' in modelJson) {
    if (typeof modelJson.where === 'string') {
      sqlAndUser.push(modelJson.where);
    } else {
      for (const c of modelJson.where?.and || []) {
        if (c.expr) {
          sqlAndUser.push(c.expr);
        }
      }
      for (const c of modelJson.where?.or || []) {
        if (c.expr) {
          sqlOrUser.push(c.expr);
        }
      }
    }
  }

  const sqlWhereFramework: string[] = [];
  const sqlWhereUser: string[] = [];
  if (sqlAndFramework.length) {
    sqlWhereFramework.push(sqlAndFramework.join(' and '));
  }
  if (sqlOrFramework.length) {
    sqlWhereFramework.push(sqlOrFramework.join(' or '));
  }
  if (sqlAndUser.length) {
    sqlWhereUser.push(sqlAndUser.join(' and '));
  }
  if (sqlOrUser.length) {
    sqlWhereUser.push(sqlOrUser.join(' or '));
  }

  if (sqlWhereFramework.length && sqlWhereUser.length) {
    appendSql('where');
    appendSql('(');
    appendSql(sqlWhereFramework.join(' or '));
    appendSql(') and (');
    appendSql(sqlWhereUser.join(' or '));
    appendSql(')');
  } else if (sqlWhereFramework.length) {
    appendSql('where');
    appendSql(sqlWhereFramework.join(' or '));
  } else if (sqlWhereUser.length) {
    appendSql('where');
    appendSql(sqlWhereUser.join(' or '));
  }

  return { comments, sql };
}

export function frameworkSourceManifestMerge({
  project,
  sourceJson,
}: {
  project: DbtProject;
  sourceJson: FrameworkSource;
}): DbtProjectManifest {
  let manifest = { ...project.manifest };
  const sourceProperties = frameworkSourceProperties(sourceJson);
  for (const sourceTable of sourceProperties.tables) {
    const sourceId = `source.${project.name}.${sourceProperties.name}.${sourceTable.name}`;
    const existingSource = manifest.sources[sourceId];
    const columns: DbtProjectManifestSourceColumns = {};
    for (const column of sourceTable.columns) {
      const existingSourceColumn = existingSource?.columns?.[column.name];
      columns[column.name] = {
        ...existingSourceColumn,
        ...column,
      } as DbtProjectManifestSourceColumn;
    }
    manifest = {
      ...manifest,
      sources: {
        ...manifest.sources,
        [sourceId]: {
          ...existingSource,
          // Only setting the properties that are needed for a temporary in-memory merge, re-parsing the project will add the remaining
          columns,
          database: sourceProperties.database,
          meta: sourceTable.meta,
          name: sourceTable.name,
          package_name: project.name,
          resource_type: 'source',
          schema: sourceProperties.schema,
          source_meta: sourceProperties.meta,
          source_name: sourceProperties.name,
          tags: sourceProperties.tags,
          unique_id: `source.${project.name}.${sourceProperties.name}`,
        },
      },
    };
  }
  return manifest;
}

export function frameworkSourceProperties(
  sourceJson: FrameworkSource,
): DbtSourceProperties {
  const sourceName = frameworkMakeSourceName(sourceJson);
  const tables = _.map(sourceJson.tables, (t) => {
    const columns = _.map(t.columns, ({ type, lightdash, ...c }) => {
      const columnMeta: DbtSourceTableColumn['meta'] = {};
      if (type) columnMeta.type = type;
      if (lightdash) columnMeta.lightdash = lightdash;
      const column: DbtSourceTableColumn = {
        ...c,
      };
      if (_.size(columnMeta) > 0) {
        column.meta = columnMeta;
      }
      return column;
    });
    return {
      ...t,
      columns,
    };
  });
  const sourceProperties: DbtSourceProperties = removeEmpty({
    name: sourceName,
    database: sourceJson.database,
    description: sourceJson.description,
    freshness: sourceJson.freshness,
    loaded_at_field: sourceJson.loaded_at_field,
    schema: sourceJson.schema,
    meta: sourceJson.meta,
    tables,
  });
  return sourceProperties;
}

export function frameworkSuffixAgg(name: string): FrameworkColumnAgg | null {
  const nameParts = name.split('_');
  const suffix = nameParts[nameParts.length - 1] as FrameworkColumnAgg;
  return FRAMEWORK_AGGS.includes(suffix) ? suffix : null;
}

export function frameworkTableFunctionSql({
  datetimeInterval,
  modelJson,
  project,
}: {
  datetimeInterval: FrameworkInterval | null;
  modelJson: FrameworkModel;
  project: DbtProject;
}): string {
  const sourceRef = frameworkGetSourceRef(modelJson);
  if (!sourceRef) return '';

  const source = frameworkGetSource({ project, source: sourceRef });
  const sourceMeta = frameworkGetSourceMeta({ project, source: sourceRef });
  const tableFunction = sourceMeta?.table_function;
  const tableFunctionDatabase = tableFunction?.database;
  if (!source || !tableFunction || !tableFunctionDatabase) return '';

  // For table functions, we pass select all fields within the inner query, but ensure that filters are applied
  const dialect = tableFunction.dialect;
  // Path for the function to execute
  const tableFunctionPath = `${tableFunctionDatabase}.${tableFunction.schema}.${tableFunction.name}`;
  // The source table inside the context of the table function
  const tableFunctionSource = `${source.schema}.${source.name}`;
  const innerSql = sqlFormat(
    `
  select *
  from \`${tableFunctionSource}\`
  ${frameworkModelWhere({ datetimeInterval, modelJson, project }).sql.replaceAll("'", '"')}
  `,
    dialect, // The inner query should be formatted in the dialect of the table function;
  );
  const sql = sqlFormat(
    `TABLE(${tableFunctionPath}(${tableFunction.arg} => '
${innerSql}
'
))`,
  );

  return sql;
}
