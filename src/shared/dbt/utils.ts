import {
  dateAddDaysIso,
  dateDiffDays,
  DateIso,
  dateToIso,
  jsonParse,
  yamlParse,
} from '@shared';
import {
  DbtProject,
  DbtProjectCatalog,
  DbtProjectManifest,
  DbtProjectProperties,
  DbtProperties,
} from '@shared/dbt/types';
import * as _ from 'lodash';

export function dbtEventDates({ project }: { project: DbtProject }): {
  dates: string[];
  ranges: { end: string; start: string }[];
} {
  let dates: DateIso[] = [];
  const ranges: { end: string; start: string }[] = [];
  const varEventDates = project.variables?.event_dates;

  if (!varEventDates) return { dates, ranges };

  if (/d{4}-d{2}-d{2}~d{4}-d{2}-d{2}/.test(varEventDates)) {
    const [start, end] = varEventDates.split('~') as DateIso[];
    ranges.push({
      start,
      end: dateAddDaysIso(end, 1), // When doing a range, we'll exclude the end date
    });
    const days = dateDiffDays(start, end);
    for (let i = 0; i <= days; i++) {
      const date = new Date(start);
      date.setDate(date.getDate() + i);
      dates.push(dateToIso(date));
    }
  } else {
    dates = _.chain(varEventDates)
      .split(',')
      .uniq()
      .sort()
      .value() as DateIso[];
    for (const start of dates) {
      ranges.push({
        start,
        end: dateAddDaysIso(start, 1),
      });
    }
  }

  return { dates, ranges };
}

export function dbtSourcePropertiesString({
  project,
  sourceId,
}: {
  project: DbtProject;
  sourceId: string;
}) {
  const source = project.manifest.sources[sourceId];
  const properties = JSON.stringify(source).replaceAll("'", "''");
  return properties;
}

export function dbtSourceRegisterSql({
  project,
  sourceId,
}: {
  project: DbtProject;
  sourceId: string;
}) {
  const properties = dbtSourcePropertiesString({ project, sourceId });

  const sql = `
MERGE INTO ${project.name}.source_etl.dbt_sources old
USING (VALUES ('${sourceId}', '${properties}', false)) new (source_id, properties, etl_active)
ON (old.source_id = new.source_id)
WHEN MATCHED
    THEN UPDATE SET properties = new.properties
WHEN NOT MATCHED
    THEN INSERT (source_id, properties, etl_active) VALUES (new.source_id, new.properties, new.etl_active)
`;

  return sql;
}

/**
 * Function to normalize the macro id to include the project/package name
 * @param name Current string name of macro
 * @param project Project containing macro
 * @returns Id for macro incorporating both namespace and macro name
 */
export function getDbtMacroId({
  name = '',
  project,
}: {
  name: string | undefined;
  project: Partial<DbtProject>;
}): string | null {
  const nameArray = name.split('.');
  return nameArray.length === 1
    ? `${project.name}.${name}`
    : nameArray.length === 2
      ? name
      : null;
}

/**
 * Function to normalize the model id to include the project/package name
 * @param name Current string name of model
 * @param project Project containing model
 * @returns Id for model incorporating both namespace and model name
 */
export function getDbtModelId({
  modelName,
  projectName,
}: {
  modelName: string;
  projectName: string;
}): string {
  const nameArray = modelName.split('.');
  return nameArray.length === 1
    ? `model.${projectName}.${modelName}`
    : nameArray.length === 2
      ? modelName
      : '';
}

export function getDbtProjectProperties(content: string) {
  return yamlParse(content) as Partial<DbtProjectProperties>;
}

export function getDbtProjectCatalog(content: string) {
  return jsonParse(content) as Partial<DbtProjectCatalog>;
}

export function getDbtProjectManifest(content: string) {
  return jsonParse(content) as Partial<DbtProjectManifest>;
}

export function getDbtProperties(content: string) {
  return yamlParse(content) as Partial<DbtProperties>;
}
