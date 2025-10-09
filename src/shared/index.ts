import * as _ from 'lodash';
import * as yaml from 'yaml';

export type AppError = { message: string; details?: Record<string, any> };

const yamlParse = yaml.parse;
const yamlStringify = (obj: object) =>
  yaml.stringify(obj, { aliasDuplicateObjects: false });
export { yamlParse, yamlStringify };

export { parse as jsonParse } from 'jsonc-parser';

/** Utility function to ensure we're handling all cases in switch statements */
export function assertExhaustive<T extends any = any>(
  x: never,
  fallback?: any,
) {
  // throw new Error('Unexpected Case in Switch Statement: ' + x);
  return (fallback || null) as T;
}

export function convertTemplate(
  template: string | undefined = '',
  values: Record<string, string | undefined>,
): string {
  let converted = template;
  const matches = template.matchAll(/{{ (\S+) }}/g);
  for (const match of matches) {
    const variable = match[1];
    const value = values[variable] || '';
    converted = converted.replace(`{{ ${variable} }}`, value);
  }
  return converted;
}

export function dateAddDays(date: Date, days: number): Date {
  const _date = new Date(date);
  _date.setDate(_date.getDate() + days);
  return _date;
}

export function dateAddDaysIso(
  dateIso: string | DateIso,
  days: number,
): DateIso {
  return dateToIso(dateAddDays(new Date(dateIso), days));
}

export function dateDiffDays(
  start: Date | DateIso,
  end: Date | DateIso,
): number {
  const days = Math.round(
    (new Date(end).getTime() - new Date(start).getTime()) /
      (1000 * 60 * 60 * 24),
  );
  return days;
}

export function dateToIso(date: Date): DateIso {
  const dateIso = date.toISOString().split('T')[0] as DateIso;
  return dateIso;
}

export function datetimeIso(): string {
  return new Date().toISOString().replace('T', ' ').replace('Z', '');
}

export function isObject(item: any) {
  return (
    item && typeof item === 'object' && !Array.isArray(item) && item !== null
  );
}

export function mergeDeep<T extends Record<string, any>>(
  obj: T | null | undefined,
  add: RecursivePartial<T> | null | undefined,
): T {
  if (!obj && !add) return {} as T;
  if (obj && !add) return { ...obj } as T;
  if (!obj && add) return { ...add } as T;
  let _obj = { ...obj } as T;
  for (const key in add) {
    const v = add[key] as T[keyof T];
    if (isObject(v)) {
      if (!_obj[key]) {
        _obj = { ..._obj, [key]: {} } as T;
      }
      _obj[key] = mergeDeep(_obj[key], v);
    } else {
      _obj = { ..._obj, [key]: v } as T;
    }
  }
  return _obj as T;
}

// Utility function to order keys within an object
export function orderKeys<T extends Record<string, any>>(
  obj?: T,
  order?: (keyof T)[],
): T {
  order = order || [];
  const remaining = _.difference(Object.keys(obj || {}), order).sort();
  order = [...order, ...remaining];
  let ordered: Record<string, any> = {};
  for (const key of order) {
    if (obj && key in obj) ordered = { ...ordered, [key]: obj[key] };
  }
  return ordered as T;
}

// Utility function to remove empty keys from an object
export function removeEmpty<T extends Record<string, any>>(_obj?: T): T {
  if (!isObject(_obj)) return {} as T;
  const obj = { ..._obj };
  for (const [k, v] of Object.entries(obj)) {
    if (
      (typeof v === 'object' && _.isEmpty(v)) ||
      _.isNil(v) ||
      v === '' ||
      v === false
    ) {
      if (obj) delete obj[k];
    }
  }
  return obj as T;
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function textToStartCase(text: string): string {
  return _.chain(text).split('_').map(_.upperFirst).join(' ').trim().value();
}

export type DistributiveOmit<T, K extends keyof any> = T extends any
  ? Omit<T, K>
  : never;

export type RecursivePartial<T> = {
  [P in keyof T]?: RecursivePartial<T[P]>;
};

export type DateIso = `${number}-${number}-${number}`;
