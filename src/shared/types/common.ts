/**
 * Common utility types shared across extension and web.
 */

export type AppError = { message: string; details?: Record<string, any> };

export type LogLevel = 'debug' | 'error' | 'info' | 'warn';

export type DateIso = `${number}-${number}-${number}`;

export type DistributiveOmit<T, K extends keyof any> = T extends any
  ? Omit<T, K>
  : never;

export type RecursivePartial<T> = {
  [P in keyof T]?: RecursivePartial<T[P]>;
};
