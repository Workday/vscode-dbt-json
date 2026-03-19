/**
 * Constants for lineage parsing
 *
 * SQL keywords, Jinja keywords, and regex patterns used for
 * parsing expressions and extracting column references.
 */

// =============================================================================
// SQL Keywords
// =============================================================================

/**
 * SQL keywords to filter out when parsing expressions.
 * These are common SQL reserved words, functions, and data types
 * that should not be treated as column references.
 */
export const SQL_KEYWORDS = new Set([
  // SQL clauses and operators
  'select',
  'from',
  'where',
  'and',
  'or',
  'not',
  'null',
  'true',
  'false',
  'case',
  'when',
  'then',
  'else',
  'end',
  'as',
  'in',
  'like',
  'between',
  'is',
  'by',
  'on',
  'join',
  'left',
  'right',
  'inner',
  'outer',
  'cross',
  'union',
  'having',
  'group',
  'order',
  'limit',
  'offset',
  'distinct',
  'all',
  'any',
  'some',
  'exists',
  'the',
  'for',
  'to',
  'with',
  'into',
  'if',
  'value',
  'derived',

  // Aggregate functions
  'sum',
  'count',
  'avg',
  'min',
  'max',

  // String functions
  'cast',
  'coalesce',
  'lower',
  'upper',
  'trim',
  'concat',
  'substring',
  'extract',
  'convert',

  // Array functions
  'cardinality',
  'split',

  // Data types
  'varchar',
  'integer',
  'int',
  'bigint',
  'smallint',
  'decimal',
  'numeric',
  'double',
  'float',
  'boolean',
  'string',
  'text',
  'char',
  'date',
  'datetime',
  'time',
  'timestamp',
  'array',
  'struct',
  'map',
]);

// =============================================================================
// Jinja Keywords
// =============================================================================

/**
 * Jinja keywords and common macro parameters to filter out.
 * These are Jinja template syntax and common parameter names
 * that should not be treated as column references.
 */
export const JINJA_KEYWORDS = new Set([
  // Common macro parameters
  'expr',
  'data_type',
  'dialect',
  'include',
  'interval',
  'source_id',
  'use_range',
  'use_event_dates',
  'compile_dates',
  'ranges',
  'in_list',
  'prefix',
  'quote',
  'range',
  'query',
  'results',
  'partition_dates',
  'partition_date',
  'start',
  'end',
  'values',
  'filter_list',

  // Date/time related parameters
  'event_datetimes',
  'dates',
  'event_datetime',
  'date',
  'datetimes',
  'datetimes_iso',
  'event_dates_var',
  'start_date',
  'end_date',
  'start_datetime',
  'end_datetime',
  'days',
  'datetime',
  'event_month_starts',
  'event_month_start',
  'event_month_next',
  'event_month_days',

  // Jinja control structures
  'modules',
  'execute',
  'macro',
  'endmacro',
  'set',
  'if',
  'endif',
  'elif',
  'for',
  'endfor',
  'block',
  'endblock',
  'do',
  'call',
  'filter',
  'endfilter',
  'with',
  'endwith',
  'autoescape',
  'endautoescape',
  'trans',
  'endtrans',
  'pluralize',
  'raw',
  'endraw',
]);

// =============================================================================
// Regex Patterns
// =============================================================================

/**
 * Pattern to extract macro name and parameters from Jinja expression.
 * Matches: {{ macro_name(params) }}
 * Groups: [1] = macro name, [2] = parameters
 */
export const MACRO_PATTERN = /\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]*)\)/;

/**
 * Pattern to match identifiers (column names) in SQL expressions.
 * Matches both qualified (table.column) and unqualified (column) references.
 * Groups: [1] = table name (if qualified), [2] = column name (if qualified), [3] = column name (if unqualified)
 */
export const IDENTIFIER_PATTERN =
  /\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b|\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;

/**
 * Pattern to match string literals (single or double quoted).
 * Handles escaped quotes within strings.
 */
export const STRING_LITERAL_PATTERN = /'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"/g;

/**
 * Pattern to match function calls.
 * Used to remove function names before extracting column references.
 */
export const FUNCTION_CALL_PATTERN = /\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/g;

/**
 * Pattern to match simple column identifiers.
 * Matches: column_name (no dots, no special characters)
 */
export const SIMPLE_COLUMN_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Pattern to match qualified column references.
 * Matches: table.column
 * Groups: [1] = table/model name, [2] = column name
 */
export const QUALIFIED_COLUMN_PATTERN =
  /^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z0-9_]+)$/;

/**
 * Pattern to match literal values (numbers and strings).
 * Matches: 123, 123.45, 'string', "string"
 */
export const LITERAL_PATTERN = /^(['"].*['"]|[0-9]+(\.[0-9]+)?)$/;

// =============================================================================
// Aggregation Types
// =============================================================================

/**
 * Supported aggregation types for column suffix extraction.
 */
export const AGG_TYPES = [
  'count',
  'hll',
  'max',
  'min',
  'sum',
  'tdigest',
] as const;

export type AggType = (typeof AGG_TYPES)[number];
