/**
 * SQL utilities shared between extension and webview.
 */

import * as _ from 'lodash';
import type { DialectOptions, FormatOptionsWithDialect } from 'sql-formatter';
import { bigquery, formatDialect, trino } from 'sql-formatter';

export const SQL_COLORS: { [name: string]: string } = {
  bracket: '#da70d6',
  comment: '#6a9955',
  function: '#dcdcaa',
  keyword: '#569cd6',
  number: '#6a9955',
  string: '#ce9178',
};

export const SQL_KEYWORDS = [
  'ADD',
  'ADD CONSTRAINT',
  'ALL',
  'ALTER',
  'ALTER COLUMN',
  'ALTER TABLE',
  'AND',
  'ANY',
  'AS',
  'ASC',
  'AUTO_INCREMENT',
  'BACKUP DATABASE',
  'BEGIN',
  'BETWEEN',
  'BINARY',
  'BLOB',
  'BY',
  'CASCADE',
  'CASE',
  'CHAR',
  'CHECK',
  'COLUMN',
  'COMMIT',
  'CONSTRAINT',
  'CREATE',
  'CREATE DATABASE',
  'CREATE INDEX',
  'CREATE OR REPLACE VIEW',
  'CREATE PROCEDURE',
  'CREATE TABLE',
  'CREATE UNIQUE INDEX',
  'CREATE VIEW',
  'CURRENT_DATE',
  'CURRENT_TIME',
  'DATABASE',
  'DECIMAL',
  'DEFAULT',
  'DELETE',
  'DESC',
  'DISTINCT',
  'DROP',
  'DROP COLUMN',
  'DROP CONSTRAINT',
  'DROP DATABASE',
  'DROP DEFAULT',
  'DROP INDEX',
  'DROP TABLE',
  'DROP VIEW',
  'EACH',
  'ELSE',
  'ELSEIF',
  'END',
  'ENGINE',
  'EXEC',
  'EXISTS',
  'FALSE',
  'FOR',
  'FOREIGN KEY',
  'FROM',
  'FULL OUTER JOIN',
  'GROUP',
  'GROUP BY',
  'HAVING',
  'IF',
  'IFNULL',
  'ILIKE',
  'IN',
  'INDEX',
  'INDEX_LIST',
  'INNER JOIN',
  'INSERT',
  'INSERT INTO',
  'INSERT INTO SELECT',
  'INTEGER',
  'INTERVAL',
  'INTO',
  'IS',
  'IS NOT NULL',
  'IS NULL',
  'JOIN',
  'KEY',
  'KEYS',
  'LEADING',
  'LEFT',
  'LEFT JOIN',
  'LIKE',
  'LIMIT',
  'LONGTEXT',
  'MATCH',
  'NOT',
  'NOT NULL',
  'NULL',
  'ON',
  'OPTION',
  'OR',
  'ORDER',
  'ORDER BY',
  'OUT',
  'OUTER',
  'OUTER JOIN',
  'OVERLAPS',
  'PRAGMA',
  'PRIMARY',
  'PRIMARY KEY',
  'PRINT',
  'PROCEDURE',
  'REFERENCES',
  'REPLACE',
  'RETURNING',
  'RIGHT',
  'RIGHT JOIN',
  'ROWNUM',
  'SELECT',
  'SELECT DISTINCT',
  'SELECT INTO',
  'SELECT TOP',
  'SET',
  'SHOW',
  'TABLE',
  'TEXT',
  'THEN',
  'TINYBLOB',
  'TINYINT',
  'TINYTEXT',
  'TO',
  'TOP',
  'TRAILING',
  'TRUE',
  'TRUNCATE TABLE',
  'UNION',
  'UNION ALL',
  'UNIQUE',
  'UNSIGNED',
  'UPDATE',
  'VALUE',
  'VALUES',
  'VARBINARY',
  'VARCHAR',
  'VIEW',
  'WHEN',
  'WHERE',
  'WITH',
];

export const SQL_HIGHLIGHTERS = [
  /(?<number>[+-]?(?:\d+\.\d+|\d+|\.\d+)(?:E[+-]?\d+)?)/,
  /(?<string>'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")/,
  /(?<comment>--[^\n\r]*|#[^\n\r]*|\/\*(?:[^*]|\*(?!\/))*\*\/)/,
  /\b(?<function>\w+)(?=\s*\()/,
  /(?<bracket>[()])/,
  /(?<identifier>\b\w+\b|`(?:[^`\\]|\\.)*`)/,
  /(?<whitespace>\s+)/,
  /(?<special>\^-=|\|\*=|\+=|-=|\*=|\/=|%=|&=|>=|<=|<>|!=|!<|!>|>>|<<|.)/,
];

const charCodeMap: { [code: number]: string } = {
  34: '&quot;',
  38: '&amp;',
  39: '&#39;',
  60: '&lt;',
  62: '&gt;',
};

function escapeHtml(str: string) {
  let html = '';
  let lastIndex = 0;

  for (let i = 0; i < str.length; i++) {
    const replacement = charCodeMap[str.charCodeAt(i)];
    if (!replacement) {
      continue;
    }

    if (lastIndex !== i) {
      html += str.substring(lastIndex, i);
    }

    lastIndex = i + 1;
    html += replacement;
  }

  return html + str.substring(lastIndex);
}

/**
 * Clean a SQL identifier by quoting it if it's a reserved keyword.
 */
export function sqlCleanLine(line: string) {
  if (SQL_KEYWORDS.includes(_.toUpper(line))) {
    return `"${line}"`;
  }
  return line;
}

/**
 * Format SQL code with proper indentation and keyword casing.
 * Supports BigQuery and Trino dialects with dbt template syntax.
 */
export function sqlFormat(sql: string, dialect?: 'bigquery' | 'trino') {
  const options: Partial<FormatOptionsWithDialect> = {
    keywordCase: 'upper',
    tabWidth: 2,
    useTabs: true,
  };
  const stringTypes: DialectOptions['tokenizerOptions']['stringTypes'] = [
    { regex: String.raw`\{\{.*?\}\}` },
    { regex: String.raw`\{%.*?%\}` },
  ];
  switch (dialect) {
    case 'bigquery': {
      return formatDialect(sql, {
        dialect: {
          name: 'dbt',
          formatOptions: { ...bigquery.formatOptions },
          tokenizerOptions: {
            ...bigquery.tokenizerOptions,
            stringTypes: [
              ...bigquery.tokenizerOptions.stringTypes,
              ...stringTypes,
            ],
          },
        },
        ...options,
      });
    }
    default: {
      return formatDialect(sql, {
        dialect: {
          formatOptions: { ...trino.formatOptions },
          name: 'dbt',
          tokenizerOptions: {
            ...trino.tokenizerOptions,
            stringTypes: [
              ...trino.tokenizerOptions.stringTypes,
              ...stringTypes,
            ],
          },
        },
        ...options,
      });
    }
  }
}

/**
 * Convert SQL to syntax-highlighted HTML.
 * Used for displaying SQL in webviews with VS Code colors.
 */
export function sqlToHtml(sql: string) {
  const tokenizer = new RegExp(
    [
      `\\b(?<keyword>${SQL_KEYWORDS.join('|')})\\b`,
      ...SQL_HIGHLIGHTERS.map((regex) => regex.source),
    ].join('|'),
    'gis',
  );

  return `<div>${Array.from(sql.matchAll(tokenizer), (match) => {
    const groups = match.groups;
    if (groups) {
      return {
        name: Object.keys(groups).find((key) => groups[key]),
        content: match[0],
      };
    } else {
      return {
        name: '',
        content: '',
      };
    }
  })
    .map(({ name, content }) => {
      const escapedContent = escapeHtml(content);
      if (!name) {
        return escapedContent;
      }
      const color = SQL_COLORS[name];
      return name === 'whitespace'
        ? escapedContent
        : color
          ? `<span style="color:${color}">${escapedContent}</span>`
          : escapedContent;
    })
    .join('')
    .replace(/\t/g, '&nbsp;&nbsp;')
    .split('\n')
    .join('<br/>')}</div>`;
}
