import {
  SQL_COLORS,
  SQL_KEYWORDS,
  SQL_HIGHLIGHTERS,
} from '@shared/sql/constants';
import * as _ from 'lodash';
import {
  bigquery,
  DialectOptions,
  formatDialect,
  FormatOptionsWithDialect,
  trino,
} from 'sql-formatter';

const charCodeMap: { [code: number]: string } = {
  34: '&quot;', // "
  38: '&amp;', // &
  39: '&#39;', // '
  60: '&lt;', // <
  62: '&gt;', // >
};

function escapeHtml(str: string) {
  let html = '';
  let lastIndex = 0;

  for (let i = 0; i < str.length; i++) {
    const replacement = charCodeMap[str.charCodeAt(i)];
    if (!replacement) continue;

    if (lastIndex !== i) {
      html += str.substring(lastIndex, i);
    }

    lastIndex = i + 1;
    html += replacement;
  }

  return html + str.substring(lastIndex);
}

export function sqlCleanLine(line: string) {
  if (SQL_KEYWORDS.includes(_.toUpper(line))) {
    return `"${line}"`;
  }
  return line;
}

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

export function sqlToHtml(sql: string) {
  // Regex of the shape /(?<token1>...)|(?<token2>...)|.../g
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
      if (!name) return escapedContent;
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
