import { makeClassName } from '@web';
import type { Change } from 'diff';
import { diffLines, structuredPatch } from 'diff';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import json from 'react-syntax-highlighter/dist/esm/languages/hljs/json';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import yaml from 'react-syntax-highlighter/dist/esm/languages/hljs/yaml';
import vs from 'react-syntax-highlighter/dist/esm/styles/hljs/vs';
import vscDarkPlus from 'react-syntax-highlighter/dist/esm/styles/hljs/vs2015';

import { DiffViewMode, type DiffViewProps } from './DiffView.types';

// Register languages
SyntaxHighlighter.registerLanguage('json', json);
SyntaxHighlighter.registerLanguage('sql', sql);
SyntaxHighlighter.registerLanguage('yaml', yaml);

interface LineData {
  content: string;
  type: 'added' | 'removed' | 'normal';
  lineNumber: number;
}

interface Style {
  [key: string]: React.CSSProperties;
}

const styles: Style = {
  dark: { background: 'rgb(30, 30, 30)', color: 'rgb(220, 220, 220)' },
  light: { background: 'white', color: 'black' },
};

// Helper function to get background color for diff lines
const getDiffBackgroundColor = (
  type: 'added' | 'removed' | 'normal' | 'header',
  theme: 'light' | 'dark',
): string => {
  if (type === 'added') {
    return theme === 'dark' ? 'rgba(34, 197, 94, 0.2)' : '#e6ffe6';
  }
  if (type === 'removed') {
    return theme === 'dark' ? 'rgba(239, 68, 68, 0.2)' : '#ffe6e6';
  }
  if (type === 'header') {
    return theme === 'dark' ? 'rgba(59, 130, 246, 0.2)' : '#dbeafe';
  }
  return 'transparent';
};

export function DiffView({
  original,
  modified,
  language,
  theme,
  viewMode = DiffViewMode.SPLIT,
  className = '',
}: DiffViewProps) {
  const style: Style = theme === 'dark' ? vscDarkPlus : vs;

  if (viewMode === DiffViewMode.UNIFIED) {
    return (
      <DiffViewUnified
        original={original}
        modified={modified}
        language={language}
        theme={theme}
        style={style}
        className={className}
      />
    );
  } else {
    return (
      <DiffViewSplit
        original={original}
        modified={modified}
        language={language}
        theme={theme}
        style={style}
        className={className}
      />
    );
  }
}

const DiffViewUnified = ({
  original,
  modified,
  language,
  theme,
  className = '',
  style,
}: {
  original: string;
  modified: string;
  language: 'json' | 'yaml' | 'sql';
  theme: 'light' | 'dark';
  className?: string;
  style: Style;
}) => {
  // Use structuredPatch to get parsed hunks with line number information
  const patch = structuredPatch(
    'original',
    'modified',
    original,
    modified,
    '',
    '',
    { context: 3 }, // Number of context lines around changes
  );

  // Build unified diff lines with dual line numbers
  const diffLines: string[] = [];
  const lineTypes: Array<'added' | 'removed' | 'normal' | 'header'> = [];
  const dualLineNumbers: Array<{ old: string; new: string }> = [];

  // Check if there are any changes
  if (patch.hunks.length === 0) {
    // No changes - show the original content as context
    const lines = modified.split('\n');
    lines.forEach((line, index) => {
      diffLines.push(` ${line}`); // Leading space indicates context line
      lineTypes.push('normal');
      const lineNum = String(index + 1);
      dualLineNumbers.push({ old: lineNum, new: lineNum });
    });
  } else {
    // Has changes - build diff from hunks
    patch.hunks.forEach((hunk) => {
      // Add hunk header
      const header = `@@ -${hunk.oldStart},${hunk.oldLines} +${hunk.newStart},${hunk.newLines} @@`;
      diffLines.push(header);
      lineTypes.push('header');
      dualLineNumbers.push({ old: '', new: '' });

      let oldLine = hunk.oldStart;
      let newLine = hunk.newStart;

      hunk.lines.forEach((line) => {
        diffLines.push(line);

        if (line.startsWith('+')) {
          // Added line - only show new line number
          lineTypes.push('added');
          dualLineNumbers.push({ old: '', new: String(newLine) });
          newLine++;
        } else if (line.startsWith('-')) {
          // Removed line - only show old line number
          lineTypes.push('removed');
          dualLineNumbers.push({ old: String(oldLine), new: '' });
          oldLine++;
        } else {
          // Context line (unchanged) - show both line numbers
          lineTypes.push('normal');
          dualLineNumbers.push({ old: String(oldLine), new: String(newLine) });
          oldLine++;
          newLine++;
        }
      });
    });
  }

  const unifiedCode = diffLines.join('\n');

  const lineNumberColor = theme === 'dark' ? '#6b7280' : '#9ca3af';
  const headerColor = theme === 'dark' ? '#60a5fa' : '#3b82f6';
  const bgColor = theme === 'dark' ? '#1e1e1e' : '#ffffff';

  return (
    <div
      className={makeClassName(
        'diff-view-unified grid grid-cols-10',
        className,
      )}
    >
      {/* Custom dual line number column */}
      <div
        className="col-span-1 select-none font-mono text-xs border-r"
        style={{
          backgroundColor: bgColor,
          borderColor: theme === 'dark' ? '#374151' : '#d1d5db',
          minWidth: '8em',
        }}
      >
        {dualLineNumbers.map((lineNum, index) => {
          const type = lineTypes[index];
          const isHeader = type === 'header';

          return (
            <div
              key={index}
              className="flex items-center px-2"
              style={{
                backgroundColor: getDiffBackgroundColor(type, theme),
                color: isHeader ? headerColor : lineNumberColor,
                minHeight: '1.5rem',
                lineHeight: '1.5rem',
              }}
            >
              {isHeader ? (
                <span className="w-full text-center font-bold">...</span>
              ) : (
                <>
                  <span className="w-10 text-right">{lineNum.old || ''}</span>
                  <span className="mx-1">|</span>
                  <span className="w-10 text-right">{lineNum.new || ''}</span>
                  <span className="ml-2">
                    {type === 'added' ? '+' : type === 'removed' ? '-' : ' '}
                  </span>
                </>
              )}
            </div>
          );
        })}
      </div>

      {/* Code content */}
      <div className="col-span-9">
        <SyntaxHighlighter
          language={language}
          style={style}
          showLineNumbers={true}
          wrapLines={true}
          lineNumberStyle={{ display: 'none' }}
          lineProps={(lineNumber: number) => {
            const type = lineTypes[lineNumber - 1];
            if (!type) return {};

            return {
              style: {
                background: getDiffBackgroundColor(type, theme),
                display: 'block',
                fontWeight: type === 'header' ? 'bold' : 'normal',
                minHeight: '1.5rem',
                lineHeight: '1.5rem',
              },
            };
          }}
          customStyle={{
            margin: 0,
            padding: '0',
            backgroundColor: 'transparent',
          }}
        >
          {unifiedCode}
        </SyntaxHighlighter>
      </div>
    </div>
  );
};

const DiffViewSplit = ({
  original,
  modified,
  language,
  theme,
  className = '',
  style,
}: {
  original: string;
  modified: string;
  language: 'json' | 'yaml' | 'sql';
  theme: 'light' | 'dark';
  className?: string;
  style: Style;
}) => {
  // Calculate diff using diffLines for split view
  const diff = diffLines(original, modified);

  // Split view (side-by-side)
  const originalLines: LineData[] = [];
  const modifiedLines: LineData[] = [];
  let originalLineNum = 1;
  let modifiedLineNum = 1;

  diff.forEach((part: Change) => {
    const lines = part.value.split('\n');
    // Remove last empty line if it exists
    if (lines[lines.length - 1] === '') {
      lines.pop();
    }

    if (part.added) {
      lines.forEach((line) => {
        modifiedLines.push({
          content: line,
          type: 'added',
          lineNumber: modifiedLineNum++,
        });
      });
    } else if (part.removed) {
      lines.forEach((line) => {
        originalLines.push({
          content: line,
          type: 'removed',
          lineNumber: originalLineNum++,
        });
      });
    } else {
      lines.forEach((line) => {
        originalLines.push({
          content: line,
          type: 'normal',
          lineNumber: originalLineNum++,
        });
        modifiedLines.push({
          content: line,
          type: 'normal',
          lineNumber: modifiedLineNum++,
        });
      });
    }
  });

  // Pad arrays to same length for side-by-side display
  const maxLength = Math.max(originalLines.length, modifiedLines.length);
  while (originalLines.length < maxLength) {
    originalLines.push({ content: '', type: 'normal', lineNumber: 0 });
  }
  while (modifiedLines.length < maxLength) {
    modifiedLines.push({ content: '', type: 'normal', lineNumber: 0 });
  }

  // Reconstruct full code strings for syntax highlighting
  const originalCode = originalLines.map((l) => l.content).join('\n');
  const modifiedCode = modifiedLines.map((l) => l.content).join('\n');

  return (
    <div className={makeClassName('diff-view-split', className)}>
      <div className="diff-view-split-header">
        <h3 className="text-sm font-semibold p-2" style={styles[theme]}>
          Original
        </h3>
        <h3 className="text-sm font-semibold p-2" style={styles[theme]}>
          Modified
        </h3>
      </div>
      <div className="diff-view-split-content">
        {/* Original (Left) */}
        <SyntaxHighlighter
          className="h-full"
          language={language}
          style={style}
          showLineNumbers={true}
          wrapLines={true}
          lineProps={(lineNumber: number | boolean) => {
            if (typeof lineNumber !== 'number') return {};
            const lineData = originalLines[lineNumber - 1];
            if (!lineData) return {};

            return {
              style: {
                background: getDiffBackgroundColor(lineData.type, theme),
                display: 'block',
              },
            };
          }}
        >
          {originalCode}
        </SyntaxHighlighter>
        {/* Modified (Right) */}
        <SyntaxHighlighter
          className="h-full"
          language={language}
          style={style}
          showLineNumbers={true}
          wrapLines={true}
          lineProps={(lineNumber: number | boolean) => {
            if (typeof lineNumber !== 'number') return {};
            const lineData = modifiedLines[lineNumber - 1];
            if (!lineData) return {};

            return {
              style: {
                background: getDiffBackgroundColor(lineData.type, theme),
                display: 'block',
              },
            };
          }}
        >
          {modifiedCode}
        </SyntaxHighlighter>
      </div>
    </div>
  );
};
