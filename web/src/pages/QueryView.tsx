import { sqlFormat, sqlToHtml } from '@shared/sql/utils';
import type { TrinoSystemQuery, TrinoSystemTask } from '@shared/trino/types';
import { useApp } from '@web/context';
import { Alert, Box, Progress, Spinner, Text } from '@web/elements';
import Tooltip from '@web/elements/Tooltip';
import { useError, useMount, useUnmount } from '@web/hooks';
import DOMPurify from 'dompurify';
import parse from 'html-react-parser';
import React, { useState } from 'react';
import { useParams } from 'react-router';

import { getQueryErrorMessage } from '../utils/errors';

export type QueryViewParams = {
  queryId: string;
};

function Value({
  children,
  label,
  type,
  tooltipText,
}: {
  children?: string | React.ReactNode | null;
  label: string;
  type?: 'datetime';
  tooltipText?: string;
}) {
  const labelContent = (
    <div className="flex gap-1 items-center py-1">
      <Text variant="label">{label}</Text>
      {tooltipText && <Tooltip content={tooltipText} />}
    </div>
  );

  switch (type) {
    case 'datetime':
      return (
        <Box variant="padded">
          {labelContent}
          <Box variant="bordered">
            {(children && new Date(children as string).toLocaleString()) || ''}
          </Box>
        </Box>
      );
    default:
      return (
        <Box variant="padded">
          {labelContent}
          <Box
            variant="bordered"
            style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}
          >
            {children || ''}
          </Box>
        </Box>
      );
  }
}

export function QueryView() {
  const { api } = useApp();
  const { error, handleError } = useError();
  const { queryId } = useParams<QueryViewParams>();

  const [loading, setLoading] = useState(true);

  const [query, setQuery] = useState<
    (TrinoSystemQuery & Partial<TrinoSystemTask>) | null
  >(null);
  const [querySql, setQuerySql] = useState<string | null>(null);

  const [timeoutQuery, setTimeoutQuery] = useState<NodeJS.Timeout | null>(null);

  async function handleQuery() {
    if (!queryId) return;
    try {
      const _query = await api.post({
        type: 'trino-fetch-system-query-with-task',
        request: { id: queryId },
      });
      setQuery(_query);
    } catch (err) {
      handleError(err);
    }
    const _timeoutQuery = setTimeout(() => void handleQuery(), 5000);
    setTimeoutQuery(_timeoutQuery);
  }

  async function handleQuerySql() {
    if (!queryId) return;
    try {
      let _querySql = await api.post({
        type: 'trino-fetch-system-query-sql',
        request: { id: queryId },
      });
      try {
        _querySql = sqlFormat(_querySql);
        _querySql = DOMPurify.sanitize(_querySql);
      } catch (err) {
        // Fail this silently
        console.error('ERROR FORMATTING QUERY SQL', err);
      }
      setQuerySql(_querySql);
    } catch (err) {
      handleError(err);
    }
  }

  useMount(() => {
    const initSystemQuery = async () => {
      if (!queryId) return;
      try {
        // Handle initial query status, will set timeout on response
        await handleQuery();
        // Handle initial query sql
        await handleQuerySql();
        setLoading(false);
      } catch (err) {
        console.error('ERROR FETCHING SYSTEM QUERY', err);
      }
    };

    void initSystemQuery();
  });

  useUnmount(() => {
    if (timeoutQuery) clearTimeout(timeoutQuery);
  });

  if (error) {
    return (
      <Alert
        label={getQueryErrorMessage(error.message).label}
        description={getQueryErrorMessage(error.message)?.description}
        variant="error"
      />
    );
  }

  if (loading || !query || !querySql)
    return (
      <Box style={{ padding: 3 }}>
        <Spinner />
        <Text>Loading...</Text>
      </Box>
    );

  return (
    <Box variant="padded">
      {query.state === 'QUEUED' ? (
        <Progress label="Query Progress" percent={10} />
      ) : query.state === 'RUNNING' && query.splits ? (
        <Progress
          caption={`${query.completed_splits || 0} / ${query.splits} Splits Completed`}
          label="Query Progress"
          percent={(10 + 90 * (query.completed_splits || 0)) / query.splits}
        />
      ) : (
        <Progress label="Query Progress" percent={100} />
      )}
      <Value
        label="Query ID"
        tooltipText="Unique identifier for this Trino query. Use this ID to reference the query in logs or other Trino operations."
      >
        {query.query_id}
      </Value>
      <Value
        label="Status"
        tooltipText="Current execution state of the query (QUEUED, RUNNING, FINISHED, FAILED). Shows the query's progress through the execution pipeline."
      >
        {query.state}
      </Value>
      <Value
        label="Created"
        type="datetime"
        tooltipText="Timestamp when the query was initially submitted to Trino for execution."
      >
        {query.created}
      </Value>
      <Value
        label="Start"
        type="datetime"
        tooltipText="Timestamp when Trino began actively executing the query (after any queuing period)."
      >
        {query.started}
      </Value>
      <Value
        label="End"
        type="datetime"
        tooltipText="Timestamp when the query execution completed, either successfully or with an error."
      >
        {query.end}
      </Value>
      <Value
        label="Query SQL"
        tooltipText="The formatted SQL statement that was executed. This shows the actual query sent to Trino."
      >
        {parse(sqlToHtml(querySql))}
      </Value>
    </Box>
  );
}
