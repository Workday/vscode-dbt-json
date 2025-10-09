import { assertExhaustive } from '@shared';
import type {
  Api,
  ApiHandler,
  ApiMessage,
  ApiResponse,
} from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import type { DbtProject, DbtProjectManifest } from '@shared/dbt/types';
import { useEnvironment } from '@web/context/environment';
import { TrinoProvider } from '@web/context/trino';
import { Home } from '@web/pages/Home';
import { ModelCreate } from '@web/pages/ModelCreate';
import { QueryView } from '@web/pages/QueryView';
import { SourceCreate } from '@web/pages/SourceCreate';
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from 'react';
import {
  BrowserRouter,
  createBrowserRouter,
  Route,
  Routes,
  RouterProvider,
} from 'react-router';
import { useMount, useUnmount } from 'react-use';
import * as uuid from 'uuid';

type WebRoute = {
  element: React.ReactElement;
  label: string;
  path: string;
  regex: RegExp;
};

type AppValue = {
  api: { post: ApiHandler };
};

const routeConfigs: WebRoute[] = [
  {
    element: <Home />,
    label: 'Home',
    regex: /^\/$/,
    path: '/',
  },
  {
    element: <ModelCreate />,
    label: 'Model Create',
    path: '/model/create',
    regex: /^\/model\/create$/,
  },
  {
    element: <QueryView />,
    label: 'Query View',
    path: '/query/view/:queryId',
    regex: /^\/query\/view\/([0-9]|[a-z]|_)+$/,
  },
  {
    element: <SourceCreate />,
    label: 'Source Create',
    path: '/source/create',
    regex: /^\/source\/create$/,
  },
];

const AppContext = createContext<AppValue | null>(null);

const apiChannels: {
  [id: string]: {
    resolve: (value: Promise<ApiResponse>) => void;
    reject: (err: unknown) => void;
  };
} = {};

export function AppProvider() {
  const { environment, route, vscode } = useEnvironment();
  // const [colorMode, setColorMode] = useColorMode();
  const [ready, setReady] = useState(false);

  const handleApi = useCallback(
    async (payload: ApiMessage): Promise<ApiResponse> => {
      let promise: Promise<ApiResponse>;
      switch (environment) {
        // In coder env, we attach a unique id to the request so we can send the response back to the correct channel
        case 'coder': {
          promise = new Promise<ApiResponse>((resolve, reject) => {
            const _channelId = uuid.v4();
            // We're assigning the resolve and reject functions to the channel obj so we can resolve the promise later
            apiChannels[_channelId] = { resolve, reject };
            const message = { ...payload, _channelId };
            console.log('SENDING MESSAGE TO VSCODE:', message);
            vscode?.postMessage(message);
          });
          break;
        }
        // In web, we are just returning test responses
        case 'web': {
          promise = new Promise((resolve) => {
            // Simulate network delay with setTimeout
            setTimeout(() => {
              const payloadType = payload.type as Api['type'];
              switch (payloadType) {
                case 'dbt-fetch-modified-models': {
                  return resolve(
                    apiResponse<typeof payloadType>(['model_a', 'model_b']),
                  );
                }
                case 'dbt-fetch-projects': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      { name: 'test_project_1' } as DbtProject,
                    ]),
                  );
                }
                case 'dbt-parse-project': {
                  return resolve(
                    apiResponse<typeof payloadType>({} as DbtProjectManifest),
                  );
                }
                case 'dbt-run-model': {
                  return resolve(apiResponse<typeof payloadType>(null));
                }
                case 'dbt-run-model-lineage': {
                  return resolve(apiResponse<typeof payloadType>(null));
                }
                case 'framework-model-create': {
                  return resolve(
                    apiResponse<typeof payloadType>(
                      'Model created successfully',
                    ),
                  );
                }
                case 'framework-source-create': {
                  return resolve(
                    apiResponse<typeof payloadType>(
                      'Source created successfully',
                    ),
                  );
                }
                case 'lightdash-start-preview': {
                  return resolve(
                    apiResponse<typeof payloadType>({
                      url: 'https://www.google.com',
                    }),
                  );
                }
                case 'trino-fetch-catalogs': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      'catalog_1',
                      'catalog_2',
                      'catalog_3',
                      'catalog_4',
                      'catalog_5',
                    ]),
                  );
                }
                case 'trino-fetch-columns': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      {
                        column: 'column_1',
                        type: 'varchar',
                        extra: '',
                        comment: '',
                      },
                      {
                        column: 'column_2',
                        type: 'varchar',
                        extra: '',
                        comment: '',
                      },
                      {
                        column: 'column_3',
                        type: 'varchar',
                        extra: '',
                        comment: '',
                      },
                    ]),
                  );
                }
                case 'trino-fetch-current-schema': {
                  return resolve(apiResponse<typeof payloadType>('my_schema'));
                }
                case 'trino-fetch-etl-sources': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      {
                        source_id: 'source_1',
                        properties: '{}',
                        etl_active: true,
                      },
                      {
                        source_id: 'source_2',
                        properties: '{}',
                        etl_active: false,
                      },
                    ]),
                  );
                }
                case 'trino-fetch-schemas': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      'schema_1',
                      'schema_2',
                      'schema_3',
                      'schema_4',
                      'schema_5',
                    ]),
                  );
                }
                case 'trino-fetch-system-nodes': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      {
                        coordinator: true,
                        http_uri: 'http://localhost:8080',
                        node_id: 'node_1',
                        node_version: 1,
                        state: 'active',
                      },
                      {
                        coordinator: false,
                        http_uri: 'http://localhost:8081',
                        node_id: 'node_2',
                        node_version: 1,
                        state: 'active',
                      },
                    ]),
                  );
                }
                case 'trino-fetch-system-queries': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      {
                        // analysis_time_ms: 0,
                        created: '',
                        end: '',
                        // error_code: '',
                        // error_type: '',
                        // last_heartbeat: '',
                        // planning_time_ms: 0,
                        // queued_time_ms: 0,
                        // query: '',
                        query_id: '',
                        // resource_group_id: [],
                        source: '',
                        started: '',
                        state: 'FINISHED',
                        // user: '',
                      },
                    ]),
                  );
                }
                case 'trino-fetch-system-query-with-task': {
                  return resolve(
                    apiResponse<typeof payloadType>({
                      // analysis_time_ms: 0,
                      created: '2025-01-01T00:00:00Z',
                      end: '2025-01-01T01:00:00Z',
                      // error_code: '',
                      // error_type: '',
                      // last_heartbeat: '',
                      // planning_time_ms: 0,
                      // queued_time_ms: 0,
                      // query: '',
                      query_id: 'abc',
                      // resource_group_id: [],
                      source: '',
                      started: '2025-01-01T00:01:00Z',
                      state: 'FINISHED',
                      // user: '',
                    }),
                  );
                }
                case 'trino-fetch-system-query-sql': {
                  return resolve(
                    apiResponse<typeof payloadType>(`
/* {""app"": ""dbt"", ""dbt_version"": ""1.7.17"", ""profile_name"": ""profile"", ""target_name"": ""default"", ""node_id"": ""model.project.name""} */

select a, b, c
from table
where a = 1      
`),
                  );
                }
                case 'trino-fetch-tables': {
                  return resolve(
                    apiResponse<typeof payloadType>([
                      'table_1',
                      'table_2',
                      'table_3',
                      'table_4',
                      'table_5',
                    ]),
                  );
                }
                case 'state-save': {
                  return resolve(
                    apiResponse<typeof payloadType>({ success: true }),
                  );
                }
                case 'state-load': {
                  return resolve(
                    apiResponse<typeof payloadType>({ data: null }),
                  );
                }
                case 'state-clear': {
                  return resolve(
                    apiResponse<typeof payloadType>({ success: true }),
                  );
                }
                default:
                  return assertExhaustive<ApiResponse>(payloadType);
              }
            }, 1000);
          });
        }
      }

      return await promise;
    },
    [environment, vscode],
  );

  const api = useMemo(() => ({ post: handleApi as ApiHandler }), [handleApi]);

  /** Listen for message to come back from parent iframe and update the channel so api request can resolve */
  const handleMessage = useCallback((event: MessageEvent) => {
    const _channelId = event.data?._channelId;
    if (!_channelId) return;
    const apiChannel = apiChannels[_channelId];
    if (!apiChannel) return;
    const { err, response } = event.data;
    if (err) {
      apiChannel.reject(err);
    } else if (response) {
      apiChannel.resolve(response as Promise<ApiResponse>);
    }
    delete apiChannels[_channelId];
  }, []);

  useMount(() => {
    switch (environment) {
      case 'coder': {
        // Listen for messages from parent iframe
        window.addEventListener('message', handleMessage);
        break;
      }
      case 'web': {
        break;
      }
    }
    setReady(true);
  });

  useUnmount(() => {
    window.removeEventListener('message', handleMessage);
  });

  const value: AppValue = useMemo(
    () => ({ api, environment, vscode }),
    [api, environment, vscode],
  );

  if (!ready) return null;

  return (
    <AppContext.Provider value={value}>
      <TrinoProvider initialValue={{}}>
        <RenderRoute route={route} />
      </TrinoProvider>
    </AppContext.Provider>
  );
}

function RenderRoute({ route }: { route: string | null }) {
  if (route) {
    // Running in extension at specific route
    const routeConfig = routeConfigs.find((r) => r.regex.test(route));
    if (!routeConfig) {
      return <div>404: Route not found</div>;
    }
    // Even though we're running in the extesion, wrap this like a browser route, so we can use react-router hooks
    return (
      <BrowserRouter basename="/">
        <Routes location={{ pathname: route }}>
          <Route path={routeConfig.path} element={routeConfig.element} />
        </Routes>
      </BrowserRouter>
    );
  } else {
    // Running in separate browser
    const router = createBrowserRouter(routeConfigs);
    return <RouterProvider router={router} />;
  }
}

// eslint-disable-next-line react-refresh/only-export-components
export function useApp() {
  const app = useContext(AppContext);
  if (!app) {
    throw new Error('useApp must be used within AppProvider');
  }
  return app;
}
