import { assertExhaustive } from '@shared';
import { Dbt } from '@services/dbt';
import { Framework } from '@services/framework';
import { Lightdash } from '@services/lightdash';
import { Trino } from '@services/trino';
import { State } from '@services/webviewcontroller';
import { ApiHandler } from '@shared/api/types';

export class Api {
  dbt: Dbt;
  handleApi: ApiHandler;
  framework: Framework;
  lightdash: Lightdash;
  trino: Trino;
  state: State;

  constructor({
    dbt,
    framework,
    lightdash,
    trino,
    state,
  }: {
    dbt: Dbt;
    framework: Framework;
    lightdash: Lightdash;
    trino: Trino;
    state: State;
  }) {
    this.dbt = dbt;
    this.framework = framework;
    this.lightdash = lightdash;
    this.trino = trino;
    this.state = state;

    this.handleApi = async (payload) => {
      switch (payload.type) {
        case 'dbt-fetch-modified-models':
        case 'dbt-fetch-projects':
        case 'dbt-parse-project':
        case 'dbt-run-model':
        case 'dbt-run-model-lineage':
          return await this.dbt.handleApi(payload);
        case 'framework-model-create':
        case 'framework-source-create':
          return await this.framework.handleApi(payload);
        case 'lightdash-start-preview':
          return await this.lightdash.handleApi(payload);
        case 'trino-fetch-catalogs':
        case 'trino-fetch-columns':
        case 'trino-fetch-current-schema':
        case 'trino-fetch-etl-sources':
        case 'trino-fetch-schemas':
        case 'trino-fetch-system-nodes':
        case 'trino-fetch-system-queries':
        case 'trino-fetch-system-query-with-task':
        case 'trino-fetch-system-query-sql':
        case 'trino-fetch-tables':
          return await this.trino.handleApi(payload);
        case 'state-load':
        case 'state-save':
        case 'state-clear':
          return await this.state.handleApi(payload);
        default:
          return assertExhaustive<any>(payload);
      }
    };
  }

  deactivate() {}
}
