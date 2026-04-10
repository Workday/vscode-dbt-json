# AGENTS.md

This file provides guidance to AI coding agents working with code in this repository.

## Project Overview

DJ (dbt-json) Framework is a VS Code extension that revolutionizes dbt development through a structured, JSON-first approach. Users define dbt models and sources as validated `.model.json` and `.source.json` files that automatically generate corresponding SQL and YAML configurations.

The extension provides a rich visual UI built with React, including interactive model and column lineage graphs, a visual model creation wizard, query result previews, and a data modeling canvas -- all rendered as VS Code webviews.

**Key Technologies:**

- TypeScript extension backend running in Node.js/VS Code extension host
- React 18 + Vite frontend for webviews (interactive lineage graphs, model wizards, data explorer)
- dbt Core integration with manifest.json parsing
- Trino CLI integration for data catalog browsing and query execution
- Lightdash CLI integration for BI dashboards

## Setup Commands

```bash
# Install dependencies
npm install

# Start all watchers (recommended for active development)
npm run dev

# Watch specific components
npm run watch:esbuild      # Extension JS bundle
npm run watch:tsc          # Extension TypeScript
npm run watch:web-build    # Web assets (Vite build)
npm run watch:web-tsc      # Web TypeScript
npm run watch:test         # Tests in watch mode
```

## Testing

```bash
npm test                   # Run all tests
npm run watch:test         # Watch mode
npm run fixtures:update    # Update test fixtures
```

## Building

```bash
npm run compile            # Compile extension TypeScript
npm run compile:web        # Build web assets (Vite)
npm run vscode:prepublish  # Full production build with linting
npm run package            # Create .vsix package
```

## Linting & Formatting

```bash
npm run lint               # Extension only
npm run lint:web           # Web only
npm run lint:all           # Both extension and web
npm run lint:fix:all       # Fix both

npm run format             # Format all files (Prettier)
npm run format:check       # Check formatting
```

## Schema Management

```bash
npm run schema             # Regenerate TypeScript types from JSON schemas
npm run update-test-schemas
```

## Running the Extension

### Method 1: Extension Development Host (F5)

1. Open project in VS Code
2. Press `F5` to launch Extension Development Host
3. Open a dbt project in the new window
4. Make changes and press `Cmd+Shift+F5` to reload

### Method 2: Install VSIX

```bash
npm run package
code --install-extension dj-framework-*.vsix
```

## Architecture

### Dual-Architecture System

**Extension Backend (`src/`)** — TypeScript running in VS Code extension host:

- Entry point: `src/extension.ts` activates the `Coder` service
- Core orchestrator: `src/services/coder/index.ts` wires up all services
- Uses **ServiceLocator pattern** for lazy dependency injection to avoid circular dependencies

**Web Frontend (`web/`)** — React app rendered in VS Code webviews:

- Built with Vite, outputs to `dist/web`
- Pages: ModelCreate, SourceCreate, Home, QueryView, ModelRun, ModelTest, LightdashPreviewManager
- Lineage views: DataExplorer (model lineage), ColumnLineage, ModelLineage
- Message-based RPC communication with extension host
- State management with Zustand stores

**Shared Code (`src/shared/`)** — Cross-environment utilities used by both backend and frontend

### Service Architecture

Services follow a **layered architecture**:

```text
VS Code Commands & Views (UI Layer)
         ↓
Api Router (Message Bus) - src/services/api.ts
         ↓
Domain Services:
  • Framework (src/services/framework/index.ts) - JSON↔SQL/YAML sync
  • Dbt (src/services/dbt.ts) - manifest parsing, tree views
  • Trino (src/services/trino.ts) - query execution, catalog browsing
  • DataExplorer (src/services/dataExplorer.ts) - model lineage graph
  • ColumnLineage (src/services/columnLineage.ts) - column-level lineage
  • ModelLineage (src/services/modelLineage.ts) - model-level lineage
  • Lightdash (src/services/lightdash/index.ts) - BI integration
         ↓
Shared Utilities & Types (src/shared/)
```

**ServiceLocator Pattern** (`src/services/ServiceLocator.ts`):

- Breaks circular dependencies between services
- Lazy instantiation on first access via factory registration
- Type-safe access via `SERVICE_NAMES` constants
- Example:

```typescript
locator.register('dbt', () => new Dbt(locator.get('logger')));
const dbt = locator.get<Dbt>(SERVICE_NAMES.Dbt);
```

**Handler Pattern**:

- Large services split into specialized handlers
- Framework service uses handlers in `src/services/framework/handlers/`: UIHandlers, ModelDataHandlers, ModelCrudHandlers, ColumnLineageHandlers, SourceHandlers, PreferencesHandlers

### The Core Framework: JSON to SQL/YAML Sync

**Flow:**

```text
.model.json (user-editable, JSON Schema validated)
    ↓
Framework.handleJsonSync() triggers after debounce (configurable via dj.syncDebounceMs)
    ↓
SyncEngine orchestration (src/services/sync/SyncEngine.ts):
  1. Discovery - scan all .model.json files
  2. Dependency Resolution - build dependency graph (dependencyGraph.ts)
  3. Validation - validate against schemas (ValidationService.ts)
  4. Processing - ModelProcessor & SourceProcessor in dependency order
  5. Execution - generate and write SQL/YAML files (fileOperations.ts)
    ↓
.sql + .yml files (auto-generated, read-only)
```

**Key Points:**

- **JSON Schema Validation** using Ajv with 93 schemas in `schemas/`
- **Hash-based caching** (`cacheManager.ts`) to skip unchanged files
- **Batch updates** to prevent VS Code API overload
- **Rename detection** (`RenameHandler.ts`) tracks old→new file pairs
- **Manifest management** (`ManifestManager.ts`) for dbt manifest parsing
- **Sync queue** (`SyncQueue.ts`) for ordered processing

### 11 Model Types

Each model has a `type` field validated by JSON schema:

**Staging** (3 types):

- `stg_select_source` — Select from Trino source tables
- `stg_union_sources` — Union multiple sources
- `stg_select_model` — Transform staging data

**Intermediate** (6 types):

- `int_select_model` — Aggregations and business logic
- `int_join_models` — Join multiple models
- `int_join_column` — Unnest arrays/flatten complex data
- `int_union_models` — Union processed models
- `int_lookback_model` — Trailing time window analysis
- `int_rollup_model` — Roll up to higher time intervals

**Mart** (2 types):

- `mart_select_model` — Analytics-ready datasets
- `mart_join_models` — Comprehensive 360-degree views

Each type has its own schema file: `schemas/model.type.*.schema.json`

### dbt Integration

**Dbt Service** (`src/services/dbt.ts`):

- Discovers dbt projects by scanning for `dbt_project.yml`
- Parses `target/manifest.json` to build in-memory maps:
  - `models: Map<string, DbtModel>` — all dbt models with dependencies
  - `sources: Map<string, DbtSource>` — all dbt sources
  - `macros: Map<string, DbtMacro>` — all dbt macros
  - `projects: Map<string, DbtProject>` — all dbt projects
- Executes dbt commands via Python virtual environment (configured in `dj.pythonVenvPath`)
- Provides tree view data for VS Code sidebar

**Python Integration:**

```typescript
const env = buildProcessEnv({ venv: '.venv' }); // Uses configured venv
spawn('dbt', ['compile', '--select', model], { env });
```

### Trino Integration

**Trino Service** (`src/services/trino.ts`):

- Executes SQL queries via Trino CLI subprocess
- Browses catalogs/schemas/tables/columns for source creation
- Provides query results to Data Explorer
- Configured via environment variables: `TRINO_HOST`, `TRINO_PORT`, `TRINO_USERNAME`, `TRINO_CATALOG`, `TRINO_SCHEMA`
- Falls back to VS Code setting `dj.trinoPath`

### Webview Communication

**Message-based RPC** between React webviews and extension host:

```typescript
// Webview sends (web/src/):
api.send({ type: 'framework-model-create', request: { projectName, type, ... } });

// Extension routes (src/services/api.ts):
switch (payload.type) {
  case 'framework-model-create': return framework.handleApi(payload);
  case 'dbt-run-model': return dbt.handleApi(payload);
  // ... routes to appropriate service
}
```

All API message types defined in `src/shared/api/types.ts` with full TypeScript safety.

### Column Lineage Engine

**ColumnLineageService** (`src/services/columnLineage.ts`):

- Traces column origins and transformations across the DAG
- Analyzes SQL expressions to classify transformations:
  - **passthrough**: `{ expr: "customer_id" }`
  - **renamed**: `{ expr: "id", name: "customer_id" }`
  - **derived**: `{ expr: "amount * quantity" }`
  - **aggregated**: `{ expr: "SUM(revenue)" }`
- Expands bulk selects like `dims_from_model`, `all_from_model`
- Builds DAG rendered in webview using React Flow

### Data Explorer / Model Lineage

**ModelLineage Service** (`src/services/modelLineage.ts`):

- Model-level dependency visualization
- Shows upstream/downstream traversal
- Uses manifest.json's dependency graph
- Renders with Dagre layout algorithm + @xyflow/react
- Layer visualization by model type (staging/intermediate/mart)

## Project Structure

```text
vscode-dbt-json/
├── src/
│   ├── extension.ts              # Entry point - activates Coder service
│   ├── admin.ts                  # Platform utilities (paths, process runners)
│   ├── services/
│   │   ├── coder/                # Main orchestrator (ServiceLocator, commands, views)
│   │   │   ├── index.ts
│   │   │   └── types.ts
│   │   ├── api.ts                # Message router between webviews and services
│   │   ├── ServiceLocator.ts     # ServiceLocator for dependency injection
│   │   ├── framework/            # JSON↔SQL/YAML sync engine
│   │   │   ├── index.ts
│   │   │   ├── FrameworkState.ts
│   │   │   ├── handlers/         # UIHandlers, ModelCrudHandlers, etc.
│   │   │   └── utils/            # column-utils, model-utils, source-utils, sql-utils
│   │   ├── sync/                 # Sync engine components
│   │   │   ├── SyncEngine.ts
│   │   │   ├── ModelProcessor.ts
│   │   │   ├── SourceProcessor.ts
│   │   │   ├── ManifestManager.ts
│   │   │   ├── ValidationService.ts
│   │   │   ├── dependencyGraph.ts
│   │   │   ├── cacheManager.ts
│   │   │   ├── fileOperations.ts
│   │   │   ├── RenameHandler.ts
│   │   │   └── SyncQueue.ts
│   │   ├── dbt.ts                # dbt integration and manifest parsing
│   │   ├── trino.ts              # Trino CLI integration
│   │   ├── dataExplorer.ts       # Model lineage visualization
│   │   ├── modelLineage.ts       # Model lineage graph
│   │   ├── columnLineage.ts      # Column-level lineage
│   │   ├── lightdash/            # Lightdash BI integration
│   │   ├── agent/                # AI agent support (prompts, utils, decorators)
│   │   ├── webview/              # Webview providers and utilities
│   │   ├── modelEdit.ts          # Model editing service
│   │   ├── config.ts             # Configuration service
│   │   ├── djLogger.ts           # Logging service
│   │   ├── statemanager.ts       # State management
│   │   ├── validationErrors.ts   # Custom validation functions
│   │   ├── constants.ts          # Command IDs and constants
│   │   └── utils/                # fileNavigation, fileSystem, git, process, sql
│   └── shared/                   # Cross-environment utilities
│       ├── api/                  # API message types and routing
│       ├── dbt/                  # dbt types and utilities
│       ├── framework/            # Framework types
│       ├── trino/                # Trino types and constants
│       ├── lightdash/            # Lightdash types
│       ├── modellineage/         # Model lineage types
│       ├── lineage/              # Lineage shared types
│       ├── dataexplorer/         # Data explorer types
│       ├── schema/types/         # Generated from schemas/ via `npm run schema`
│       ├── state/                # State types and constants
│       ├── types/                # Common types and config
│       ├── sql/                  # SQL utilities
│       ├── web/                  # Web constants
│       └── webview/              # Webview types
├── web/                          # React webview frontend (Vite)
│   ├── src/
│   │   ├── pages/
│   │   │   ├── ModelCreate.tsx
│   │   │   ├── SourceCreate.tsx
│   │   │   ├── Home.tsx
│   │   │   ├── QueryView.tsx
│   │   │   ├── DataExplorer/
│   │   │   ├── ModelLineage/
│   │   │   ├── ModelRun/
│   │   │   ├── ModelTest/
│   │   │   ├── ColumnLineage/
│   │   │   └── LightdashPreviewManager/
│   │   ├── features/             # DataModeling, Lineage, ModelWizard, Tutorial
│   │   ├── context/              # React contexts (Environment, App, Trino)
│   │   ├── elements/             # Reusable React components
│   │   ├── forms/                # Form components
│   │   ├── hooks/                # Custom React hooks
│   │   ├── stores/               # Zustand stores
│   │   ├── utils/                # Utility functions
│   │   └── styles/               # CSS styles
│   └── vite.config.ts            # Builds to ../dist/web
├── schemas/                      # JSON Schema definitions (93 files)
│   ├── model.schema.json         # Root schema (anyOf 11 model types)
│   ├── model.type.*.schema.json  # 11 model type schemas
│   ├── source.schema.json        # Source definition schema
│   └── column.*.schema.json      # Column properties schemas
├── macros/                       # dbt macros shipped with extension
├── airflow/                      # Airflow DAG templates
│   ├── v2_7/
│   └── v2_10/
├── templates/                    # Code templates
├── tests/                        # Test files and fixtures
│   ├── fixtures/                 # Test data (manifest + model files)
│   ├── update-fixtures.ts
│   └── update-test-schemas.ts
├── docs/                         # Documentation
│   ├── models/                   # 11 model type docs
│   ├── setup/                    # Setup guides (Trino, Lightdash)
│   └── integrations/             # Integration guides
├── .github/                      # GitHub workflows and actions
├── dist/                         # Build output
│   ├── extension/
│   └── web/
├── CONTRIBUTING.md
├── DEVELOPMENT_SETUP.md
├── CODE_OF_CONDUCT.md
├── CHANGELOG.md
├── ROADMAP.md
└── Makefile
```

## Code Style and Conventions

### Import Aliases

**Extension Code** (tsconfig.json):

```typescript
import { log } from 'admin'; // src/admin.ts
import { Dbt } from '@services/dbt'; // src/services/dbt.ts
import { ApiMessage } from '@shared/api/types'; // src/shared/api/types.ts
import { parseModel } from '@shared/framework'; // src/shared/framework/index.ts
```

**Web Code** (web/tsconfig.json):

```typescript
import { useEnvironment } from '@web/context/environment';
import { Button } from '@web/elements/Button';
import { useModelStore } from '@web/stores/useModelStore';
```

### TypeScript Configuration

- **Target**: ES2023
- **Module**: CommonJS for extension, ESNext for web
- **Strict mode**: enabled
- Paths configured for import aliases (see above)

### Test Configuration

**Jest Configuration** (jest.config.js):

- Preset: `ts-jest`
- Supports same path aliases as tsconfig.json via `moduleNameMapper`
- Test files: `**/*.test.ts`
- Ignores: `out/`, `dist/`, `.venv/`

### Commit Message Format

```text
type(scope): description
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
Scopes: `extension`, `web`, `macros`, `schemas`, `scripts`

### Changelog

When adding features or making notable changes, update `CHANGELOG.md`:

- Add a concise bullet under the current top-level version heading (e.g., `## 1.1.0`)
- Use the same style as existing entries — short, descriptive, no prefix labels
- Group related changes into a single bullet when possible
- Do not add date suffixes or create new version headings unless explicitly asked

### Naming Conventions

- Classes: `PascalCase`
- Methods/variables: `camelCase`
- Constants: `UPPER_SNAKE_CASE`
- Types/interfaces: `PascalCase`
- Command IDs: `dj.command.*` via `COMMAND_ID` constants in `src/services/constants.ts`

## Key VS Code Integration Points

### Commands (28 registered in package.json)

Important commands:

- `dj.command.jsonSync` — Regenerate SQL/YAML from JSON
- `dj.command.modelCreate` — Open model creation wizard
- `dj.command.modelEdit` — Edit model (opens draft)
- `dj.command.modelClone` — Clone existing model
- `dj.command.modelCompile` — Compile with dbt
- `dj.command.modelRun` — Run model with dbt
- `dj.command.modelPreview` — Preview model results
- `dj.command.modelLineage` — Open Data Explorer
- `dj.command.columnLineage` — Open Column Lineage panel
- `dj.command.lightdashPreview` — Start Lightdash preview
- `dj.command.sourceCreate` — Create source from Trino catalog
- `dj.command.refreshProjects` — Refresh dbt projects
- `dj.command.clearSyncCache` — Clear JSON sync cache
- `dj.command.testTrinoConnection` — Test Trino connection

### Keybindings

- `Cmd+Shift+C` — Compile model
- `Cmd+Shift+J` — Jump to JSON
- `Cmd+Shift+M` — Jump to Model SQL
- `Cmd+Shift+Y` — Jump to YAML
- `Cmd+Shift+L` — Open Data Explorer
- `Cmd+Enter` — Preview model (when editor focused and DJ active)
- `Cmd+'` — Open target compiled SQL (when editor focused and DJ active)

### Views

**Activity Bar (sidebar):**

- Project Navigator — dbt project tree
- Selected Resource — current model/source details
- Query Engine — Trino connection and query history
- Actions — quick actions (create model/source)
- Edit Drafts — in-progress model edits

**Panel (bottom):**

- Data Explorer — model lineage graph (webview)
- Column Lineage — column-level lineage (webview)

### File Watchers

Extension watches for changes to:

- `.model.json` — triggers JSON sync
- `.source.json` — triggers JSON sync
- `manifest.json` — triggers manifest reload
- `.sql` / `.yml` — for related updates

## Common Workflows

### Creating a Model

1. User clicks "Create Model" in Actions view
2. Extension opens webview with ModelCreate React component
3. User fills form (project, type, group, topic, name)
4. Webview sends `framework-model-create` API message
5. Framework validates, generates `.model.json`
6. JSON sync triggers and generates `.sql` and `.yml`
7. Extension opens generated files in editor

### JSON Sync Process

1. User edits `.model.json` file
2. FileSystemWatcher detects change
3. Framework adds to `SyncQueue`
4. Debounce timer (configurable via `dj.syncDebounceMs`, default 1500ms) triggers sync
5. SyncEngine:
   - Discovers dependencies from manifest
   - Validates JSON against schema
   - Generates SQL/YAML via processors
   - Writes files in batches
6. VS Code refreshes file tree

### Column Lineage Trace

1. User opens `.model.json`, clicks "Column Lineage"
2. Extension opens Column Lineage panel (webview)
3. ColumnLineageService:
   - Parses model JSON
   - Expands bulk selects (`dims_from_model`)
   - Analyzes expressions (passthrough vs derived)
   - Recursively traces upstream columns
   - Builds DAG
4. Webview renders graph with React Flow
5. User clicks column to navigate to definition

## Development Tips

### Debugging

- **Extension logs**: View → Output → "DJ"
- **Log level**: configurable via `dj.logLevel` setting (debug, info, warn, error)
- **Reload extension**: `Cmd+Shift+F5` in Extension Development Host
- **Webview debugging**: Developer → Open Webview Developer Tools
- **Full reload**: Developer → Reload Window

### Working with Services

1. Services are lazily initialized via ServiceLocator (`src/services/ServiceLocator.ts`)
2. Use `SERVICE_NAMES` constants for type-safe service access
3. Be careful with circular dependencies — use ServiceLocator to break cycles
4. Handler pattern preferred for large services (see Framework service `handlers/` directory)
5. All service API messages must be typed in `src/shared/api/types.ts`

### Working with Schemas

To add/modify JSON schemas:

1. Edit schema files in `schemas/`
2. Run `npm run schema` to regenerate TypeScript types
3. Types appear in `src/shared/schema/types/`
4. Update validators in Framework service if needed

### Working with Webviews

1. React code in `web/src/pages/`
2. Use `api.send()` to communicate with extension host
3. Define message types in `src/shared/api/types.ts`
4. Handle messages in appropriate service (via Api router)
5. State management with Zustand stores in `web/src/stores/`
6. Reusable components in `web/src/elements/`

### File System Operations

- Extension writes files via VS Code workspace API (atomic updates)
- Batch updates to prevent API overload
- Hash-based caching to skip unchanged files
- Always use absolute paths from `admin.WORKSPACE_ROOT`

## Configuration

### Extension Settings (dj.\*)

| Setting                        | Type    | Default      | Description                                      |
| ------------------------------ | ------- | ------------ | ------------------------------------------------ |
| `dj.pythonVenvPath`            | string  | —            | Python virtual environment path (e.g., `.venv`)  |
| `dj.trinoPath`                 | string  | `trino-cli`  | Trino CLI executable path                        |
| `dj.dbtProjectNames`           | array   | `[]`         | Restrict which dbt projects to recognize         |
| `dj.dbtMacroPath`              | string  | `_ext_`      | Subfolder for extension-provided macros          |
| `dj.airflowGenerateDags`       | boolean | `false`      | Toggle Airflow DAG generation                    |
| `dj.airflowTargetVersion`      | string  | `2.7`        | Target Airflow version (2.7, 2.8, 2.9, 2.10)     |
| `dj.airflowDagsPath`           | string  | `dags/_ext_` | Folder where extension writes Airflow DAGs       |
| `dj.syncDebounceMs`            | number  | `1500`       | Debounce delay (ms) before triggering sync       |
| `dj.logLevel`                  | string  | `info`       | Logging level (debug, info, warn, error)         |
| `dj.codingAgent`               | boolean | `false`      | Enable AI agent integration (AGENTS.md + skills) |
| `dj.aiHintTag`                 | string  | —            | Standard tag for resources with AI Hints         |
| `dj.columnLineage.autoRefresh` | boolean | `true`       | Auto-refresh Column Lineage on file switch       |
| `dj.dataExplorer.autoRefresh`  | boolean | `false`      | Auto-refresh Data Explorer on file switch        |
| `dj.lightdashProjectPath`      | string  | —            | Custom path to dbt project for Lightdash         |
| `dj.lightdashProfilesPath`     | string  | —            | Custom path to dbt profiles for Lightdash        |
| `dj.autoGenerateTests`         | object  | —            | (Experimental) Auto-generate tests on models     |

### Environment Variables

**Trino Connection:**

```bash
TRINO_HOST=your-trino-host
TRINO_PORT=8080                    # 443 for Starburst Galaxy
TRINO_USERNAME=your-username
TRINO_CATALOG=your-catalog
TRINO_SCHEMA=your-schema
```

**Lightdash (optional):**

```bash
LIGHTDASH_URL=your-lightdash-url
LIGHTDASH_PREVIEW_NAME=your-preview-name
LIGHTDASH_PROJECT=your-project-uuid
LIGHTDASH_TRINO_HOST=host.docker.internal  # Trino host override for Docker
```

## AI Agent Integration

The extension includes built-in support for AI coding agents to help users create DJ-compliant dbt models.

**Project-level AGENTS.md generation:** When installed in a dbt project, the extension generates an `AGENTS.md` file in the workspace root's `.dj/` directory (typically in `.gitignore`). This file contains DJ framework-specific instructions (model types, JSON schema structure, naming conventions, column definitions) that users can reference in their LLM workflows to generate valid `.model.json` and `.source.json` files. The generated file is tailored to the project's configuration and available models/sources.

**Skill files (`.dj/skills/`):** The extension writes agent-agnostic skill directories to the workspace root's `.dj/skills/` directory, following the [Agent Skills](https://agentskills.io) open standard. Each skill is a subdirectory containing a `SKILL.md` file with YAML frontmatter (`name` and `description`) and markdown instructions (e.g., `.dj/skills/dj-create-new-model/SKILL.md`). Skill templates are bundled with the extension in `templates/skills/` and copied to the workspace at activation time.

**Coding agent setting (`dj.codingAgent`):** Set to `true` to enable AI agent integration. When enabled, both `AGENTS.md` and skill files are written to the workspace root's `.dj/` directory (typically in `.gitignore`). Legacy string values (`github-copilot`, `claude-code`, `cline`) are still accepted but deprecated — skills are now agent-agnostic.

**Agent service (`src/services/agent/`):** Contains `utils.ts` with `generateAgentsMd()` for reading the `templates/_AGENTS.md` template. Skill files are read from `templates/skills/` and written to `.dj/skills/` at the workspace root by `writeSkillFiles()` in the Dbt service.

## Additional Resources

- **README.md** — User-facing documentation and setup guide
- **DEVELOPMENT_SETUP.md** — Detailed development environment setup
- **CONTRIBUTING.md** — Contribution guidelines and code standards
- **CHANGELOG.md** — Release history
- **ROADMAP.md** — Future plans
- **docs/** — Complete documentation including tutorials and examples
- **docs/models/README.md** — Detailed reference for all 11 model types
- **docs/SETTINGS.md** — Full settings reference
