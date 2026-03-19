# Change Log

## 1.0.0 (March 2026)

### Visual Data Modeling

- **Interactive Model Builder** - Node-based visual canvas for creating and editing models
  - 10+ node types: Select, Join, Union, Rollup, Lookback, GroupBy, Where, Column Selection, Column Configuration, Lightdash
  - Configure joins, select columns, add filters, and group data directly in the UI
  - Add custom columns with expressions, formulas, and SQL
  - Real-time validation and SQL preview
  - Instant preview of generated JSON, SQL, and YAML outputs with syntax highlighting
  - Final preview with side-by-side or unified diff comparison
  - Model cloning to duplicate existing models
  - Identical output to JSON configuration
  - Seamless switching between visual and JSON editing
  - Source Model reference for aggregated columns in `int_select_model` and `int_lookback_model`
  - Aggregation support (`agg`, `aggs`) in join model types (`int_join_models`, `mart_join_models`) with `HAVING` clause for `mart_join_models`
  - Inline CTEs (Common Table Expressions) for `int_select_model`, `int_join_models`, `int_union_models`, `mart_select_model`, and `mart_join_models` — define lightweight SQL `WITH` clauses within model JSON, with CTE chaining, CTE joins, CTE unions, and lineage tracing
  - Seed CSV column reading when columns are not defined in the dbt manifest

### Interactive Learning

- **Tutorial System** - Two interactive modes accessible from Model Create wizard
  - "Play Tutorial" button - Guided walkthroughs with pre-filled data for Select, Join, Union, Rollup, and Lookback models
  - "Assist Me" button - Toggle contextual help and on-demand guidance while working
  - Accessed via help icon (?) in Model Create wizard header
  - Automatic navigation through wizard steps with highlighted UI elements

### Data Explorer & Lineage

- **Model Lineage Graph** - Improved visualization for large projects

  - Progressive node expansion to explore upstream/downstream dependencies
  - Visual indicators for model types with color-coded badges
  - Query and preview model data with configurable row limits
  - Sortable query results with data and SQL view modes
  - Compile model with real-time log streaming in dedicated panel
  - Auto-sync toggle - automatically updates when switching model files (toggle in toolbar)

- **Column-Level Lineage** - Column-level dependency tracing
  - Interactive DAG visualization to trace upstream/downstream column dependencies
  - Expression analysis (raw, passthrough, renamed, derived transformations)
  - Transformation tooltips show SQL expressions for derived columns
  - CSV export for single column or all columns in a model
  - Auto-refresh toggle - automatically updates when switching files (toggle in toolbar)

### Integration Enhancements

- **dbt Command Integration** - Run dbt directly from VS Code

  - Compile with `Cmd+Shift+C`
  - Execute dbt commands with multiple options (build, defer, full refresh, clean, deps, seed)
  - Run single model, multi-model, full project, or modified models with lineage tracking
  - Preview and copy generated commands with syntax highlighting

- **Lightdash Preview** - One-click BI dashboard previews

  - Manage multiple active previews with custom suffixes
  - Progress tracking with status updates
  - Enhanced Lightdash metadata configuration
  - AI hint support for semantic layer
  - Open previews in browser or copy links to clipboard
  - Inherited metrics filtering based on downstream column availability

- **Trino Integration** - Database execution and monitoring
  - Executes dbt models using Trino as query engine
  - Query monitoring view shows running queries from DJ operations
  - System monitoring (node status and cluster health)
  - Source introspection for automatic column detection when creating sources
  - Test connection command to verify Trino connectivity

### Developer Experience

- **Edit Drafts** - Non-destructive model editing

  - Edit models without affecting generated SQL/YAML
  - Save/discard draft changes
  - Draft management in sidebar view
  - Persistent form state stored in `.dj/state` subdirectory for draft model preservation

- **Run DBT Test** - Interactive webview for running dbt data tests

  - Model selection with git-changed model detection and manual selection
  - Smart test auto-detection based on model structure (joins, aggregates)
  - Bulk test configuration and per-model lineage controls (upstream/downstream)
  - Real-time test execution with streaming console output
  - Test status tracking and analytics summary (success/failure rates)
  - Configurable tests: `equal_row_count`, `equal_or_lower_row_count`, `no_null_aggregates`

- **First Launch** - Background checks and .gitignore prompt

  - Silent background checks for prerequisites (Trino CLI, Python venv, dbt)
  - .gitignore configuration prompt for `.dj/` folder

- **Settings Validation** - Instant feedback when configuring the extension

  - Real-time validation of Python venv path, Trino path, and dbt projects
  - Clear error messages if paths are invalid or missing
  - Helpful prompts to refresh projects or test connections

- **Enhanced Error Messages** - Clear, actionable validation errors

  - Model-type-specific error messages explain exactly what's wrong
  - Contextual suggestions for fixing common mistakes
  - Friendly error formatting instead of technical schema errors

- **Incremental Model Defaults** - Pre-configured hook values for incremental models

### 🤖 AI & Agent Support

- **AI Hints** - Natural language descriptions for semantic layer
  - Column-level AI hints
  - Model-level AI hints
  - Bulk AI hint updates
  - Integration with coding agents (Copilot, Claude, Cline)
  - Auto-generated `AGENTS.md` providing LLM context for dbt projects

### 🎨 UI/UX

- **Dark Mode Support** - Full dark mode theming across all features and UI components

### ⚡ Performance

- **Faster Extension Startup** - Extension loads more quickly when opening workspaces
- **Smoother File Switching** - Intelligent debouncing prevents lag when rapidly switching between files
- **Faster Sync Operations** - Smart caching skips regenerating unchanged SQL/YAML files

### Documentation

- New comprehensive guides: Visual Editor, Lineage, Integrations
- Enhanced tutorial with interactive modes
- Setup guide improvements
- GIF demonstrations for key features

---

## 0.1.0 (Initial release)

- Initial version of DJ (dbt-json) Framework extension
