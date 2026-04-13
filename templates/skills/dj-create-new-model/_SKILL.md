---
name: dj-create-new-model
description: >-
  Create a DJ .model.json file for a new dbt model. Use when the user wants to
  create, add, or scaffold a dbt model -- staging, intermediate, or mart --
  including joins, CTEs, rollup, subqueries, or aggregations.
compatibility: DJ extension workspace with .dj/schemas/ and .agents/dj/AGENTS.md
metadata:
  dj-framework-skill: '1.0'
---

# Create DJ model

**Create** new **`.model.json`** files (and **`.source.json`** when adding sources). **Never** hand-edit auto-generated **`.sql`** / **`.yml`** — only the JSON sources of truth.

**Reading order:** **`.dj/schemas/`** (type schema + **`$ref`s**) for exact shapes → **`.agents/dj/AGENTS.md`** **Model Types** (examples) → **Advanced** (short map: CTEs, rollup, shorthands, subqueries — still defer to schemas) → **Important Conventions** **#6**–**#12**.

## Model `type` (infer — do not ask the user)

| Layer    | Intent → `type`                                                                                                                                    |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **stg**  | raw source → `stg_select_source`; seed/model → `stg_select_model`; union sources → `stg_union_sources`                                             |
| **int**  | one model → `int_select_model`; joins → `int_join_models`; unnest → `int_join_column`; lookback → `int_lookback_model`; union → `int_union_models` |
| **mart** | one model → `mart_select_model`; joins → `mart_join_models`                                                                                        |

**Rollup:** optional **`from.model.rollup`** on **`int_select_model`** / **`int_join_models`** — coarser time **`interval`**, **`agg`/`aggs`** re-aggregated for the new grain (**`model.from.rollup.schema.json`**; **AGENTS** **Advanced**, **#9**–**#10**).

One clarifying question if source vs existing model is unclear.

## Inputs, path, workflow

**Fields:** `type`, `group`, `name`; `topic` on all types in schema **except** `int_join_models` is not in **`required`** (still set in practice). Ask for missing names; mirror project patterns.

**Path:** `models/<staging|intermediate|mart>/<group>/<topic>/<layer>__<group>__<topic>__<name>.model.json` (`stg_*`→`staging`, etc.).

**Checklist**

- [ ] `type` from table; read **`.dj/schemas/model.type.<type>.schema.json`**; if CTE / subquery / **`from.model.rollup`** / hooks / **`agg`**, also **`model.cte`**, **`model.subquery`**, **`model.from.rollup`**, **`model.sql_hooks`**, **`model.select.*.with.agg`** as needed
- [ ] **`.agents/dj/AGENTS.md`**: **Model Types** example; **Advanced** if CTE / rollup / shorthand / subquery
- [ ] Upstream columns from **`.model.json`** / **`.source.json`** (trace **`ctes`** if any)
- [ ] Write **JSONC**; validate against schema

## Conventions & gotchas

- **type**: Type of the model - mart, staging, intermediate, source, etc. Determined from the decision tree above.
- **group**: Group of the model - analytics, finops, sales, marketing, etc.
- **topic**: Topic of the model - aws_cur, gcp_billing, salesforce, etc.
- **name**: Name of the model - accounts_billing_daily, opportunities_facts, etc.

If the user hasn't provided group/topic/name, ask for them. Look at existing models in the project for naming conventions.

## File Naming and Path

- **Model name**: `<layer>__<group>__<topic>__<name>` (e.g., `int__analytics__billing__daily_summary`)
- **File name**: `<model_name>.model.json`
- **Directory**: `models/<layer>/<group>/<topic>/` where layer is `staging`, `intermediate`, or `mart`

The layer directory is derived from the type prefix: `stg_*` -> `staging`, `int_*` -> `intermediate`, `mart_*` -> `mart`.

## Workflow

- [ ] Step 1: Determine the model type from the user's request
- [ ] Step 2: Gather required inputs (group, topic, name, type-specific fields)
- [ ] Step 3: Read the JSON schema at `.dj/schemas/model.type.<type>.schema.json` to understand required and optional fields. Follow `$ref` links to sub-schemas as needed
- [ ] Step 4: Refer to the AGENTS.md "Model Types" section for the example structure of the selected type
- [ ] Step 5: Read upstream model/source files to verify available columns before writing `select`
- [ ] Step 6: Create the `.model.json` file at the correct path using JSONC format (comments and trailing commas allowed)
- [ ] Step 7: Verify the file is valid against the schema

## Important Conventions

- Never edit generated `.sql` or `.yml` files -- only edit `.model.json`
- Use JSONC format: trailing commas are allowed, preserve any existing comments
- Source references use `<database>__<schema>.<table>` format (double underscore, then dot)
- Column types are `dim` (dimension) or `fct` (fact/measure), default is `dim`
- When using `agg`, always include `"group_by": [{ "type": "dims" }]`
- For joins, verify upstream columns exist by reading the upstream model's `.model.json` or source `.source.json`
- Rename models by changing JSON fields (type/group/topic/name), never by renaming the file on disk
- `int_select_model` and `int_join_models` support `from.rollup` for time-grain re-aggregation without needing a separate `int_rollup_model`. See AGENTS.md "Model Types" and `model.from.rollup.schema.json`
- Use the `ctes` array for inline CTEs on `int_select_model`, `int_join_models`, `int_union_models`, `mart_select_model`, `mart_join_models`. See AGENTS.md "Inline CTEs" and `model.cte.schema.json`
- WHERE, HAVING, and JOIN ON conditions support inline subqueries via the `subquery` key. See AGENTS.md "Inline Subqueries" and `model.subquery.schema.json`

## Gotchas

- Subquery `column` is required for all operators except `exists`/`not_exists`
- CTEs must be ordered: a CTE can only reference CTEs defined **before** it in the `ctes` array
- `from.rollup` requires the upstream model to have a select column with an `"interval"` field (e.g., `{ "name": "datetime", "interval": "day" }`)
- Cross joins have no `on` property -- do not include `on: {}` or `on: null`
- Subquery `from` can reference a model, source, or CTE -- use `{ "cte": "name" }` for CTEs defined in the same model
- `topic` is not in `required` for `int_join_models` (it is for all other types) -- still set it in practice
- `mart_select_model` and `int_union_models` do not support `agg`/`aggs` in select items -- use only passthrough or expression columns
