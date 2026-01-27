# AGENTS.md

## Purpose
This repository contains a Streamlit app that can run locally using DuckDB (in-memory) or connect to a remote Snowflake database. It can also be deployed to Snowflake Streamlit using the Snowflake CLI and the configuration in snowflake.yml.

---

## ⚠️ WAREHOUSE RUNTIME CONSTRAINTS (CRITICAL)

**Snowflake Streamlit apps do not run in a general-purpose Python environment.**

They run:
- Inside Snowflake-managed infrastructure
- With warehouse-backed execution
- With strict dependency resolution rules

### The Golden Rule
> **If it cannot run inside a Snowflake warehouse with Anaconda-managed dependencies, it is not part of the real application — only a local convenience.**

### Allowed Dependencies
Only packages available via:
- Snowflake-managed Anaconda channel: https://repo.anaconda.com/pkgs/snowflake/
- Explicitly supported versions in `environment.yml`

**NOT allowed:**
- No `pip install` at runtime
- No dynamic dependency resolution
- No packages outside Anaconda channel

### What Breaks Warehouse Runtimes
- ❌ Optional imports that fail silently locally
- ❌ Environment-conditional imports like `if local: import x`
- ❌ Hidden transitive dependencies
- ❌ DuckDB usage inside Snowflake runtime
- ❌ Top-level imports of local-only packages

### DuckDB Boundary (Hard Rule)
**DuckDB is strictly LOCAL-ONLY.** Snowflake runtime code must NEVER import it.

The codebase enforces this via:
1. **Lazy imports**: `import duckdb` is inside `get_duckdb_connection()`, not at module level
2. **Runtime guards**: Function raises `RuntimeError` if called in deployed mode
3. **Environment detection BEFORE imports**: Mode is checked before any local-only code executes

```python
# ✅ CORRECT: Guarded lazy import
def get_duckdb_connection():
    if os.getenv("RUNTIME_MODE") == "snowflake_deployed":
        raise RuntimeError("DuckDB not available in warehouse runtime")
    import duckdb  # Only imported when actually needed locally
    return duckdb.connect(":memory:")

# ❌ WRONG: Top-level import breaks warehouse runtime
import duckdb  # This line alone will crash in Snowflake
```

**Local type translation:** DuckDB does not support Snowflake-only data types such as `VARIANT` or `TIMESTAMP_NTZ`. The shared `execute_ddl_dml()` helper automatically rewrites CREATE statements when `RUNTIME_MODE=duckdb` so the canonical Snowflake DDL can still run locally (e.g., `VARIANT → JSON`, `TIMESTAMP_NTZ → TIMESTAMP`). This keeps a single source of truth while respecting DuckDB limits. Avoid adding new Variant columns unless they are truly required in Snowflake, because the translation only covers common primitives.

### Snowpark is the Lingua Franca
Snowpark Python is first-class in warehouse runtime:
- Fully supported and optimized
- Stable dependency surface
- Preferred over snowflake-connector-python in deployed mode

---

## Architecture: Snowflake-First Design

### Mental Model
Snowflake-hosted Streamlit is the **"truth environment."**
- Local and remote development are simulators, not peers
- The Snowflake execution model is the primary reference
- Local DX conforms downward to Snowflake, not the other way around

### Hard Boundary: Local vs Warehouse Code

```
common/
  └── db.py           ✅ Contains both paths, with guards
      └── get_duckdb_connection()      # LOCAL-ONLY (guarded)
      └── get_snowpark_session()       # WAREHOUSE-NATIVE
      └── get_snowflake_connector()    # Local dev (Anaconda-safe)
```

Environment detection happens BEFORE imports, not after.

### Data Strategy (Warehouse-Optimized)

Correct hierarchy:
1. **Canonical schema definitions** (DDL in `common/test_data.py`)
2. **Canonical mock data definitions** (DML in `common/test_data.py`)
3. **Execution adapters**:
   - DuckDB adapter (local only, guarded)
   - Snowpark adapter (remote + in-Snowflake)

This avoids:
- Schema drift between local and deployed
- Divergent test data
- Warehouse runtime surprises

### Button-Based DDL/DML (Cost-Aware)
Warehouses are expensive. DDL/DML is explicit:
- No hidden setup on app load
- User clicks "Setup Test Data" button
- Debuggable and enterprise-friendly

---

## Repo map (key files)
- streamlit_app.py: Entrypoint file (router) - uses st.navigation for multipage app
- snowflake.yml: Snowflake CLI app definition for Streamlit deployment
- environment.yml: Conda dependencies for Snowflake Streamlit runtime (Anaconda-managed)
- requirements.txt: Local Python dependencies (includes DuckDB)
- Makefile: Local run targets
- pages/: Page modules for the multipage app
  - pages/home.py: Home/overview page (default)
  - pages/ingestion_monitoring.py: Ingestion pipeline monitoring
  - pages/modelling_monitoring.py: Transformation/modelling monitoring
  - pages/governance.py: Data governance and compliance
  - pages/alerting.py: Alert configuration and history
  - pages/decisions.py: Decision logging and validation tests
  - pages/admin_copilot.py: AI-powered ingestion copilot
  - pages/query_settings.py: Query catalog viewer for Copilot SQL
  - pages/settings.py: App configuration and ingestion environment settings
- common/: Shared helpers (must include `__init__.py` for Python package recognition)
  - common/__init__.py: Package marker (required for Snowflake deployment)
  - common/navigation.py: Navigation configuration (defines pages and sections)
  - common/layout.py: Shared layout components (init_page, runtime status, etc.)
  - common/db.py: Database connection helpers with guarded imports and auto-detection
  - common/test_data.py: Test data definitions (DDL/DML) - single source of truth
  - common/hello.py: Simple hello helper
  - common/ingestion_templates.py: Centralized ingestion DDL/procedure templates plus metadata helpers

---

## Multipage App Architecture

This app uses **st.navigation** (recommended method) for multipage structure.

### Why st.navigation?
- Maximum flexibility in page organization
- Programmatic control over navigation flow
- Grouped navigation sections in sidebar
- Pages can be defined anywhere in source directory
- Entrypoint file acts as a router

### Structure
```
streamlit_app.py          # Router/entrypoint - sets up navigation
├── common/
│   ├── navigation.py     # Page definitions and sections
│   ├── layout.py         # Shared UI components
│   └── db.py             # Database helpers
└── pages/
    ├── home.py           # Default page (Overview)
    ├── ingestion_monitoring.py
    ├── modelling_monitoring.py
    ├── governance.py
    ├── alerting.py
    ├── decisions.py
    ├── admin_copilot.py
    └── settings.py
```

### Adding a New Page
1. Create a new `.py` file in `pages/` directory
2. Add the page to `common/navigation.py` in the appropriate section
3. Use `init_page()` from `common/layout.py` for consistent setup
4. The page will automatically appear in the navigation

Example page structure:
```python
# pages/my_new_page.py
import streamlit as st
from common.layout import init_page, get_runtime_mode

def main():
    conn = init_page()  # Initialize page, get DB connection
    runtime_mode = get_runtime_mode()
    
    st.title("My New Page")
    # Page content here...

main()
```

### Navigation Sections
Pages are organized into logical groups:
- **Overview**: Home page
- **Monitoring**: Ingestion, Modelling monitoring
- **Operations**: Governance, Alerting, Decisions
- **Copilot**: Ingestion
- **System**: Settings, Query Settings

### Snowflake URL Format
When deployed to Snowflake, URL pathnames are prefixed with `/!`:
- Local: `localhost:8501/ingestion_monitoring`
- Snowflake: `https://app.snowflake.com/.../!/ingestion_monitoring`

---

## Runtime Modes (Auto-Detected)
The app auto-detects runtime mode based on `RUNTIME_MODE` env var or available connections:
- **duckdb**: Local in-memory DuckDB (set by `make run-duckdb`) — LOCAL ONLY
- **snowflake_local**: Local Snowflake connector (set by `make run-snowflake`)
- **snowflake_deployed**: Snowflake Streamlit via `get_active_session()` (deployed)

No manual selection needed - the Makefile sets `RUNTIME_MODE` automatically.

## Test Data System
Test data is defined centrally in `common/test_data.py` and can be created consistently across all modes:

**Setup buttons in sidebar:**
- 🔍 **Check Table Status** – verifies existence and row counts before you query
- 🚀 **Setup Test Data** – runs CREATE OR REPLACE plus seed inserts for every table below

| Table | Description |
| --- | --- |
| TESTTABLE | Call center reference data (TPC-DS style) |
| SAMPLE_DATA | Simple key-value sample table (note: not "SAMPLE" - that's a reserved keyword) |
| DAILY_METRICS | Time-series metrics data |
| SCANDS_FINANCE_S2T | Finance source-to-target lineage mappings |
| SCANDS_PROD_S2T | Production/admin lineage mappings |
| SCANDS_QUALITYRISK_S2T | Quality & risk lineage mappings |
| VALIDATION_OBJECTS | Validation registry supporting Settings → Validation |
| VALIDATION_METRICS_SOURCE | SQL Server/source-side ingestion metrics feeding validation joins |
| VALIDATION_METRICS_TARGET | Snowflake target ingestion metrics compared against source baselines |
| VALIDATION_MODEL_RUNS | Audit log of modelling validation executions (manual or scheduled) |
| VALIDATION_MODEL_RESULTS | Step-level modelling validation payloads persisted as VARIANT |

`VALIDATION_METRICS_SOURCE` and `VALIDATION_METRICS_TARGET` share the same DDL so the Validation → Ingestion page can full-outer-join the two datasets and spotlight matches or gaps between source scans and Snowflake landings.
`VALIDATION_MODEL_RUNS` and `VALIDATION_MODEL_RESULTS` capture the schedule metadata plus step outputs for the Validation → Modelling workflow so every execution (manual or scheduled) is auditable.

**For Snowflake modes:** Configure target Database and Schema in sidebar before setup.

## Compatibility View Builder

- Unified mapping view: `SOURCE_TO_TARGET_MAPPING_VW` lives alongside the S2T tables and UNION ALLs `SCANDS_FINANCE_S2T`, `SCANDS_PROD_S2T`, and `SCANDS_QUALITYRISK_S2T`, tagging each record with a `DOMAIN` value for filtering.
- Navigate to **Compatibility → Compatibility Views** to configure mapping/view schemas, refresh the unioned mapping view, and build/drop compatibility views in your chosen Snowflake schema.
- Filtering: Domain, source schema, and source table selectors drive the SQL preview; each generated `CREATE OR REPLACE VIEW` statement maps target columns back to legacy source column names in ordinal order.
- Safety checks: Before creating a view the app verifies the Snowflake target table exists; all SQL executes through the shared `run_query`/`execute_ddl_dml` helpers (no arbitrary execution, no DuckDB imports).
- Settings now includes a **Validation** tab that writes directly to `VALIDATION_OBJECTS`, providing an auditable registry of fully qualified validation targets referenced by the builder.
- DuckDB mode automatically strips database/schema qualifiers when generating mapping SQL so local runs succeed even though DuckDB lacks Snowflake catalogs.

### Builder Configuration

The builder always pulls its lineage columns from the canonical S2T tables below, so teams can verify coverage before refreshing or generating views:
- `SCANDS_FINANCE_S2T`
- `SCANDS_PROD_S2T`
- `SCANDS_QUALITYRISK_S2T`

## Validation Workspace

- **Validation → Ingestion** reads `VALIDATION_METRICS_SOURCE` and `VALIDATION_METRICS_TARGET`, full-outer-joins the metrics, and highlights matches, mismatches, and source/target-only rows with filterable severity slices.
- **Validation → Modelling** runs the Info Mart comparison script step-by-step directly inside the UI, shows each query's status/result, and optionally appends the JSON summary step when the checkbox is enabled.
- **Validation → Model Monitoring** summarizes every scheduled modelling task (task name, latest status, days active, source FQN, compatibility view FQN) and exposes a drill-down that filters audit history by database/schema/table before showing the stored step results.
- Every modelling run captures scheduling metadata (`schedule_flag`, `schedule_start_date`, `schedule_end_date`, `run_type`) plus operator notes, then persists into `VALIDATION_MODEL_RUNS`.
- Step outputs (column inventory, row-level summary, detailed mismatches, orphan checks, JSON payloads) are written to `VALIDATION_MODEL_RESULTS` as VARIANT for day-over-day comparisons.
- The page surfaces the latest audit log so teams can replay prior runs, inspect saved payloads, and prove what was executed (and when) without rerunning heavy SQL unnecessarily.
- When the **Schedule daily run?** toggle is enabled, the app now enforces an explicit `DEV/TEST/UAT/PROD` environment selection, records it in `VALIDATION_MODEL_RUNS`, and refreshes the warehouse-side command queue `DATA_VAULT_TEMP.MIGRATION_COPILOT.VALIDATION_<TABLE>_<ENV>_COMMAND` so Snowflake Tasks can execute the exact same SQL workflow without Streamlit.

## Ingestion Metadata Environments

- The **Settings → Ingestion** tab controls which Snowflake database/schema hosts `METADATA_CONFIG_TABLE_ELT`.
- Environments available by default: DEV, TEST, UAT, PROD (mapped to `STAGE_<ENV>.ELT`).
- Selecting an active environment updates Pipeline Metrics and the Query Settings preview.
- Each environment section lets you override the default database/schema if your Snowflake layout differs.
- DuckDB mode still uses the standalone `METADATA_CONFIG_TABLE_ELT` created via the Test Data buttons.

## Ingestion Monitoring (environment-aware)

- The page now shows combined and per-environment tabs (DEV/TEST/UAT/PROD) for charts and tables.
- Combined summaries aggregate selected environments; tabs render one environment at a time.
- An environment summary grid (last 24h) uses the warehouse view `ELT_ENVIRONMENT_SUMMARY_24H_VW` and expects an `ENVIRONMENT` column in upstream views (e.g., unioned `ELT_JOB_RUN` per env). Ensure the view exists in each env stage (`STAGE_DEV`, `STAGE_TEST`, `STAGE_UAT`, `STAGE_PROD`).
- When you need multi-environment rollups, do **not** change the base DDLs. Create views that UNION the per-environment tables, tagging each branch with its env label (e.g., `SELECT ..., 'DEV' AS ENVIRONMENT FROM STAGE_DEV.ELT.<TABLE> UNION ALL ... TEST ... UAT ... PROD`). This keeps Snowflake DDLs untouched while enabling cross-env monitoring.

## Ingestion Copilot (Builder + Library)

- The **Copilot → Ingestion** page now walks developers through provisioning a new source across SQL Server, Azure DB, Flat File, or Excel patterns.
- The guided form captures landing schema/table, wildcard patterns, delta columns, server metadata, and variant payload notes, then emits two warehouse-ready statements:
  - **Adhoc trigger insert** (DEV/TEST/UAT only) creates a record in `METADATA_CONFIG_TABLE_ELT_ADHOC` with `ERROR_STATUS='TRIGGERED_NOT_STARTED'` so the ADF process picks it up immediately.
  - **Schedule enable** updates the canonical `METADATA_CONFIG_TABLE_ELT` row once a finished adhoc proof exists, ensuring PROD requests cannot bypass the safety check.
- ADF polling assumptions are explicit (hourly on the :00) and the generated SQL stamps `LAST_MODIFIED_DATE_LOADED` so command queues only fetch freshly-modified payloads.
- All SQL templates, payload builders, and metadata key helpers live in `common/ingestion_templates.py`, making it easy to keep the Snowflake-first text canonical and reuse it across pages.
- Below the builder the page exposes a reference DDL library: Azure/SQL dictionaries, EDW/source-file tables, metadata tables, and the `CREATE_SCHEMA_DYNAMIC` procedure can be previewed or copy/pasted per environment.

## Local development (regular Streamlit)
Use the Makefile targets (these install requirements.txt before running):
- make run-duckdb: Run locally with DuckDB in-memory
- make run-snowflake: Run locally with Snowflake using st.secrets generated from environment variables
- make upload-stage: Upload all app files directly to Snowflake stage (useful when deploy fails)
- make help: Show available commands

## Snowflake deployment (Streamlit in Snowflake)
Snowflake Streamlit runs inside Snowflake's managed environment:
- Dependencies declared in environment.yml (Anaconda channel only)
- No .env files, no OS-level configuration, no filesystem assumptions
- Everything is stateless, declarative, warehouse-scoped
- Secrets come from Snowflake (st.secrets) not local files
- DuckDB is NOT available — treated as local-only

The deployment definition is in snowflake.yml. The Snowflake CLI uses this file to package artifacts and deploy.

## CI/CD (Snowflake CLI)
- Use `snow streamlit deploy --replace` for idempotent updates.
- Store Snowflake CLI config in ~/.snowflake/config.toml (or use a CI secret to inject password).
- Pin Python version in CI for consistent packaging.
- Ensure the Snowflake CLI connection uses a valid account format; if you see a host format error, recreate the connection with the correct account identifier and region.
- If `snow streamlit deploy` fails with host format errors, use `make upload-stage` to directly upload files.

## Snowflake deployment artifacts (snowflake.yml)
The `artifacts` section in snowflake.yml must include ALL files needed at runtime:
- Include the entire `common/` directory (not individual files) for helper modules
- Ensure `common/__init__.py` exists to make it a valid Python package
- The stage name must match an existing stage (check with `SHOW STAGES IN SCHEMA`)

Example artifacts configuration:
```yaml
artifacts:
  - streamlit_app.py
  - environment.yml
  - pages
  - common
```

## Snowflake SQL compatibility
When writing SQL for Snowflake (in test_data.py or elsewhere):
- **Always terminate statements with semicolons** (`;`) - required for Snowflake
- **Avoid reserved keywords as table names**: `SAMPLE`, `TABLE`, `ORDER`, `GROUP`, etc.
  - Use `SAMPLE_DATA` instead of `SAMPLE`
- **Use USE DATABASE/SCHEMA** before DDL to set context, or use fully-qualified names
- **Wrap identifiers in double quotes** if they contain special characters or are case-sensitive

## Guardrails and checks
- Never commit credentials or generated .streamlit/secrets.toml to source control.
- Keep environment.yml aligned with Anaconda-available packages only.
- Keep requirements.txt for local-only dependencies (DuckDB, dev tools).
- DuckDB imports must be lazy and guarded — never at module level.
- When running in Snowflake Streamlit, use Snowpark session (get_active_session).
- When running locally, allow explicit Snowflake credentials via st.secrets or environment variables.
- Cache query results with st.cache_data and reusable connections with st.cache_resource.
- Keep SQL parameterized where possible; validate user inputs.

## Safety and quality checklist
- ✅ Secrets are not hard-coded into documentation or committed files.
- ✅ DuckDB is strictly local-only with runtime guards.
- ✅ All local-only imports are lazy (inside functions, not at module level).
- ✅ Snowflake connections do not require local-only files when deployed.
- ✅ environment.yml contains only Anaconda-available packages.
- ✅ Snowpark is used as the primary execution path in deployed mode.
- ✅ All SQL statements end with semicolons for Snowflake compatibility.
- ✅ No reserved SQL keywords used as table names (SAMPLE → SAMPLE_DATA).
- ✅ common/__init__.py exists for package recognition in Snowflake.
- ✅ Button-driven DDL/DML for cost-aware warehouse usage.
- ✅ Streamlit UI components use `width="stretch"|"content"` — `use_container_width` is deprecated after 2025-12-31.

---

## If you're an automated agent

### Warehouse Runtime Awareness
- **Treat Snowflake runtime as immutable** — code must work with Anaconda packages only
- **Treat Anaconda availability as law** — check https://repo.anaconda.com/pkgs/snowflake/
- **Forbid dynamic imports in Snowflake paths** — all imports must be resolvable at deploy time
- **Gate all local-only imports** — use lazy imports inside functions with runtime guards

### Snowflake-First Design Bias
- Snowpark as default execution path in deployed mode
- DuckDB as a local emulation tool only (never import at module level)
- Snowflake DDL as the schema authority
- Local testing validates logic; Snowflake testing validates reality

### Code Changes
- Favor minimal, targeted changes.
- When adding dependencies: check Anaconda channel first, add to environment.yml if available, requirements.txt if local-only.
- When adding imports: if local-only, use lazy import with runtime guard.
- Update environment.yml and snowflake.yml if deploying to Snowflake.
- Update requirements.txt and Makefile if local dev changes.
- **When writing SQL**, always end statements with `;` and avoid reserved keywords.

### Testing Philosophy
- "Local testing validates logic"
- "Snowflake testing validates reality"
- Run `make run-duckdb` for fast iteration
- Run `make run-snowflake` to test Snowflake connector locally
- Deploy and test in Snowflake to validate warehouse runtime

---

## AI quality guardrails (based on recent errors)
- Always re-open edited regions to verify indentation and block structure before finalizing.
- Avoid nested try/except edits without re-reading the full block after changes.
- Prefer small, isolated edits over large refactors to reduce syntax/indentation risk.
- Run a quick syntax check after edits (lint or Python parser) before deployment.
- If changing multi-level logic, re-run the app locally and confirm no SyntaxError/IndentationError.
- Keep error handling minimal and clear; avoid deep nesting when not needed.

## Additional safeguards to prevent AI mistakes
- Enable a lightweight formatter or linter (e.g., Ruff) and fail CI on syntax errors.
- Add a pre-commit hook to run `python -m py_compile` or `ruff check` on `streamlit_app.py`.
- Keep a small "smoke test" path that runs the app and executes a trivial query.
- Use smaller helper functions to reduce indentation depth and improve edit safety.

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'duckdb'" in Snowflake
- DuckDB is not available in Snowflake warehouse runtime
- Ensure all DuckDB imports are lazy (inside functions) and guarded by runtime checks
- Check that no top-level `import duckdb` exists anywhere in deployed code

### "ModuleNotFoundError: No module named 'common.db'"
- Ensure `common/__init__.py` exists
- Ensure `snowflake.yml` includes `common` (not just `common/hello.py`) in artifacts
- Run `make upload-stage` to push all files to the stage

### "SQL compilation error: unexpected 'SAMPLE'"
- `SAMPLE` is a reserved keyword in Snowflake (used for TABLESAMPLE clause)
- Rename to `SAMPLE_DATA` or another non-reserved name

### "SQL compilation error: syntax error at position X"
- Ensure SQL statements end with semicolons (`;`)
- Check for missing commas, parentheses, or quotes
- Verify table/column names don't conflict with reserved keywords

### "Host format error" during deploy
- The Snowflake CLI connection may have incorrect account format
- Use `make upload-stage` as a workaround to directly upload files
- Or recreate the connection with `snow connection add`

### "Works locally, fails in Snowflake" syndrome
- Check for local-only imports (DuckDB, dev tools)
- Verify all imports are Anaconda-available
- Ensure environment detection happens BEFORE local-only code paths
- Test with `make run-snowflake` before deploying

### Files not updating in deployed app
- Use `make upload-stage` to force upload all files
- Refresh the Streamlit app in browser (may need to wait a few seconds)
