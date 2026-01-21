# AGENTS.md

## Purpose
This repository contains a Streamlit app that can run locally using DuckDB (in-memory) or connect to a remote Snowflake database. It can also be deployed to Snowflake Streamlit using the Snowflake CLI and the configuration in snowflake.yml.

## Repo map (key files)
- streamlit_app.py: Main Streamlit app logic and UI
- snowflake.yml: Snowflake CLI app definition for Streamlit deployment
- environment.yml: Conda dependencies for Snowflake Streamlit runtime
- requirements.txt: Local Python dependencies
- Makefile: Local run targets
- pages/: Streamlit multipage content
- common/: Shared helpers (must include `__init__.py` for Python package recognition)
  - common/__init__.py: Package marker (required for Snowflake deployment)
  - common/db.py: Database connection helpers with auto-detection of runtime mode
  - common/test_data.py: Test data definitions (DDL/DML) - single source of truth
  - common/hello.py: Simple hello helper

## Runtime Modes (Auto-Detected)
The app auto-detects runtime mode based on `RUNTIME_MODE` env var or available connections:
- **duckdb**: Local in-memory DuckDB (set by `make run-duckdb`)
- **snowflake_local**: Local Snowflake connector (set by `make run-snowflake`)
- **snowflake_deployed**: Snowflake Streamlit via `get_active_session()` (deployed)

No manual selection needed - the Makefile sets `RUNTIME_MODE` automatically.

## Test Data System
Test data is defined centrally in `common/test_data.py` and can be created consistently across all modes:

**Tables available:**
- TESTTABLE: Call center reference data (TPC-DS style)
- SAMPLE_DATA: Simple key-value sample table (note: not "SAMPLE" - that's a reserved keyword)
- DAILY_METRICS: Time-series metrics data

**Setup buttons in sidebar:**
- 🔍 Check Table Status: Verify which tables exist and row counts
- 🚀 Setup Test Data: CREATE OR REPLACE tables and populate with sample data

**For Snowflake modes:** Configure target Database and Schema in sidebar before setup.

## Local development (regular Streamlit)
Use the Makefile targets (these install requirements.txt before running):
- make run-duckdb: Run locally with DuckDB in-memory
- make run-snowflake: Run locally with Snowflake using st.secrets generated from environment variables
- make upload-stage: Upload all app files directly to Snowflake stage (useful when deploy fails)
- make help: Show available commands

## Snowflake deployment (Streamlit in Snowflake)
Snowflake Streamlit runs inside Snowflake's managed environment and is not the same as a local Streamlit app:
- Dependencies must be declared in environment.yml (Snowflake uses this to build the runtime)
- Secrets should come from Snowflake (st.secrets) rather than local .streamlit/secrets.toml
- Network access and filesystem behavior differ from local execution
- DuckDB should be treated as local-only (not guaranteed in Snowflake Streamlit)
- Some Streamlit features differ or are unsupported in Snowflake; avoid unsupported widgets and components
- CSP restrictions apply: external scripts/styles/fonts are blocked; external media is limited

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
- Keep environment.yml aligned with any packages needed in Snowflake Streamlit (Snowflake runtime only).
- Keep requirements.txt aligned with local dependencies (local runtime only).
- When running in Snowflake Streamlit, prefer st.secrets and Snowflake's active session (no username/password needed).
- When running locally, allow explicit Snowflake credentials via st.secrets or environment variables.
- Do not assume DuckDB works in Snowflake Streamlit; gate DuckDB usage to local development only.
- Use Snowflake session helpers in Snowflake Streamlit (st.connection("snowflake") or get_active_session()).
- Cache query results with st.cache_data and reusable connections with st.cache_resource for performance.
- Keep SQL parameterized where possible; validate user inputs.
- Keep page configuration minimal and compatible with Snowflake Streamlit limitations.

## Suggested runtime behavior (for agents and contributors)
- Local (DuckDB): default to in-memory DB and sample data.
- Local (Snowflake): use credentials from st.secrets or the Makefile-generated .streamlit/secrets.toml.
- Snowflake Streamlit: use st.secrets or Snowflake-provided session, and avoid local-only options.
- Provide a demo/fallback mode when Snowflake objects are missing to improve DX.
- Use session state for user workflows (filters, chat history) and rerun-safe patterns.

## Safety and quality checklist
- ✅ Secrets are not hard-coded into documentation or committed files.
- ✅ DuckDB usage is limited to local dev.
- ✅ Snowflake connections do not require local-only files when deployed.
- ✅ environment.yml and snowflake.yml are updated together for deployment changes.
- ✅ Streamlit in Snowflake limitations are respected (no blocked components or CSP-violating resources).
- ✅ Queries and expensive computations are cached where appropriate.
- ✅ Utilities/data access code is separated into helper modules for maintainability.
- ✅ All SQL statements end with semicolons for Snowflake compatibility.
- ✅ No reserved SQL keywords used as table names (SAMPLE → SAMPLE_DATA).
- ✅ common/__init__.py exists for package recognition in Snowflake.

## If you're an automated agent
- Favor minimal, targeted changes.
- Keep Snowflake Streamlit behavior in mind when adding dependencies or local-only features.
- Update environment.yml and snowflake.yml if deploying to Snowflake.
- Update requirements.txt and Makefile if local dev changes.
- Keep README and assets/ in sync when adding new demo flows or features.
- Use consistent structure: streamlit_app.py + utils.py + pages/ + assets/ + data/ (if applicable).
- **When adding new modules under common/**, ensure they're uploaded via `make upload-stage` or full deploy.
- **When writing SQL**, always end statements with `;` and avoid reserved keywords.

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

## Troubleshooting

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

### Files not updating in deployed app
- Use `make upload-stage` to force upload all files
- Refresh the Streamlit app in browser (may need to wait a few seconds)
