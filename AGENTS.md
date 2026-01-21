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
- common/: Shared helpers

## Local development (regular Streamlit)
Use the Makefile targets (these install requirements.txt before running):
- make run-duckdb: Run locally with DuckDB in-memory
- make run-snowflake: Run locally with Snowflake using st.secrets generated from environment variables
- make help: Show available commands

## Snowflake deployment (Streamlit in Snowflake)
Snowflake Streamlit runs inside Snowflake’s managed environment and is not the same as a local Streamlit app:
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

## Guardrails and checks
- Never commit credentials or generated .streamlit/secrets.toml to source control.
- Keep environment.yml aligned with any packages needed in Snowflake Streamlit (Snowflake runtime only).
- Keep requirements.txt aligned with local dependencies (local runtime only).
- When running in Snowflake Streamlit, prefer st.secrets and Snowflake’s active session (no username/password needed).
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

## If you’re an automated agent
- Favor minimal, targeted changes.
- Keep Snowflake Streamlit behavior in mind when adding dependencies or local-only features.
- Update environment.yml and snowflake.yml if deploying to Snowflake.
- Update requirements.txt and Makefile if local dev changes.
- Keep README and assets/ in sync when adding new demo flows or features.
- Use consistent structure: streamlit_app.py + utils.py + pages/ + assets/ + data/ (if applicable).

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
- Keep a small “smoke test” path that runs the app and executes a trivial query.
- Use smaller helper functions to reduce indentation depth and improve edit safety.
