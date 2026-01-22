# Migration Copilot Codebase Overview

## Purpose & Product Scope
Migration Copilot is a Streamlit-based application designed to support Snowflake migration, validation, and administration workflows. It is intentionally **Snowflake-first**: the deployed app runs inside Snowflake Streamlit with Snowpark, while local execution uses DuckDB or the Snowflake connector as a developer convenience. The UI is organized into operational areas (monitoring, governance, alerting, decisions, copilot) that scaffold future features for migration validation and operational oversight. The overall intent is to become a single operational hub for migration health, pipeline status, governance controls, and a copilot experience for administrators and data engineers. 【F:streamlit_app.py†L1-L48】【F:common/db.py†L1-L214】【F:common/navigation.py†L1-L88】

---

## Architecture at a Glance
### Runtime Modes
The app auto-detects runtime mode and uses **guarded imports** to enforce Snowflake runtime constraints:
- **snowflake_deployed**: Snowflake Streamlit runtime (Snowpark session via `get_active_session`).
- **snowflake_local**: local run with Snowflake connector for development/testing.
- **duckdb**: local in-memory DuckDB for fast iteration (strictly local-only; never imported at module level).

Runtime detection prioritizes the `RUNTIME_MODE` env var, then checks for an active Snowpark session, then checks Snowflake secrets, and finally falls back to DuckDB. DuckDB imports are lazy and guarded to prevent Snowflake runtime failures. 【F:common/db.py†L1-L214】

### Entry Point + Navigation
`streamlit_app.py` is the entrypoint/router and sets page configuration, initializes environment/session state, and delegates page rendering using `st.navigation`. Page grouping is declared in `common/navigation.py`, making it easy to add new pages by file and section. 【F:streamlit_app.py†L1-L48】【F:common/navigation.py†L1-L88】

### Shared Layout + State Management
`common/layout.py` provides common initialization (`init_environment`, `init_session_state`, `init_page`), runtime status display, and database connection management. This ensures all pages behave consistently and share a single connection lifecycle. 【F:common/layout.py†L1-L186】

---

## Key Modules & Responsibilities
### `common/db.py` – Runtime-aware Data Access
Core responsibilities:
- Runtime mode detection and guarded import boundaries.
- Connection factories for DuckDB, Snowflake connector, and Snowpark session.
- Query execution wrapper (`run_query`) that normalizes results across runtimes.
- Test-data setup utilities for creating sample tables (DDL/DML).
- Ingestion metadata environment resolution (DEV/TEST/UAT/PROD table routing).

This is the primary layer for any future persistence, data validation queries, and migration checks. 【F:common/db.py†L1-L333】

### `common/test_data.py` – Canonical Test Data Definitions
Defines schema + sample data for test tables, used consistently across runtime modes. This prevents schema drift between local and deployed environments and supports button-driven data setup in the UI. Tables include:
- `TESTTABLE` (TPC-DS-style call center data)
- `SAMPLE_DATA` (simple values for quick queries)
- `DAILY_METRICS` (time-series metrics)
- `METADATA_CONFIG_TABLE_ELT` (ingestion metadata configuration)

Utilities for formatting DDL/DML are provided to generate runtime-safe statements. 【F:common/test_data.py†L1-L310】

### `common/query_library.py` – Reusable SQL Catalog
Defines a small query library for Copilot features (e.g., active/inactive pipeline metrics). Each query has metadata (key/name/description) and a templated SQL block resolved against the active ingestion metadata table. This is the beginnings of a query catalog for the copilot surface. 【F:common/query_library.py†L1-L79】

---

## UI Pages & What They Enable
### Overview/Home (`pages/home.py`)
The landing page provides runtime status, a SQL query runner, a sample data preview, and test data setup controls. It also exposes schema selection for Snowflake modes. This page is the **primary operational scratchpad** and test data bootstrap. 【F:pages/home.py†L1-L155】

### Monitoring
- **Ingestion Monitoring** (`pages/ingestion_monitoring.py`): placeholder with planned pipeline status, load history, errors, and source health. 【F:pages/ingestion_monitoring.py†L1-L86】
- **Modelling Monitoring** (`pages/modelling_monitoring.py`): placeholder with planned dbt/model health, lineage, and quality metrics. 【F:pages/modelling_monitoring.py†L1-L86】

### Operations
- **Governance** (`pages/governance.py`): placeholder for access control, data classification, compliance, and audit logging. 【F:pages/governance.py†L1-L86】
- **Alerting** (`pages/alerting.py`): placeholder for alerts, notifications, escalation, and history. Includes demo alert list. 【F:pages/alerting.py†L1-L92】
- **Decisions** (`pages/decisions.py`): functional page for capturing governance/architecture decisions with validation SQL that should return zero rows. It includes decision categories, sample decisions, add-decision form, and a “run test” workflow. This is a key foundation for **migration validation rules** and operational policy enforcement. 【F:pages/decisions.py†L1-L256】

### Copilot
- **Ingestion Copilot** (`pages/admin_copilot.py`): shows pipeline metrics from the query library and a demo chat UI. This page is the natural home for natural-language query generation, diagnostics, and recommendations. 【F:pages/admin_copilot.py†L1-L146】

### System
- **Settings** (`pages/settings.py`): runtime info, test data config, ingestion metadata environment management, and system debug info. It is the configuration hub for metadata routing and environment selection. 【F:pages/settings.py†L1-L206】
- **Query Settings** (`pages/query_settings.py`): displays the query catalog with rendered SQL for the active environment. This is the governance/UI surface for reusable SQL definitions. 【F:pages/query_settings.py†L1-L33】

---

## Deployment + Dependency Model
### Snowflake Streamlit Deployment
`snowflake.yml` defines the Streamlit deployment artifacts and entrypoint. `environment.yml` is limited to Snowflake Anaconda packages (`streamlit`, `snowflake-snowpark-python`). This ensures the deployed app stays within Snowflake warehouse constraints. 【F:snowflake.yml†L1-L14】【F:environment.yml†L1-L6】

### Local Development
`requirements.txt` includes local-only dependencies (e.g., `duckdb`, `snowflake-connector-python`) plus shared packages. The local environment is intentionally broader than the Snowflake runtime, but all Snowflake-executed code avoids importing local-only packages. 【F:requirements.txt†L1-L6】【F:common/db.py†L1-L214】

---

## Extension Points for Future Features
### Migration Validation & Rule Enforcement
- Build new validation rules using the Decisions page pattern: store rules (decision metadata + SQL) and execute them with standardized pass/fail outputs. This can become a full validation framework that scans target schemas post-migration. 【F:pages/decisions.py†L1-L256】

### Copilot Intelligence Layer
- The query library in `common/query_library.py` can evolve into a curated analytics catalog that powers Copilot responses, explains metrics, and generates parameterized SQL. Pair with the “Query Settings” page to keep governance visibility for generated SQL. 【F:common/query_library.py†L1-L79】【F:pages/query_settings.py†L1-L33】

### Ingestion Metadata & Health
- The ingestion metadata table (`METADATA_CONFIG_TABLE_ELT`) is already integrated into the copilot metrics and settings. Extend this to include pipeline health summaries, anomaly detection, and SLA tracking. 【F:common/test_data.py†L83-L220】【F:common/db.py†L214-L333】

### Monitoring & Observability
- Ingestion/Modelling Monitoring pages are placeholder scaffolds that can be filled with:
  - Pipeline DAG status + SLA breaches.
  - Data freshness and volume anomaly tracking.
  - Lineage/impact analysis connected to validation outcomes.
  - dbt model history / test results.

### Governance, Alerting, and Administration
- Governance + Alerting pages establish UI structure for policy enforcement and alert workflows. Integrate these with decision/test outputs and ingestion metadata to provide a full “control plane” view. 【F:pages/governance.py†L1-L86】【F:pages/alerting.py†L1-L92】

---

## Practical Next Steps (Suggested Roadmap)
1. **Persist decisions + validations** in Snowflake tables (vs. session state) and add ownership/audit metadata.
2. **Expand query library** with validation metrics, ingestion SLA checks, and lineage summaries.
3. **Add a migration validation runner** that executes rule packs against target schemas with result persistence.
4. **Integrate alerting** based on validation failures and ingestion anomalies.
5. **Copilot enrichment**: use the query catalog + metadata tables to answer natural language questions with guarded, explainable SQL.
6. **Centralized governance config**: manage roles/tags/data policies and surface drift from expected state.

---

## What to Watch For (Snowflake Constraints)
- Avoid any top-level import of DuckDB or other local-only packages.
- Ensure all SQL statements use Snowflake-compatible syntax with proper terminators.
- Keep runtime detection and import guards intact when adding new integrations.
- Maintain environment parity: only Anaconda-available packages should be used in Snowflake runtime. 【F:common/db.py†L1-L214】【F:environment.yml†L1-L6】
