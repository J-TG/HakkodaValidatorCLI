# Migration Copilot - Setup & Deployment Guide

A Streamlit-based application for Snowflake migration, validation, and administration workflows. The app runs locally with DuckDB or Snowflake connectors during development, and deploys to Snowflake Streamlit for production use.

---

## Local Development Setup

### Prerequisites
- **PowerShell 5.1+** (Windows) or **Bash** (Linux/macOS)
- **Python 3.9+**
- **Git**

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd sf_cli_deploy
```

### Step 2: Set Up Python Environment
Using `uv` (recommended for fast dependency resolution):

#### Install `uv` via PowerShell:
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Create and activate virtual environment:
```bash
uv venv
.venv\Scripts\Activate.ps1  # Windows PowerShell
# or
source .venv/bin/activate   # Linux/macOS
```

#### Install dependencies:
```bash
uv pip install -r requirements.txt
```

### Step 3: Run Locally with DuckDB (Fastest)
```bash
make run-duckdb
```
Visit `http://localhost:8501` in your browser.

### Step 4: Run Locally with Snowflake Connector
Create a `.env.snowflake-dev` file with your Snowflake credentials:
```env
SNOWFLAKE_ACCOUNT=<account_identifier>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_WAREHOUSE=<warehouse_name>
SNOWFLAKE_DATABASE=<database_name>
SNOWFLAKE_SCHEMA=<schema_name>
```

Then run:
```bash
make run-snowflake
```

---

## Snowflake CLI Setup (for Deployment)

### Step 1: Install Snowflake CLI via `uv`

#### First, install `uv` (if not already installed):
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Then install the Snowflake CLI:
```powershell
uv tool install snowflake-cli
```

Alternatively, see the [Snowflake CLI GitHub repository](https://github.com/snowflakedb/snowflake-cli) for other installation methods.

### Step 2: Add a Snowflake Connection
```powershell
snow connection add
```
Follow the prompts to enter:
- Connection name (e.g., `dev`, `prod`)
- Account identifier
- Username
- Warehouse
- Database
- Schema

### Step 3: Set as Default Connection (Optional)
```powershell
snow connection set-default <connection_name>
```

### Step 4: Test Connection
```powershell
snow connection test <connection_name>
```

---

## Deployment to Snowflake Streamlit

### Prerequisites
- Snowflake CLI configured with a valid connection (see above)
- An active Snowflake account with Streamlit-enabled warehouse

### Deploy the App
```powershell
snow streamlit deploy --replace
```

The CLI reads deployment configuration from `snowflake.yml` and packages all app files.

### View Live App
After successful deployment, visit your Snowflake account URL with the deployed app path.

### Troubleshooting Deployment

**Host format error:**
```powershell
# Recreate the connection with the correct account format
snow connection remove <connection_name>
snow connection add
```

**Files not updating:**
```powershell
# Force upload all files directly to the Snowflake stage
make upload-stage
```

---

## Local Development Commands

### Makefile Targets

| Command | Purpose |
|---------|---------|
| `make run-duckdb` | Run app locally with DuckDB (fastest) |
| `make run-snowflake` | Run app locally with Snowflake connector |
| `make upload-stage` | Upload all files to Snowflake stage (dev troubleshooting) |
| `make help` | Show all available targets |

### Manual Commands

```bash
# Run with DuckDB
streamlit run streamlit_app.py

# Run with custom Python path
python -m streamlit run streamlit_app.py
```

---

## Project Structure

```
sf_cli_deploy/
├── README.md                          # This file
├── AGENTS.md                          # Architecture & guardrails
├── overview.MD                        # Codebase overview
├── streamlit_app.py                   # App entrypoint/router
├── snowflake.yml                      # Snowflake CLI deployment config
├── environment.yml                    # Conda dependencies (Snowflake)
├── requirements.txt                   # Python dependencies (local dev)
├── Makefile                           # Development commands
│
├── common/                            # Shared libraries
│   ├── __init__.py
│   ├── db.py                          # Runtime-aware DB helpers
│   ├── layout.py                      # Shared UI components
│   ├── navigation.py                  # Page routing config
│   ├── test_data.py                   # Test table definitions
│   ├── ingestion_templates.py         # Ingestion DDL/procedure templates
│   ├── query_library.py               # Reusable SQL queries
│   ├── validation_queries.py          # Validation SQL templates
│   └── hello.py                       # Sample helper
│
├── pages/                             # Streamlit page modules
│   ├── home.py                        # Overview & test data setup
│   ├── ingestion_monitoring.py        # Pipeline monitoring
│   ├── modelling_monitoring.py        # Transformation monitoring
│   ├── governance.py                  # Data governance
│   ├── alerting.py                    # Alert management
│   ├── decisions.py                   # Decision logging
│   ├── admin_copilot.py               # Ingestion copilot
│   ├── query_settings.py              # Query catalog
│   ├── settings.py                    # App configuration
│   ├── compatibility_views.py         # Legacy view projections
│   ├── validation_ingestion.py        # Ingestion validation
│   ├── validation_modelling.py        # Transformation validation
│   ├── reporting_views.py             # Reporting view builder
│   └── ...
│
├── scripts/                           # SQL scripts
│   ├── ELT_JOB_RUN.sql
│   ├── ELT_LOAD_LOG.sql
│   └── ELT_ERROR_LOG.sql
│
└── data/                              # Sample data files
```

---

## Test Data & Setup

### Overview
The app supports two categories of tables:
- **Mock Data**: Sample tables for demos (TESTTABLE, SAMPLE_DATA, DAILY_METRICS)
- **Metadata**: App storage tables (validation logs, team members, config, etc.)

### Setup Options
On the Home page, you can:
1. **Setup All Data** — Creates both mock and metadata tables
2. **Setup Mock Data** — Creates only sample/demo tables
3. **Setup Metadata** — Creates only app configuration tables

### Local Development (DuckDB)
All tables are created in DuckDB (in-memory). They are ephemeral and reset on app restart.

### Snowflake Deployment
- **Metadata tables** are created in `DATA_VAULT_TEMP.MIGRATION` for persistence
- **Mock data tables** are optional; set up via the UI as needed
- Source-to-target mapping tables (SCANDS_*_S2T) reference `DATA_VAULT_DEV.INFO_MART` in production

---

## Runtime Modes

The app auto-detects which runtime to use:

| Mode | Environment | Usage |
|------|-------------|-------|
| `duckdb` | Local | Fast iteration, no Snowflake needed |
| `snowflake_local` | Local | Test with real Snowflake conn |
| `snowflake_deployed` | Snowflake Streamlit | Production |

Detection order:
1. `RUNTIME_MODE` env var
2. Active Snowpark session (indicates Snowflake Streamlit)
3. Snowflake secrets configured
4. Falls back to DuckDB

---

## Key Features

### 🚀 Ingestion Monitoring
Real-time pipeline health and metrics across environments (DEV/TEST/UAT/PROD).

### 🔍 Ingestion Copilot
AI-powered ingestion form builder. Captures source metadata and generates provisioning SQL.

### ⚙️ Compatibility Views
Build legacy view projections that map source-to-target columns, maintaining backward compatibility.

### ✅ Validation Framework
- **Ingestion validation**: Compare source/target row counts and metrics
- **Modelling validation**: Step-by-step data quality checks with audit logging

### 📊 Governance & Alerting
Track decisions, manage validation objects, and set up data quality alerts.

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'duckdb'"
- This error occurs only in Snowflake runtime (where DuckDB is not available).
- Ensure DuckDB imports are lazy (inside functions) and guarded.
- Check that `RUNTIME_MODE != "duckdb"` when accessing local-only code.

### "Unable to retrieve team members" / "Mapping view unavailable"
- Run **Check Table Status** on the Home page to verify tables exist.
- If missing, click **Setup Metadata** (or **Setup All Data**) to create them.

### Snowflake deployment fails with "Host format error"
- Recreate your connection using the correct Snowflake account identifier.
- Try `make upload-stage` as a workaround to directly push files to the stage.

### Local app runs slowly
- Use `make run-duckdb` instead of `snowflake_local` for fastest iteration.
- DuckDB is in-memory and avoids network latency.

---

## Contributing

### Before Committing
- Ensure `environment.yml` lists only Anaconda-available packages.
- Keep `requirements.txt` for local-only dependencies.
- Test locally with both `make run-duckdb` and `make run-snowflake`.
- Verify no top-level imports of DuckDB or local-only packages.

### Adding a New Page
1. Create `pages/my_page.py` with a `main()` function.
2. Import it in `common/navigation.py` and add to page definitions.
3. Use `init_page()` from `common/layout.py` for consistent setup.

### Adding a New Table
1. Define DDL/DML in `common/test_data.py` under `TEST_TABLES`.
2. Categorize it as `MOCK_DATA_TABLES` or `METADATA_TABLES`.
3. The Home page will automatically list it and allow setup.

---

## Additional Resources

- [AGENTS.md](./AGENTS.md) — Architecture, guardrails, and AI guidelines
- [overview.MD](./overview.MD) — Detailed codebase overview
- [Snowflake CLI Docs](https://github.com/snowflakedb/snowflake-cli)
- [Streamlit Docs](https://docs.streamlit.io)
- [Snowpark Python Docs](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)

---

## License & Support

For questions or issues, reach out to the development team or refer to the attached documentation.
