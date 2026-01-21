import os
from pathlib import Path
import streamlit as st
from common.hello import say_hello

import importlib
import pandas as pd

def load_env_file(env_path: Path) -> None:
	if not env_path.exists():
		return
	for line in env_path.read_text().splitlines():
		line = line.strip()
		if not line or line.startswith("#") or "=" not in line:
			continue
		key, value = line.split("=", 1)
		key = key.strip()
		value = value.strip().strip('"').strip("'")
		os.environ.setdefault(key, value)


# Load local env files for prefills (does not override existing env)
load_env_file(Path(".env.local"))
load_env_file(Path(".env.snowflake-dev"))
load_env_file(Path(".env.duckdb"))

st.title(f"Example streamlit app. {say_hello()}")

# Sidebar: choose database mode
st.sidebar.header("Database Configuration")
is_snowflake_runtime = any(
	os.getenv(var) for var in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE"]
)
if is_snowflake_runtime:
	db_options = ["Snowflake (remote)"]
	default_index = 0
	st.sidebar.info("DuckDB is local-only and not available in Streamlit in Snowflake.")
else:
	db_options = ["DuckDB (local, in-memory)", "Snowflake (remote)"]
	default_mode = os.getenv("DEFAULT_DB_MODE", "duckdb").lower()
	default_index = 1 if default_mode.startswith("snow") else 0

db_mode = st.sidebar.selectbox("Choose database", db_options, index=default_index)

# initialize persistent session state for connection and type
if "db_conn" not in st.session_state:
	st.session_state["db_conn"] = None
if "db_type" not in st.session_state:
	st.session_state["db_type"] = None

if db_mode.startswith("DuckDB"):
	st.session_state["db_type"] = "duckdb"
	try:
		import duckdb
	except Exception:
		st.sidebar.error("DuckDB is not installed. Install with `pip install duckdb` and restart.")
		st.stop()

	if st.session_state["db_conn"] is None or st.session_state.get("db_type") != "duckdb":
		duckdb_database = os.getenv("DUCKDB_DATABASE", ":memory:")
		duckdb_read_only = os.getenv("DUCKDB_READ_ONLY", "false").lower() in {"1", "true", "yes"}
		try:
			st.session_state["db_conn"] = duckdb.connect(database=duckdb_database, read_only=duckdb_read_only)
		except Exception as e:
			st.sidebar.error(f"Failed to create DuckDB connection: {e}")
			st.stop()

	if st.sidebar.checkbox("Create sample table and data", value=True):
		try:
			st.session_state["db_conn"].execute("CREATE TABLE IF NOT EXISTS sample (id INTEGER, name TEXT, value DOUBLE)")
			count = st.session_state["db_conn"].execute("SELECT count(*) FROM sample").fetchone()[0]
			if count == 0:
				st.session_state["db_conn"].execute("INSERT INTO sample VALUES (1,'alice',1.23),(2,'bob',4.56),(3,'carol',7.89)")
		except Exception as e:
			st.sidebar.warning(f"Could not create sample data: {e}")

	# Ensure TESTTABLE exists in DuckDB for local parity
	try:
		st.session_state["db_conn"].execute(
			"""
			CREATE TABLE IF NOT EXISTS TESTTABLE (
				CC_CALL_CENTER_SK BIGINT,
				CC_CALL_CENTER_ID VARCHAR,
				CC_REC_START_DATE DATE,
				CC_REC_END_DATE DATE,
				CC_CLOSED_DATE_SK BIGINT,
				CC_OPEN_DATE_SK BIGINT,
				CC_NAME VARCHAR,
				CC_CLASS VARCHAR,
				CC_EMPLOYEES BIGINT,
				CC_SQ_FT BIGINT,
				CC_HOURS VARCHAR,
				CC_MANAGER VARCHAR,
				CC_MKT_ID BIGINT,
				CC_MKT_CLASS VARCHAR,
				CC_MKT_DESC VARCHAR,
				CC_MARKET_MANAGER VARCHAR,
				CC_DIVISION BIGINT,
				CC_DIVISION_NAME VARCHAR,
				CC_COMPANY BIGINT,
				CC_COMPANY_NAME VARCHAR,
				CC_STREET_NUMBER VARCHAR,
				CC_STREET_NAME VARCHAR,
				CC_STREET_TYPE VARCHAR,
				CC_SUITE_NUMBER VARCHAR,
				CC_CITY VARCHAR,
				CC_COUNTY VARCHAR,
				CC_STATE VARCHAR,
				CC_ZIP VARCHAR,
				CC_COUNTRY VARCHAR,
				CC_GMT_OFFSET DOUBLE,
				CC_TAX_PERCENTAGE DOUBLE
			)
			"""
		)
	except Exception as e:
		st.sidebar.warning(f"Could not create DuckDB TESTTABLE: {e}")

elif db_mode.startswith("Snowflake"):
	st.session_state["db_type"] = "snowflake"
	try:
		snow_secrets = st.secrets.get("snowflake", {})
	except Exception:
		snow_secrets = {}
	# Prefill sidebar inputs with provided defaults if not set in Streamlit secrets
	sf_account = st.sidebar.text_input(
		"Account",
		value=snow_secrets.get("account", os.getenv("SF_ACCOUNT", "")),
	)
	sf_user = st.sidebar.text_input(
		"User",
		value=snow_secrets.get("user", os.getenv("SF_USER", "")),
	)
	sf_password = st.sidebar.text_input(
		"Password",
		type="password",
		value=snow_secrets.get("password", os.getenv("SF_PASSWORD", "")),
	)
	sf_warehouse = st.sidebar.text_input(
		"Warehouse",
		value=snow_secrets.get("warehouse", os.getenv("SF_WAREHOUSE", "")),
	)
	sf_database = st.sidebar.text_input(
		"Database",
		value=snow_secrets.get("database", os.getenv("SF_DATABASE", "")),
	)
	sf_schema = st.sidebar.text_input(
		"Schema",
		value=snow_secrets.get("schema", os.getenv("SF_SCHEMA", "")),
	)
	sf_role = st.sidebar.text_input("Role", value=snow_secrets.get("role", os.getenv("SF_ROLE", "")))

	if st.sidebar.button("Connect to Snowflake"):
		try:
			import snowflake.connector
		except Exception:
			st.sidebar.error("snowflake-connector-python is not installed. Install with `pip install snowflake-connector-python`.")
			st.stop()

		try:
			sf_conn = snowflake.connector.connect(
				account=sf_account,
				user=sf_user,
				password=sf_password,
				warehouse=sf_warehouse or None,
				database=sf_database or None,
				schema=sf_schema or None,
				role=sf_role or None,
			)
			st.session_state["db_conn"] = sf_conn
			st.session_state["db_type"] = "snowflake"
			st.sidebar.success("Connected to Snowflake")
		except Exception as e:
			st.sidebar.error(f"Failed to connect to Snowflake: {e}")

# Main: query input and execution
default_query = "SELECT * FROM sample LIMIT 50" if st.session_state.get("db_type") == "duckdb" else "SHOW TABLES"
query = st.text_area("SQL query", value=default_query, height=200)

if st.button("Run Query"):
	# Ensure we have an active connection; for Snowflake attempt to connect lazily
	if st.session_state.get("db_conn") is None:
		if st.session_state.get("db_type") == "snowflake":
			try:
				import snowflake.connector
				st.session_state["db_conn"] = snowflake.connector.connect(
					account=sf_account,
					user=sf_user,
					password=sf_password,
					warehouse=sf_warehouse or None,
					database=sf_database or None,
					schema=sf_schema or None,
					role=sf_role or None,
				)
				st.sidebar.success("Connected to Snowflake")
			except Exception as e:
				st.error(f"No active database connection and lazy connect failed: {e}")
				st.stop()
		else:
			st.error("No active database connection. Configure the database in the sidebar.")
			st.stop()

	try:
		if st.session_state.get("db_type") == "duckdb":
			df = st.session_state["db_conn"].execute(query).fetchdf()
			st.dataframe(df)
			# Show TESTTABLE if available
			try:
				test_df = st.session_state["db_conn"].execute("SELECT * FROM TESTTABLE LIMIT 100").fetchdf()
				st.subheader("DuckDB: TESTTABLE preview")
				st.dataframe(test_df)
				if not test_df.empty and "CC_EMPLOYEES" in test_df.columns:
					chart_df = test_df[["CC_CALL_CENTER_ID", "CC_EMPLOYEES"]].dropna()
					st.bar_chart(chart_df, x="CC_CALL_CENTER_ID", y="CC_EMPLOYEES")
			except Exception as e:
				st.info(f"DuckDB TESTTABLE not available yet: {e}")
		elif st.session_state.get("db_type") == "snowflake":
			cur = st.session_state["db_conn"].cursor()
			try:
				cur.execute(query)
				try:
					df = cur.fetch_pandas_all()
				except Exception:
					rows = cur.fetchall()
					cols = [c[0] for c in cur.description]
					df = pd.DataFrame(rows, columns=cols)
				st.dataframe(df)

				# Show Snowflake TESTTABLE preview + simple chart
				try:
					cur.execute("SELECT * FROM DATA_VAULT_DEV.INFO_MART.TESTTABLE LIMIT 100")
					try:
						test_df = cur.fetch_pandas_all()
					except Exception:
						rows = cur.fetchall()
						cols = [c[0] for c in cur.description]
						test_df = pd.DataFrame(rows, columns=cols)
					st.subheader("Snowflake: DATA_VAULT_DEV.INFO_MART.TESTTABLE preview")
					st.dataframe(test_df)
					if not test_df.empty and "CC_EMPLOYEES" in test_df.columns:
						chart_df = test_df[["CC_CALL_CENTER_ID", "CC_EMPLOYEES"]].dropna()
						st.bar_chart(chart_df, x="CC_CALL_CENTER_ID", y="CC_EMPLOYEES")
				except Exception as e:
					st.info(f"Snowflake TESTTABLE not available yet: {e}")
			finally:
				cur.close()
		else:
			st.error("Unsupported connection type")
	except Exception as e:
		st.error(f"Query failed: {e}")

st.markdown("---")
st.write("Database mode:", db_mode)

