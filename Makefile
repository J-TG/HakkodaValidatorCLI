SHELL := /bin/bash


.PHONY: run-duckdb run-snowflake clean help validate-snowflake-env print-env deploy-snowflake get-snowflake-url configure-snowflake

help: ## Show this help
	@echo "Available commands:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?##' Makefile \
		| awk 'BEGIN {FS = ":.*?##"}; {printf "  %-20s %s\n", $$1, $$2}'

configure-snowflake: ## Configure Snowflake CLI connection (interactive)
	@echo "Launching Snowflake CLI connection setup..."
	@snow connection add
	@set -a; [ -f .env.snowflake-dev ] && . .env.snowflake-dev; set +a; \
	if [ -n "$${SF_CLI_CONNECTION}" ]; then \
		snow connection set-default "$${SF_CLI_CONNECTION}"; \
		echo "Set default Snowflake CLI connection to $${SF_CLI_CONNECTION}"; \
	else \
		echo "Tip: set SF_CLI_CONNECTION in .env.snowflake-dev to auto-select a default connection."; \
	fi

print-env: ## Print loaded env vars (local + snowflake)
	@set -a; [ -f .env.local ] && . .env.local; [ -f .env.duckdb ] && . .env.duckdb; [ -f .env.snowflake-dev ] && . .env.snowflake-dev; set +a; \
	printf '%s\n' \
	"DEFAULT_DB_MODE=$${DEFAULT_DB_MODE}" \
	"DUCKDB_DATABASE=$${DUCKDB_DATABASE}" \
	"DUCKDB_READ_ONLY=$${DUCKDB_READ_ONLY}" \
	"SF_ACCOUNT=$${SF_ACCOUNT}" \
	"SF_USER=$${SF_USER}" \
	"SF_PASSWORD=<hidden>" \
	"SF_WAREHOUSE=$${SF_WAREHOUSE}" \
	"SF_DATABASE=$${SF_DATABASE}" \
	"SF_SCHEMA=$${SF_SCHEMA}" \
	"SF_ROLE=$${SF_ROLE}"

deploy-snowflake: ## Deploy Streamlit app to Snowflake and print URL
	@echo "Deploying Streamlit app to Snowflake..."
	@set -a; [ -f .env.snowflake-dev ] && . .env.snowflake-dev; [ -f .env.deploy-dev ] && . .env.deploy-dev; set +a; \
	snow sql -q "USE DATABASE \"$${SF_DEPLOY_DATABASE}\"; USE SCHEMA \"$${SF_DEPLOY_SCHEMA}\";"; \
	snow streamlit deploy \
		--database "$${SF_DEPLOY_DATABASE}" \
		--schema "$${SF_DEPLOY_SCHEMA}" \
		--warehouse "$${SF_DEPLOY_WAREHOUSE}" \
		"$${SF_DEPLOY_APP}" \
		--replace; \
	echo "Fetching Streamlit app URL..."; \
	snow streamlit get-url "$${SF_DEPLOY_APP}"

get-snowflake-url: ## Print deployed Streamlit app URL
	@set -a; [ -f .env.deploy-dev ] && . .env.deploy-dev; set +a; \
	snow streamlit get-url "$${SF_DEPLOY_APP}"

validate-snowflake-env: ## Validate required Snowflake env vars are set
	@set -a; [ -f .env.snowflake-dev ] && . .env.snowflake-dev; set +a; \
	missing=""; \
	for v in SF_ACCOUNT SF_USER SF_PASSWORD SF_WAREHOUSE SF_DATABASE SF_SCHEMA; do \
		if [ -z "$${!v}" ]; then missing="$$missing $$v"; fi; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "Missing required Snowflake env vars:$$missing"; \
		exit 1; \
	fi; \
	echo "Snowflake env vars look good."

run-duckdb: ## Install deps and run Streamlit with DuckDB (in-memory)
	@echo "Installing dependencies from requirements.txt..."
	@pip install -r requirements.txt
	@echo "Loading .env.local and .env.duckdb (if present)"
	@set -a; [ -f .env.local ] && . .env.local; [ -f .env.duckdb ] && . .env.duckdb; set +a; \
	echo "Running Streamlit app with DuckDB (in-memory)"; \
	RUNTIME_MODE=duckdb streamlit run streamlit_app.py


run-snowflake: validate-snowflake-env ## Install deps and run Streamlit connecting to Snowflake (uses env vars -> .streamlit/secrets.toml)
	@echo "Installing dependencies from requirements.txt..."
	@pip install -r requirements.txt
	@echo "Loading .env.snowflake-dev (if present)"
	@set -a; [ -f .env.snowflake-dev ] && . .env.snowflake-dev; set +a; \
	echo "Running Streamlit app with Snowflake. Requires env vars: SF_ACCOUNT, SF_USER, SF_PASSWORD. Optional: SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE"; \
	mkdir -p .streamlit; \
	echo "Preparing .streamlit/secrets.toml from environment variables..."; \
	printf '%s\n' "[snowflake]" \
		"account = \"$${SF_ACCOUNT}\"" \
		"user = \"$${SF_USER}\"" \
		"password = \"$${SF_PASSWORD}\"" \
		"warehouse = \"$${SF_WAREHOUSE}\"" \
		"database = \"$${SF_DATABASE}\"" \
		"schema = \"$${SF_SCHEMA}\"" \
		"role = \"$${SF_ROLE}\"" \
		> .streamlit/secrets.toml; \
	echo "Wrote .streamlit/secrets.toml (check .gitignore before committing)."; \
	RUNTIME_MODE=snowflake_local streamlit run streamlit_app.py

upload-stage: ## Upload all app files directly to Snowflake stage (workaround when deploy fails)
	@echo "Uploading files directly to Snowflake stage..."
	@set -a; [ -f .env.snowflake-dev ] && . .env.snowflake-dev; [ -f .env.deploy-dev ] && . .env.deploy-dev; set +a; \
	STAGE_PATH="@$${SF_DEPLOY_DATABASE}.$${SF_DEPLOY_SCHEMA}.$${SF_DEPLOY_STAGE}/$${SF_DEPLOY_APP}"; \
	echo "Target stage: $${STAGE_PATH}"; \
	snow stage copy streamlit_app.py "$${STAGE_PATH}" --overwrite; \
	snow stage copy environment.yml "$${STAGE_PATH}" --overwrite; \
	snow stage copy pages "$${STAGE_PATH}/pages" --overwrite --recursive; \
	snow stage copy common "$${STAGE_PATH}/common" --overwrite --recursive; \
	echo "All files uploaded to stage. Refresh the Streamlit app in your browser."

clean: ## Remove generated secrets file
	@echo "Removing generated secrets file"
	@rm -f .streamlit/secrets.toml
	@echo "Done"
