<#
.SYNOPSIS
    Local development script for the Migration Copilot Streamlit app (Windows/PowerShell)

.DESCRIPTION
    Replaces the Makefile for Windows environments. Provides commands to run the app
    with DuckDB or Snowflake, deploy to Snowflake, and manage configuration.

.EXAMPLE
    .\run.ps1 help
    .\run.ps1 run-duckdb
    .\run.ps1 run-snowflake
    .\run.ps1 deploy-snowflake
#>

param(
    [Parameter(Position=0)]
    [ValidateSet('help', 'run-duckdb', 'run-light', 'run-snowflake', 'configure-snowflake', 'print-env', 
                 'deploy-snowflake', 'get-snowflake-url', 'validate-snowflake-env', 
                 'upload-stage', 'clean')]
    [string]$Command = 'help'
)

# Helper function to load env file
function Import-EnvFile {
    param([string]$Path)
    if (Test-Path $Path) {
        Get-Content $Path | ForEach-Object {
            if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
                $name = $matches[1].Trim()
                $value = $matches[2].Trim()
                # Remove surrounding quotes if present
                $value = $value -replace '^["'']|["'']$', ''
                [Environment]::SetEnvironmentVariable($name, $value, 'Process')
            }
        }
    }
}

function Show-Help {
    Write-Host "Available commands:" -ForegroundColor Cyan
    Write-Host "  run-duckdb            Run Streamlit with DuckDB (in-memory)"
    Write-Host "  run-light             Run Streamlit with DuckDB (fast start, no pip install)"
    Write-Host "  run-snowflake         Run Streamlit connecting to Snowflake"
    Write-Host "  configure-snowflake   Configure Snowflake CLI connection (interactive)"
    Write-Host "  print-env             Print loaded env vars (local + snowflake)"
    Write-Host "  deploy-snowflake      Deploy Streamlit app to Snowflake and print URL"
    Write-Host "  get-snowflake-url     Print deployed Streamlit app URL"
    Write-Host "  validate-snowflake-env Validate required Snowflake env vars are set"
    Write-Host "  upload-stage          Upload all app files directly to Snowflake stage"
    Write-Host "  clean                 Remove generated secrets file"
    Write-Host "  help                  Show this help"
}

function Invoke-RunDuckDB {
    Write-Host "Installing dependencies from requirements.txt..." -ForegroundColor Yellow
    pip install -r requirements.txt

    Write-Host "Loading .env.local and .env.duckdb (if present)" -ForegroundColor Yellow
    Import-EnvFile ".env.local"
    Import-EnvFile ".env.duckdb"

    Write-Host "Running Streamlit app with DuckDB (in-memory)" -ForegroundColor Green
    $env:RUNTIME_MODE = "duckdb"
    streamlit run streamlit_app.py
}

function Invoke-RunLight {
    Write-Host "Loading .env.local and .env.duckdb (if present)" -ForegroundColor Yellow
    Import-EnvFile ".env.local"
    Import-EnvFile ".env.duckdb"

    Write-Host "Running Streamlit app with DuckDB (fast start, no pip install)" -ForegroundColor Green
    $env:RUNTIME_MODE = "duckdb"
    streamlit run streamlit_app.py
}

function Test-SnowflakeEnv {
    Import-EnvFile ".env.snowflake-dev"
    
    $required = @('SF_ACCOUNT', 'SF_USER', 'SF_PASSWORD', 'SF_WAREHOUSE', 'SF_DATABASE', 'SF_SCHEMA')
    $missing = @()
    
    foreach ($var in $required) {
        if (-not [Environment]::GetEnvironmentVariable($var, 'Process')) {
            $missing += $var
        }
    }
    
    if ($missing.Count -gt 0) {
        Write-Host "Missing required Snowflake env vars: $($missing -join ', ')" -ForegroundColor Red
        return $false
    }
    
    Write-Host "Snowflake env vars look good." -ForegroundColor Green
    return $true
}

function Invoke-RunSnowflake {
    if (-not (Test-SnowflakeEnv)) {
        return
    }

    Write-Host "Installing dependencies from requirements.txt..." -ForegroundColor Yellow
    pip install -r requirements.txt

    Write-Host "Loading .env.snowflake-dev (if present)" -ForegroundColor Yellow
    Import-EnvFile ".env.snowflake-dev"

    Write-Host "Running Streamlit app with Snowflake..." -ForegroundColor Green
    Write-Host "Requires env vars: SF_ACCOUNT, SF_USER, SF_PASSWORD. Optional: SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE"

    # Create .streamlit directory if it doesn't exist
    if (-not (Test-Path ".streamlit")) {
        New-Item -ItemType Directory -Path ".streamlit" | Out-Null
    }

    # Write secrets.toml
    Write-Host "Preparing .streamlit/secrets.toml from environment variables..." -ForegroundColor Yellow
    @"
[snowflake]
account = "$env:SF_ACCOUNT"
user = "$env:SF_USER"
password = "$env:SF_PASSWORD"
warehouse = "$env:SF_WAREHOUSE"
database = "$env:SF_DATABASE"
schema = "$env:SF_SCHEMA"
role = "$env:SF_ROLE"
"@ | Set-Content ".streamlit/secrets.toml"

    Write-Host "Wrote .streamlit/secrets.toml (check .gitignore before committing)." -ForegroundColor Yellow

    $env:RUNTIME_MODE = "snowflake_local"
    streamlit run streamlit_app.py
}

function Invoke-ConfigureSnowflake {
    Write-Host "Launching Snowflake CLI connection setup..." -ForegroundColor Yellow
    snow connection add

    Import-EnvFile ".env.snowflake-dev"
    if ($env:SF_CLI_CONNECTION) {
        snow connection set-default $env:SF_CLI_CONNECTION
        Write-Host "Set default Snowflake CLI connection to $env:SF_CLI_CONNECTION" -ForegroundColor Green
    } else {
        Write-Host "Tip: set SF_CLI_CONNECTION in .env.snowflake-dev to auto-select a default connection." -ForegroundColor Yellow
    }
}

function Show-Env {
    Import-EnvFile ".env.local"
    Import-EnvFile ".env.duckdb"
    Import-EnvFile ".env.snowflake-dev"

    Write-Host "DEFAULT_DB_MODE=$env:DEFAULT_DB_MODE"
    Write-Host "DUCKDB_DATABASE=$env:DUCKDB_DATABASE"
    Write-Host "DUCKDB_READ_ONLY=$env:DUCKDB_READ_ONLY"
    Write-Host "SF_ACCOUNT=$env:SF_ACCOUNT"
    Write-Host "SF_USER=$env:SF_USER"
    Write-Host "SF_PASSWORD=<hidden>"
    Write-Host "SF_WAREHOUSE=$env:SF_WAREHOUSE"
    Write-Host "SF_DATABASE=$env:SF_DATABASE"
    Write-Host "SF_SCHEMA=$env:SF_SCHEMA"
    Write-Host "SF_ROLE=$env:SF_ROLE"
}

function Invoke-DeploySnowflake {
    Write-Host "Deploying Streamlit app to Snowflake..." -ForegroundColor Yellow
    
    Import-EnvFile ".env.snowflake-dev"
    Import-EnvFile ".env.deploy-dev"

    snow sql -q "USE DATABASE `"$env:SF_DEPLOY_DATABASE`"; USE SCHEMA `"$env:SF_DEPLOY_SCHEMA`";"
    
    snow streamlit deploy `
        --database $env:SF_DEPLOY_DATABASE `
        --schema $env:SF_DEPLOY_SCHEMA `
        --warehouse $env:SF_DEPLOY_WAREHOUSE `
        $env:SF_DEPLOY_APP `
        --replace

    Write-Host "Fetching Streamlit app URL..." -ForegroundColor Yellow
    snow streamlit get-url $env:SF_DEPLOY_APP

    # Share the app with roles from .env.deploy-permissions
    $permissionsFile = ".env.deploy-permissions"
    if (Test-Path $permissionsFile) {
        Write-Host "Sharing Streamlit app with roles from $permissionsFile..." -ForegroundColor Yellow
        Get-Content $permissionsFile | ForEach-Object {
            $line = $_.Trim()
            # Skip empty lines and comments
            if ($line -and -not $line.StartsWith('#')) {
                Write-Host "  Sharing with role: $line" -ForegroundColor Cyan
                snow streamlit share `
                    --database $env:SF_DEPLOY_DATABASE `
                    --schema $env:SF_DEPLOY_SCHEMA `
                    $env:SF_DEPLOY_APP `
                    $line
            }
        }
        Write-Host "App shared with all configured roles." -ForegroundColor Green
    } else {
        Write-Host "No $permissionsFile file found. Skipping role sharing." -ForegroundColor Yellow
    }
}

function Get-SnowflakeUrl {
    Import-EnvFile ".env.deploy-dev"
    snow streamlit get-url $env:SF_DEPLOY_APP
}

function Invoke-UploadStage {
    Write-Host "Uploading files directly to Snowflake stage..." -ForegroundColor Yellow
    
    Import-EnvFile ".env.snowflake-dev"
    Import-EnvFile ".env.deploy-dev"

    $stagePath = "@$env:SF_DEPLOY_DATABASE.$env:SF_DEPLOY_SCHEMA.$env:SF_DEPLOY_STAGE/$env:SF_DEPLOY_APP"
    Write-Host "Target stage: $stagePath" -ForegroundColor Cyan

    snow stage copy streamlit_app.py $stagePath --overwrite
    snow stage copy environment.yml $stagePath --overwrite
    snow stage copy pages "$stagePath/pages" --overwrite --recursive
    snow stage copy common "$stagePath/common" --overwrite --recursive

    Write-Host "All files uploaded to stage. Refresh the Streamlit app in your browser." -ForegroundColor Green
}

function Invoke-Clean {
    Write-Host "Removing generated secrets file" -ForegroundColor Yellow
    if (Test-Path ".streamlit/secrets.toml") {
        Remove-Item ".streamlit/secrets.toml" -Force
    }
    Write-Host "Done" -ForegroundColor Green
}

# Main switch
switch ($Command) {
    'help'                  { Show-Help }
    'run-duckdb'            { Invoke-RunDuckDB }
    'run-light'             { Invoke-RunLight }
    'run-snowflake'         { Invoke-RunSnowflake }
    'configure-snowflake'   { Invoke-ConfigureSnowflake }
    'print-env'             { Show-Env }
    'deploy-snowflake'      { Invoke-DeploySnowflake }
    'get-snowflake-url'     { Get-SnowflakeUrl }
    'validate-snowflake-env'{ Test-SnowflakeEnv | Out-Null }
    'upload-stage'          { Invoke-UploadStage }
    'clean'                 { Invoke-Clean }
}
