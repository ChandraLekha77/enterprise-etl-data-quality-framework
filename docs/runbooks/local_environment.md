# Local Docker Environment Runbook

## Purpose

This runbook starts the project infrastructure for development and integration testing. The configuration follows production-oriented practices—pinned images, service isolation, required secrets, health checks, persistent volumes, loopback-only published ports, bounded logs, and least-capability container settings—but it is not an internet-facing production deployment.

## Services

| Service | Purpose | Host access | Persistent volume |
|---|---|---|---|
| `crm-postgres` | PostgreSQL CRM source | `127.0.0.1:5433` | `crm_postgres_data` |
| `warehouse-postgres` | PostgreSQL staging, core, control, and quality data | `127.0.0.1:5432` | `warehouse_postgres_data` |
| `airflow-postgres` | Airflow metadata only | Internal network only | `airflow_postgres_data` |
| `billing-sqlserver` | SQL Server billing source | `127.0.0.1:1433` | `sqlserver_data` |
| `billing-sqlserver-init` | Idempotent billing schema, read-only login, and sample-data initialization | None | Uses SQL Server data store |
| `airflow-init` | One-time Airflow migration and administrator creation | None | Uses Airflow metadata store |
| `airflow-api-server` | Airflow API and user interface | `127.0.0.1:8080` | `airflow_logs` |
| `airflow-scheduler` | Scheduling and LocalExecutor task execution | Internal network only | `airflow_logs` |
| `airflow-dag-processor` | DAG parsing | Internal network only | `airflow_logs` |

Airflow uses `LocalExecutor`. This avoids adding a Redis or message-broker dependency that is outside the approved project stack. The project Airflow image extends the pinned official image with the pinned Python dependencies and Microsoft ODBC Driver 18 required by the SQL Server source.

## Prerequisites

- Docker with Compose v2-compatible commands
- Linux-container support
- At least 8 GB of memory allocated to Docker because SQL Server and Airflow run together
- Python available for generating secure values
- Ports 5432, 5433, 1433, and 8080 available on localhost, or alternate values configured in `.env`

## First-time configuration

From the repository root:

```powershell
Copy-Item .env.example .env
```

Generate a Fernet key using the Python environment that contains Airflow dependencies:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Generate independent API and JWT secrets using the Python standard library:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(64))"
python -c "import secrets; print(secrets.token_urlsafe(64))"
```

Edit `.env` and replace every `CHANGE_ME` value. Use different passwords for the CRM database, warehouse, Airflow metadata database, SQL Server administrator, SQL Server read-only account, and Airflow administrator. SQL Server requires a complex password containing uppercase, lowercase, numeric, and symbol characters. For the local SQLCMD bootstrap, do not use a single quote in `MSSQL_APP_USER` or `MSSQL_APP_PASSWORD`.

Never commit `.env`. The repository ignores it by default.

## Validate configuration

This command validates interpolation and the Compose model without starting services:

```powershell
docker compose config --quiet
```

The command must exit successfully. A missing required value produces an error before any container starts.

## Build and initialize

Build the project Airflow image:

```powershell
docker compose build airflow-init
```

The first PostgreSQL startup applies the CRM source and warehouse DDL automatically, including the central documented-rule registry. SQL Server is initialized by `billing-sqlserver-init` after its health check passes.

Initialize Airflow with the one-time migration and administrator creation service:

```powershell
docker compose up airflow-init
```

The command must finish with exit code 0. It is safe to rerun after an Airflow image upgrade because database migrations are designed to be reapplied.

## Start the environment

```powershell
docker compose up -d
docker compose ps
```

Wait until PostgreSQL, SQL Server, the Airflow API server, and the Airflow scheduler report healthy. `billing-sqlserver-init` and `airflow-init` should show a successful exit. The DAG processor does not expose a separate port and is considered ready when its container remains running without restart loops.

Open `http://127.0.0.1:8080` and sign in with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` from the local `.env` file.

## Routine checks

Show container and health state:

```powershell
docker compose ps
```

Follow Airflow scheduler logs:

```powershell
docker compose logs --follow --tail 100 airflow-scheduler
```

Follow a database log without printing credentials:

```powershell
docker compose logs --follow --tail 100 warehouse-postgres
```

Validate that Airflow can parse configured DAGs:

```powershell
docker compose exec airflow-scheduler airflow dags list
```

Run database integration tests against the local services from an environment configured with host addresses and host-side ports. This PowerShell snippet loads the local `.env` into the current process without printing its values, then replaces container addresses with local endpoints:

```powershell
Get-Content .env |
    Where-Object { $_ -match '^\s*[^#][^=]*=' } |
    ForEach-Object {
        $name, $value = $_ -split '=', 2
        [Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim(), 'Process')
    }
$env:RUN_DATABASE_INTEGRATION_TESTS = "1"
$env:CRM_POSTGRES_HOST = "127.0.0.1"
$env:CRM_POSTGRES_PORT = $env:CRM_POSTGRES_HOST_PORT
$env:WAREHOUSE_POSTGRES_HOST = "127.0.0.1"
$env:WAREHOUSE_POSTGRES_PORT = $env:WAREHOUSE_POSTGRES_HOST_PORT
$env:MSSQL_HOST = "127.0.0.1"
$env:MSSQL_PORT = $env:MSSQL_HOST_PORT
.\.venv\Scripts\python.exe -m pytest -m integration
```

The host-side port variables are intentionally separate from the fixed container ports used by Airflow.

## Stop and restart

Stop containers while preserving all database and Airflow data:

```powershell
docker compose down
```

Restart with the existing volumes:

```powershell
docker compose up -d
```

## Destructive reset

The following command permanently removes the CRM, warehouse, Airflow metadata, SQL Server, and Airflow log volumes:

```powershell
docker compose down --volumes
```

Use it only when all local data can be recreated from project fixtures. It is not part of normal troubleshooting.

## Image updates

Image versions are explicit in `.env.example`. Update one image at a time and review its official release notes. After changing an image:

1. Run `docker compose config --quiet`.
2. Pull the changed image with `docker compose pull <service>`.
3. Back up any non-recreatable data.
4. Run `docker compose up airflow-init` for an Airflow upgrade.
5. Start the affected service and verify health and logs.
6. Run the integration and end-to-end tests before accepting the upgrade.

Do not replace pinned image values with `latest`.

## Security and operational limits

- Published ports bind to `127.0.0.1`; do not change them to all interfaces on a shared machine.
- `.env` is suitable only for local development. A deployed environment must inject credentials without committing them to the repository.
- The local Airflow API uses HTTP on loopback. Any nonlocal deployment requires TLS and controlled network access.
- Database volumes provide persistence, not backups. Back up non-recreatable data before upgrades or destructive operations.
- The SQL Server image uses the Developer edition and is limited to development and testing.
- Source and warehouse database accounts are separated. Airflow metadata is stored in a third isolated PostgreSQL service.

## Troubleshooting

### A service remains unhealthy

Inspect only that service first:

```powershell
docker compose ps
docker compose logs --tail 200 <service-name>
```

Confirm that the relevant password is populated and the configured host port is available. Do not paste `.env` contents into logs or issue reports.

### SQL Server exits during startup

Confirm Linux-container mode, sufficient Docker memory, an accepted image architecture, and SQL Server password complexity. SQL Server is pinned to `linux/amd64` because the project image does not provide a native ARM runtime.

### Airflow initialization fails

Confirm `airflow-postgres` is healthy, all three Airflow secrets are populated, and the administrator password is not a placeholder. Then rerun:

```powershell
docker compose up airflow-init
```

### SQL Server initialization fails

Inspect `billing-sqlserver-init` logs, verify both SQL Server passwords meet the complexity policy, and ensure the application username and password do not contain a single quote. The initializer is idempotent and can be rerun safely:

```powershell
docker compose up billing-sqlserver-init
```

### Port conflict

Change only the host-side port in `.env`, then run `docker compose config --quiet` again. Internal service ports must remain unchanged because service-to-service connections use them.
