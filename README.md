# ingest_gcp_mssql

Ingest pipeline — **GCP · MSSQL · `your-db-hostname` · `ERP` + `ECOM`**

Extracts data from MSSQL (SQL Server) and writes to GCS Data Lake as hive-partitioned Parquet files
Orchestrated with **Prefect v3** and runs on **Kubernetes**

**Stack:** Python · SQLAlchemy (pyodbc) · pandas · gcsfs · GCP Secret Manager · Prefect v3 · Docker · Jenkins

---

## TL;DR

| | |
|---|---|
| **Source** | MSSQL `your-db-hostname` — DB: `ERP` (130+ tables, 27 domains) + `ECOM` (16 tables) |
| **Sink** | GCS Data Lake · Parquet · hive-partitioned (`calendar_year/month_no/day_of_month`) |
| **Flows** | 28 Prefect v3 deployments · scheduled daily 01:40 ICT |
| **Pattern** | Registry-based monolith — TABLE_REGISTRY is the single source of truth for all tables |
| **Auth** | GCP Secret Manager — no credentials hardcoded in the repo |

---

## Architecture Overview

```
MSSQL (your-db-hostname)
    │  ERP DB (iProp)     ECOM DB (Livingmart)
    │
    │  SQLAlchemy + pyodbc
    ▼
[Extract Task]
    T-SQL builder: date-range filter / JOIN / WHERE clause
    Schema inspection via INFORMATION_SCHEMA.COLUMNS
    │
    ▼
[Transform Task]
    bit → 'true'/'false'
    int/bigint → nullable Int64
    all columns → string (except partition cols)
    normalise nulls (NaT, None, NaN → np.nan)
    column names → lowercase
    [_columns] post-filter for tables that specify a column subset
    │
    ▼
[Load Task]
    write Parquet → GCS (hive partitioning)
    │
    ▼
gs://{bucket}/gcp-storage-parquet/{application}/{table}/
    └── calendar_year=YYYY/month_no=M/day_of_month=D/
        └── {uuid}-0.parquet
```

**Credentials flow:**
- Prefect Worker Node uses **ADC** to authenticate with GCP Secret Manager
- Secret Manager returns DB credentials + SA key → constructs `service_account.Credentials`
- `PREFECT_DEPLOY_MODE=1` → skips Secret Manager during the deploy step (no DB connection required)

---

## Design Decisions

### 1. Registry-based monolith — Why not split into one repo per domain?

This DB instance (`your-db-hostname`) hosts multiple business units on the same database (`ERP`, `ECOM`)
Splitting into per-domain repos would create unjustifiable overhead:

- **Connection overhead:** every repo connects to the same DB instance — no benefit from splitting
- **Config duplication:** secrets, Dockerfile, and requirements.txt duplicated 27+ times
- **Operational cost:** 27 CI/CD pipelines instead of 1

**Decision:** repo boundary = DB instance, flow boundary = domain

```
1 repo (ingest_gcp_mssql)
├── registries/registry_erp.py   ← TABLE_REGISTRY covers ~130 tables across 27 domains
├── registries/registry_ecom.py  ← TABLE_REGISTRY covers 16 tables
└── flows/                       ← 1 file per domain → 28 Prefect deployments
    ├── ingest_gcp_mssql_erp_invoice.py
    ├── ingest_gcp_mssql_erp_party.py
    └── ...
```

**Known trade-offs:**
- If any domain requires separate infra (e.g. different resource limits), that becomes harder to achieve
- The repo accumulates many flow files — strict naming conventions are required to compensate

---

### 2. TABLE_REGISTRY — data-driven config over imperative code

Each table is described by a dict in the registry instead of repeating extraction logic:

```python
# Old approach (imperative) — repeated per table
def extract_tmCOR():
    query = "SELECT * FROM tmCOR WHERE ftUpdateDate >= ..."
    ...

def extract_ttContractD():
    query = "SELECT A.*, B.fcID FROM ttContractD AS A INNER JOIN ttContractH AS B ..."
    ...

# Registry approach — declarative
TABLE_REGISTRY = {
    'tmCOR': {
        '_domain':      'party',
        'p_createdate': 'ftCreateDate',
        'p_updatedate': 'ftUpdateDate',
    },
    'ttContractD': {
        '_domain':          'contract_legacy',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttContractD.sql'),
        'p_tablename_join': 'ttContractH',
    },
}
```

The flow loop iterates through the registry filtering by `_domain` — adding or removing a table never touches flow code

**Private keys (`_` prefix) — not passed to `extract()`:**

| Key | Description |
|---|---|
| `_domain` | Domain / flow slice this table belongs to |
| `_source_table` | Actual source table name to extract (when different from the registry key) |
| `_columns` | Column whitelist applied after transform (partition cols are always retained) |
| `_skip` | `True` → skips this table in all run loops |

---

### 3. SQL snippets extracted from Python into the `sql/` folder

Tables requiring complex JOINs or WHERE clauses store their SQL in separate `sql/*.sql` files
rather than embedding strings in Python:

```python
# Avoided — SQL embedded in Python: hard to edit, hard to test
'p_query_join': "FROM ERP.DBO.ttContractD AS A INNER JOIN ERP.DBO.ttContractH AS B ON B.fcID = A.fcContractHID",

# Used — SQL in a separate file, loaded at import time
'p_query_join': load_sql('erp_join_ttContractD.sql'),
```

`_loader.py` resolves the path relative to the project root — works identically from local and inside the container

**Result:** SQL logic can be edited without touching Python files; git diffs are easier to review

---

### 4. GCS path boundary ≠ flow boundary

Some domains require a separate GCS path (`livingmart/`, `smartaudit/`, `visitor/`, etc.) because
downstream ETL pipelines were already built and depend on those paths

Letting each flow define its own `GCS_APPLICATION` instead of deriving it from the domain name means:
- existing ETL paths remain unchanged (backward compatible)
- new domains can be added by simply specifying the desired `GCS_APPLICATION`

```python
# flows/ingest_gcp_mssql_erp_invoice.py
GCS_APPLICATION = 'gcp-ingest-mssql'   # default GCS path

# flows/ingest_gcp_mssql_erp_visitor.py
GCS_APPLICATION = 'visitor'            # separate path for the visitor domain
```

---

### 5. `PREFECT_DEPLOY_MODE` guard

The deploy step must import `config/__init__.py` to load constants but does not need to
actually connect to the DB. Calling `get_secret()` at import time would fail without GCP credentials
in the CI environment

Solved with an env var guard instead of try/except:

```python
if os.getenv('PREFECT_DEPLOY_MODE'):
    DB_HOST = ''
    DB_PASSWORD = ''
    GCSFS = None
else:
    DB_HOST     = get_secret(os.getenv('SECRET_DB_HOST', SECRET_DB_HOST))
    DB_PASSWORD = get_secret(os.getenv('SECRET_DB_PASSWORD', SECRET_DB_PASSWORD))
    GCSFS       = get_gcsfs()
```

This lets `PREFECT_DEPLOY_MODE=1 python deploy.py` run in CI without requiring DB credentials

---

### 6. Branch → environment mapping

Config is loaded based on `CI_COMMIT_BRANCH` (set by Jenkins):

```python
if APP_ENV == 'main':
    from .production import *   # GCS production bucket
elif APP_ENV == 'develop':
    from .development import *  # GCS non-production bucket
else:
    from .development import *  # fallback → development
```

Secret names are identical across both environments — only the GCS bucket differs
This avoids duplicate secrets and allows promoting from develop → main without changing any secret config

---

## Project Structure

```
ingest_gcp_mssql/
├── flows/
│   ├── __init__.py                                  # exports all flows
│   ├── ingest_gcp_mssql_ecom.py                    # ECOM DB · livingmart
│   ├── ingest_gcp_mssql_erp.py                     # ERP · domain: erp (misc tables)
│   ├── ingest_gcp_mssql_erp_accounting_doc.py
│   ├── ingest_gcp_mssql_erp_asset.py
│   ├── ingest_gcp_mssql_erp_bookbank.py
│   ├── ingest_gcp_mssql_erp_budget.py
│   ├── ingest_gcp_mssql_erp_contract_legacy.py
│   ├── ingest_gcp_mssql_erp_favoritemenu.py
│   ├── ingest_gcp_mssql_erp_gl.py
│   ├── ingest_gcp_mssql_erp_invoice.py
│   ├── ingest_gcp_mssql_erp_livingmart.py          # ERP source → GCS path: livingmart/
│   ├── ingest_gcp_mssql_erp_mobile.py
│   ├── ingest_gcp_mssql_erp_notice.py
│   ├── ingest_gcp_mssql_erp_party.py               # supports _columns whitelist
│   ├── ingest_gcp_mssql_erp_payment.py
│   ├── ingest_gcp_mssql_erp_paymentrequest.py
│   ├── ingest_gcp_mssql_erp_product.py
│   ├── ingest_gcp_mssql_erp_project.py
│   ├── ingest_gcp_mssql_erp_receipt.py
│   ├── ingest_gcp_mssql_erp_report.py
│   ├── ingest_gcp_mssql_erp_role.py
│   ├── ingest_gcp_mssql_erp_smartaudit.py          # GCS path: smartaudit/
│   ├── ingest_gcp_mssql_erp_standardform.py
│   ├── ingest_gcp_mssql_erp_stockkeycard.py        # GCS path: stockkeycard/
│   ├── ingest_gcp_mssql_erp_timeattendance.py      # GCS path: timeattendance/
│   ├── ingest_gcp_mssql_erp_transferslip.py
│   ├── ingest_gcp_mssql_erp_unit.py                # supports _columns whitelist
│   └── ingest_gcp_mssql_erp_visitor.py             # GCS path: visitor/
│
├── registries/
│   ├── _loader.py                                   # load_sql() — reads .sql snippet files
│   ├── registry_erp.py                             # TABLE_REGISTRY for ERP DB (~130 tables, 27 domains)
│   └── registry_ecom.py                            # TABLE_REGISTRY for ECOM DB (16 tables)
│
├── tasks/
│   ├── tasks_gcp.py                                 # Prefect @task wrappers: extract / transform / load
│   └── main_components_gcp.py                      # T-SQL builder, MSSQL connector, GCS writer
│
├── config/
│   ├── __init__.py                                  # env dispatcher + GCP Secret Manager helpers
│   ├── production.py                                # config for branch main
│   └── development.py                               # config for branch develop
│
├── config_flows/
│   └── __init__.py                                  # Prefect infra constants (work pool, cron, image)
│
├── sql/
│   └── erp_*.sql                                    # SQL snippets (JOIN / WHERE) separated from Python
│
├── scripts/
│   ├── deploy.sh                                    # Build Docker image + register deployments
│   ├── run_local.sh                                 # Wrapper: python run_local.py
│   └── clean_repo.sh                               # removes __pycache__ and .pyc files
│
├── run_local.py                                     # CLI: run any flow locally (28 flow keys)
├── deploy.py                                        # Registers deployments via Prefect v3 Python API
├── prefect.yaml                                     # Prefect deployment definitions (28 deployments)
├── Dockerfile                                       # python:3.11-slim + ODBC Driver 17
└── requirements.txt
```

---

## Registered Flows

### ERP (iProp) — `registry_erp.py`

| Flow | Source DB | GCS Application | Tables |
|---|---|---|:---:|
| `ingest_gcp_mssql_erp_accounting_doc` | `ERP` | `gcp-ingest-mssql` | 4 |
| `ingest_gcp_mssql_erp_asset` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_bookbank` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_budget` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_contract_legacy` | `ERP` | `gcp-ingest-mssql` | 3 |
| `ingest_gcp_mssql_erp_favoritemenu` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_gl` | `ERP` | `gcp-ingest-mssql` | 4 |
| `ingest_gcp_mssql_erp_invoice` | `ERP` | `gcp-ingest-mssql` | 4 |
| `ingest_gcp_mssql_erp_livingmart` | `ERP` | `livingmart` | 3 |
| `ingest_gcp_mssql_erp_mobile` | `ERP` | `gcp-ingest-mssql` | 6 |
| `ingest_gcp_mssql_erp_notice` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_party` | `ERP` | `gcp-ingest-mssql` | 7 |
| `ingest_gcp_mssql_erp_payment` | `ERP` | `gcp-ingest-mssql` | 1 |
| `ingest_gcp_mssql_erp_paymentrequest` | `ERP` | `gcp-ingest-mssql` | 3 |
| `ingest_gcp_mssql_erp_product` | `ERP` | `gcp-ingest-mssql` | 11 |
| `ingest_gcp_mssql_erp_project` | `ERP` | `gcp-ingest-mssql` | 5 |
| `ingest_gcp_mssql_erp_receipt` | `ERP` | `gcp-ingest-mssql` | 4 |
| `ingest_gcp_mssql_erp_report` | `ERP` | `gcp-ingest-mssql` | 6 |
| `ingest_gcp_mssql_erp_role` | `ERP` | `gcp-ingest-mssql` | 6 |
| `ingest_gcp_mssql_erp_smartaudit` | `ERP` | `smartaudit` | 6 |
| `ingest_gcp_mssql_erp_standardform` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_stockkeycard` | `ERP` | `stockkeycard` | 2 |
| `ingest_gcp_mssql_erp_timeattendance` | `ERP` | `timeattendance` | 3 |
| `ingest_gcp_mssql_erp_transferslip` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_unit` | `ERP` | `gcp-ingest-mssql` | 2 |
| `ingest_gcp_mssql_erp_visitor` | `ERP` | `visitor` | 5 |
| `ingest_gcp_mssql_erp` *(misc)* | `ERP` | `gcp-ingest-mssql` | 16 |

> **`ingest_gcp_mssql_erp_livingmart`** — Source DB = ERP but loads to `livingmart/`
> These are ERP tables required by the livingmart-side ETL (distinct from `ingest_gcp_mssql_ecom` whose source is the ECOM DB)

### ECOM (Livingmart) — `registry_ecom.py`

| Flow | Source DB | GCS Application | Tables |
|---|---|---|:---:|
| `ingest_gcp_mssql_ecom` | `ECOM` | `livingmart` | 16 |

---

## GCS Application → Path Mapping

| GCS Application | GCS Prefix | Flow(s) |
|---|---|---|
| `gcp-ingest-mssql` | `gcp-storage-parquet/gcp-ingest-mssql/` | all ERP flows without a dedicated GCS application path |
| `livingmart` | `gcp-storage-parquet/livingmart/` | `erp_livingmart`, `ecom` |
| `smartaudit` | `gcp-storage-parquet/smartaudit/` | `erp_smartaudit` |
| `stockkeycard` | `gcp-storage-parquet/stockkeycard/` | `erp_stockkeycard` |
| `timeattendance` | `gcp-storage-parquet/timeattendance/` | `erp_timeattendance` |
| `visitor` | `gcp-storage-parquet/visitor/` | `erp_visitor` |

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CI_COMMIT_BRANCH` | `develop` | `main` → production config, otherwise → development |
| `IMAGE_TAG` | `ingest-gcp-mssql:local` | Docker image tag |
| `PREFECT_DEPLOY_MODE` | *(unset)* | when set → skips Secret Manager during the deploy step |
| `PREFECT_WORK_POOL` | `kubernetes-pool` | Prefect work pool |
| `JOB_BACKDATE` | `-1` | number of days to look back (default) |

### GCP Secret Manager Keys

| Secret key | Used for |
|---|---|
| `your-secret-db-host-key` | DB host |
| `your-secret-db-password-key` | DB password |
| `your-secret-sa-key` | GCS credentials (service account JSON) |

---

## TABLE_REGISTRY Format

```python
TABLE_REGISTRY: dict[str, dict] = {

    # Standard case — incremental load using a date column
    'tmCOR': {
        '_domain':      'party',
        'p_createdate': 'ftCreateDate',
        'p_updatedate': 'ftUpdateDate',
    },

    # No date column → full load every run (synthetic timestamp)
    'tmCOM': {
        '_domain':      'project',
        'p_createdate': '',
        'p_updatedate': '',
    },

    # _source_table — extracts from a different table but saves under the registry key name
    'tmCOR_Supplier': {
        '_domain':       'party',
        '_source_table': 'tmCOR',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': "fcissupp = 'Y'",
    },

    # _columns — extracts the full table but post-filters to the specified columns only
    'tmCOR_mobileuser': {
        '_domain':       'party',
        '_source_table': 'tmCOR',
        '_columns':      ['fcID', 'fcShipMobile', 'ftCreateDate', 'ftUpdateDate'],
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
    },

    # p_query_join — uses JOIN SQL loaded from the sql/ folder
    'ttVCD': {
        '_domain':          'gl',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttVCD.sql'),
        'p_tablename_join': 'ttVCH',
    },

    # _skip — excludes this table from the run loop
    'tmObsolete': {
        '_domain': 'erp',
        '_skip':   True,
        'p_createdate': '',
        'p_updatedate': '',
    },
}
```

**Supported date column types:**

| MSSQL Type | Handling |
|---|---|
| `datetime`, `datetime2`, `date`, `varchar`, `nvarchar` | uses `YEAR()` / `MONTH()` / `DAY()` directly |
| `int`, `bigint`, `smallint`, `tinyint` | wrapped with `DATEADD(s, col, '19700101')` (unix timestamp conversion) |
| no date column (`''`) | synthetic timestamp from `GETDATE()` → full load every run |

---

## Local Development

```bash
pip install -r requirements.txt

# macOS: brew install msodbcsql17
# Linux: see Dockerfile

export CI_COMMIT_BRANCH=develop

# Full run — processes all tables in the domain
python run_local.py --flow erp_party
python run_local.py --flow erp_invoice
python run_local.py --flow ecom

# Repair mode — run a single table
python run_local.py --flow erp_party --table tmCOR

# Override backdate
python run_local.py --flow erp_gl --backdate -3

# Backfill a date range
python run_local.py --flow erp_receipt --startdate 2024-01-01 --enddate 2024-01-31
```

---

## Scheduling

```
cron : "40 18 * * *"   →  01:40 (ICT / Asia/Bangkok)
active: branch main only
```

All flows share the same schedule — all 28 deployments run concurrently

---

## Adding a New Domain

1. Add entries to `registry_erp.py` or `registry_ecom.py` with the new `_domain` value
2. Copy a flow file: `cp flows/ingest_gcp_mssql_erp_asset.py flows/ingest_gcp_mssql_erp_{domain}.py`
3. Update `DOMAIN`, `GCS_APPLICATION`, and the function name
4. Add the import to `flows/__init__.py`
5. Add a deployment block to `prefect.yaml`
6. Add to `deploy.py` (import + `FLOW_OBJECTS`)
7. Add to `run_local.py` (`FLOW_MAP`)
8. Update this README