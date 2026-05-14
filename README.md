# ingest_gcp_mssql_popcorn

Ingest pipeline — **GCP · MSSQL · `your-db-hostname` · `ERP` + `ECOM`**

ดึงข้อมูลจาก MSSQL (SQL Server) แล้วเขียนลง GCS Data Lake ในรูปแบบ Parquet แบบ partitioned  
orchestrated ด้วย **Prefect v3** และ run บน **Kubernetes**

---

## Design

```
DB instance  : your-db-hostname   (GCP · MSSQL / SQL Server)
DB names     : ERP (iProp)  ·  ECOM (Livingmart)

Repo boundary = DB instance   → ingest_gcp_mssql_popcorn
Flow boundary = Domain        → ingest_gcp_mssql_{db_name}_{domain}
GCS path root = gs://{bucket}/gcp-storage-parquet/{application}/{table}/
```

> **หมายเหตุ:** flow granularity = domain (ไม่ใช่ DB) เพื่อรักษา GCS path ที่ downstream ETL ใช้อยู่

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
| `ingest_gcp_mssql_erp` *(domain: erp)* | `ERP` | `gcp-ingest-mssql` | 16 |

> **`ingest_gcp_mssql_erp_livingmart`** — Source DB = ERP แต่ load ไปที่ GCS path `livingmart/`  
> เพราะเป็น ERP tables ที่ ETL ฝั่ง livingmart ต้องการ (ต่างจาก `ingest_gcp_mssql_ecom` ซึ่ง source มาจาก ECOM DB)

### ECOM (Livingmart) — `registry_ecom.py`

| Flow | Source DB | GCS Application | Tables |
|---|---|---|:---:|
| `ingest_gcp_mssql_ecom` | `ECOM` | `livingmart` | 16 |

---

## GCS Application → Path Mapping

| GCS Application | GCS Prefix | Flow(s) |
|---|---|---|
| `gcp-ingest-mssql` | `gcp-storage-parquet/gcp-ingest-mssql/` | erp flows ทั้งหมดที่ไม่มี GCS application แยก |
| `livingmart` | `gcp-storage-parquet/livingmart/` | `erp_livingmart`, `ecom` |
| `smartaudit` | `gcp-storage-parquet/smartaudit/` | `erp_smartaudit` |
| `stockkeycard` | `gcp-storage-parquet/stockkeycard/` | `erp_stockkeycard` |
| `timeattendance` | `gcp-storage-parquet/timeattendance/` | `erp_timeattendance` |
| `visitor` | `gcp-storage-parquet/visitor/` | `erp_visitor` |

---

## Project Structure

```
ingest_gcp_mssql_popcorn/
├── flows/
│   ├── __init__.py                                  # export ทุก flow
│   ├── ingest_gcp_mssql_ecom.py                    # ECOM DB · livingmart
│   ├── ingest_gcp_mssql_erp.py                     # ERP · domain: erp (misc)
│   ├── ingest_gcp_mssql_erp_accounting_doc.py      # ERP · domain: accounting_doc
│   ├── ingest_gcp_mssql_erp_asset.py               # ERP · domain: asset
│   ├── ingest_gcp_mssql_erp_bookbank.py            # ERP · domain: bookbank
│   ├── ingest_gcp_mssql_erp_budget.py              # ERP · domain: budget
│   ├── ingest_gcp_mssql_erp_contract_legacy.py     # ERP · domain: contract_legacy
│   ├── ingest_gcp_mssql_erp_favoritemenu.py        # ERP · domain: favoritemenu
│   ├── ingest_gcp_mssql_erp_gl.py                  # ERP · domain: gl
│   ├── ingest_gcp_mssql_erp_invoice.py             # ERP · domain: invoice
│   ├── ingest_gcp_mssql_erp_livingmart.py          # ERP · domain: livingmart → GCS: livingmart/
│   ├── ingest_gcp_mssql_erp_mobile.py              # ERP · domain: mobile
│   ├── ingest_gcp_mssql_erp_notice.py              # ERP · domain: notice
│   ├── ingest_gcp_mssql_erp_party.py               # ERP · domain: party  [_columns support]
│   ├── ingest_gcp_mssql_erp_payment.py             # ERP · domain: payment
│   ├── ingest_gcp_mssql_erp_paymentrequest.py      # ERP · domain: paymentrequest
│   ├── ingest_gcp_mssql_erp_product.py             # ERP · domain: product
│   ├── ingest_gcp_mssql_erp_project.py             # ERP · domain: project
│   ├── ingest_gcp_mssql_erp_receipt.py             # ERP · domain: receipt
│   ├── ingest_gcp_mssql_erp_report.py              # ERP · domain: report
│   ├── ingest_gcp_mssql_erp_role.py                # ERP · domain: role
│   ├── ingest_gcp_mssql_erp_smartaudit.py          # ERP · domain: smartaudit
│   ├── ingest_gcp_mssql_erp_standardform.py        # ERP · domain: standardform
│   ├── ingest_gcp_mssql_erp_stockkeycard.py        # ERP · domain: stockkeycard
│   ├── ingest_gcp_mssql_erp_timeattendance.py      # ERP · domain: timeattendance
│   ├── ingest_gcp_mssql_erp_transferslip.py        # ERP · domain: transferslip
│   ├── ingest_gcp_mssql_erp_unit.py                # ERP · domain: unit  [_columns support]
│   └── ingest_gcp_mssql_erp_visitor.py             # ERP · domain: visitor
│
├── registries/
│   ├── __init__.py
│   ├── _loader.py                                   # load_sql() helper — อ่าน .sql files
│   ├── registry_ecom.py                             # TABLE_REGISTRY สำหรับ ECOM DB
│   └── registry_erp.py                             # TABLE_REGISTRY สำหรับ ERP DB (115 tables, 27 domains)
│
├── tasks/
│   ├── tasks_gcp.py                                 # Prefect @task wrappers (extract / transform / load)
│   └── main_components_gcp.py                      # ETL logic หลัก (T-SQL builder, MSSQL connector, GCS writer)
│
├── config/
│   ├── __init__.py                                  # โหลด env + ดึง secrets จาก GCP Secret Manager
│   ├── production.py                                # config สำหรับ branch main
│   └── development.py                               # config สำหรับ branch develop
│
├── config_flows/
│   └── __init__.py                                  # Prefect / scheduling constants
│
├── sql/
│   └── erp_*.sql                                    # SQL snippets (JOIN / WHERE) สำหรับ ERP tables
│
├── scripts/
│   ├── deploy.sh                                    # Build Docker + register deployments
│   ├── run_local.sh                                 # Run flow ใน local
│   └── clean_repo.sh                                # ล้างไฟล์ชั่วคราว
│
├── run_local.py                                     # CLI wrapper สำหรับรัน flow ใน local
├── deploy.py                                        # Register deployments ผ่าน Prefect v3 Python API
├── prefect.yaml                                     # Prefect deployment definitions (28 deployments)
├── Dockerfile                                       # รวม ODBC Driver 17 installation
└── requirements.txt
```

---

## GCS Output Path

```
gs://{bucket}/gcp-storage-parquet/{application}/{table}/
    calendar_year=YYYY/
        month_no=M/
            day_of_month=D/
                <uuid>-{i}.parquet
```

**ตัวอย่าง:**
```
gs://your-prd-datalake-bucket/gcp-storage-parquet/gcp-ingest-mssql/tmCOR/
    calendar_year=2024/month_no=3/day_of_month=15/abc123-0.parquet

gs://your-prd-datalake-bucket/gcp-storage-parquet/livingmart/ttOrderProduct/
    calendar_year=2024/month_no=3/day_of_month=15/abc123-0.parquet
```

| Branch | GCS Bucket |
|---|---|
| `main` (production) | `your-prd-datalake-bucket` |
| `develop` | `your-nonprd-datalake-bucket` |

---

## ETL Pipeline

```
MSSQL · your-db-hostname (SQL Server)
    │
    ▼  [extract]
    T-SQL query พร้อม date filter (backdate / startdate-enddate)
    Schema inspection ผ่าน INFORMATION_SCHEMA.COLUMNS
    │
    ▼  [transform]
    - bit  → 'true' / 'false'
    - int/bigint/smallint/tinyint → nullable Int64
    - ทุก column → string (ยกเว้น partition cols)
    - normalise nulls (NaT, None, NaN → np.nan)
    - column names → lowercase
    - [_columns] filter สำหรับ tables ที่ระบุ column subset (party, unit)
    │
    ▼  [load]
    เขียน Parquet ลง GCS (partitioned by calendar_year / month_no / day_of_month)
```

**Date column types ที่รองรับ:**

| MSSQL Type | การจัดการ |
|---|---|
| `datetime`, `datetime2`, `date`, `smalldatetime`, `varchar`, `nvarchar`, `char`, `nchar` | ใช้ `YEAR()` / `MONTH()` / `DAY()` ตรงๆ |
| `int`, `bigint`, `smallint`, `tinyint` | wrap ด้วย `DATEADD(s, col, '19700101')` (unix timestamp) |
| ไม่มี date column (`''`) | synthetic timestamp จาก `GETDATE()` → full load ทุกครั้ง |

---

## TABLE_REGISTRY Format (`registry_erp.py`)

```python
TABLE_REGISTRY: dict[str, dict] = {

    # กรณีปกติ — incremental load ด้วย date column
    'tmCOR': {
        '_domain':      'party',
        'p_createdate': 'ftCreateDate',
        'p_updatedate': 'ftUpdateDate',
    },

    # ไม่มี date column → full load ทุกครั้ง (synthetic timestamp)
    'tmCOM': {
        '_domain':      'project',
        'p_createdate': '',
        'p_updatedate': '',
    },

    # _source_table — extract จาก table อื่น แต่ save เป็นชื่อ key
    'tmCOR_Supplier': {
        '_domain':       'party',
        '_source_table': 'tmCOR',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': "fcissupp = 'Y'",
    },

    # _columns — extract ทั้ง table แต่ post-filter เหลือแค่ column ที่ระบุ
    #            (partition cols calendar_year/month_no/day_of_month ถูกเก็บเสมอ)
    'tmCOR_mobileuser': {
        '_domain':       'party',
        '_source_table': 'tmCOR',
        '_columns':      ['fcID', 'fcShipMobile', 'ftCreateDate', 'ftUpdateDate'],
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
    },

    # p_query_join — ใช้ JOIN SQL จาก sql/ folder แทน SELECT *
    'ttVCD': {
        '_domain':          'gl',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttVCD.sql'),
        'p_tablename_join': 'ttVCH',
    },

    # _skip — ข้าม table นี้ใน run loop ทั้งหมด
    'tmObsolete': {
        '_domain': 'erp',
        '_skip':   True,
        'p_createdate': '',
        'p_updatedate': '',
    },

    # p_schema — ถ้าไม่ใช่ dbo
    'dm_exec_query_stats': {
        '_domain':      'erp',
        'p_createdate': 'creation_time',
        'p_updatedate': 'last_execution_time',
        'p_schema':     'sys',
    },
}
```

**Private keys (`_` prefix) — ไม่ถูกส่งไปที่ `extract()`:**

| Key | คำอธิบาย |
|---|---|
| `_domain` | Domain / flow slice ที่ table นี้จัดอยู่ |
| `_source_table` | ชื่อ table จริงที่ extract (ถ้าต่างจาก registry key) |
| `_columns` | Column whitelist หลัง transform (partition cols ถูกเก็บเสมอ) |
| `_skip` | `True` → ข้าม table นี้ใน run loop ทั้งหมด |

---

## Environments & Secrets

Config โหลดตาม `CI_COMMIT_BRANCH`:

| Branch | Config file | GCS Bucket |
|---|---|---|
| `main` | `config/production.py` | `your-prd-datalake-bucket` |
| อื่นๆ | `config/development.py` | `your-nonprd-datalake-bucket` |

Secrets ดึงจาก **GCP Secret Manager** อัตโนมัติ:

| Secret name | ใช้กับ |
|---|---|
| `gcp-your-db-hostname-db-host` | DB host |
| `gcp-your-db-hostname-db-password` | DB password |
| `gcp-dwh-service-account` | GCS credentials (service account JSON) |

---

## Local Development

### Prerequisites

```bash
pip install -r requirements.txt

# MSSQL ODBC Driver 17 ต้อง install ใน OS ด้วย
# macOS: brew install msodbcsql17
# Linux: ดู Dockerfile สำหรับขั้นตอน apt-get

export CI_COMMIT_BRANCH=develop
```

### รัน flow ใน local

```bash
# Full run — รันทุก table ใน domain
python run_local.py --flow erp_party
python run_local.py --flow erp_invoice
python run_local.py --flow ecom

# Repair mode — รัน table เดียว
python run_local.py --flow erp_party --table tmCOR
python run_local.py --flow erp_invoice --table ttODH

# Override backdate (จำนวนวันที่ย้อนหลัง)
python run_local.py --flow erp_gl --backdate -3

# Backfill ช่วงวันที่
python run_local.py --flow erp_receipt --startdate 2024-01-01 --enddate 2024-01-31
```

**ชื่อ `--flow` ทั้งหมดที่ใช้ได้:**

| `--flow` | Domain |
|---|---|
| `ecom` | ECOM · livingmart |
| `erp` | ERP · erp (misc) |
| `erp_accounting_doc` | ERP · accounting_doc |
| `erp_asset` | ERP · asset |
| `erp_bookbank` | ERP · bookbank |
| `erp_budget` | ERP · budget |
| `erp_contract_legacy` | ERP · contract_legacy |
| `erp_favoritemenu` | ERP · favoritemenu |
| `erp_gl` | ERP · gl |
| `erp_invoice` | ERP · invoice |
| `erp_livingmart` | ERP · livingmart |
| `erp_mobile` | ERP · mobile |
| `erp_notice` | ERP · notice |
| `erp_party` | ERP · party |
| `erp_payment` | ERP · payment |
| `erp_paymentrequest` | ERP · paymentrequest |
| `erp_product` | ERP · product |
| `erp_project` | ERP · project |
| `erp_receipt` | ERP · receipt |
| `erp_report` | ERP · report |
| `erp_role` | ERP · role |
| `erp_smartaudit` | ERP · smartaudit |
| `erp_standardform` | ERP · standardform |
| `erp_stockkeycard` | ERP · stockkeycard |
| `erp_timeattendance` | ERP · timeattendance |
| `erp_transferslip` | ERP · transferslip |
| `erp_unit` | ERP · unit |
| `erp_visitor` | ERP · visitor |

หรือรันผ่าน flow file โดยตรง:

```bash
python -m flows.ingest_gcp_mssql_erp_party
python -m flows.ingest_gcp_mssql_erp_party --table tmCOR
python -m flows.ingest_gcp_mssql_erp_party --table tmCOR --backdate -3
```

---

## Scheduling

```
cron : "40 18 * * *"   →  01:40 น. (ICT / Asia/Bangkok)
active: เฉพาะ branch main เท่านั้น
```

ทุก flow ใช้ schedule เดียวกัน — run พร้อมกันทั้ง 28 deployments

---

## Deploy

### ผ่าน script (CI/CD)

```bash
export CI_COMMIT_BRANCH=main
export IMAGE_TAG=asia-southeast1-docker.pkg.dev/your-gcp-project-id/.../ingest-gcp-mssql-popcorn:latest
export PREFECT_WORK_POOL=kubernetes-pool
export PREFECT_WORK_QUEUE=default

bash scripts/deploy.sh
```

### Manual deploy

```bash
# Build image
docker build -t <IMAGE_TAG> .
docker push <IMAGE_TAG>

# Register deployments (28 flows)
PREFECT_DEPLOY_MODE=1 \
CI_COMMIT_BRANCH=main \
IMAGE_TAG=<IMAGE_TAG> \
  prefect deploy --all --prefect-file prefect.yaml
```

> **PREFECT_DEPLOY_MODE=1** — ป้องกัน config จากการดึง secrets จริงระหว่าง deploy step

### Repair via Prefect UI / CLI

```bash
# ตัวอย่าง — repair table เดียว
prefect deployment run \
  'ingest_gcp_mssql_ERP_party/ingest_gcp_mssql_erp_party' \
  -p TABLE_NAME=tmCOR \
  -p JOB_BACKDATE_DEPEN=-3

# Backfill ช่วงวันที่
prefect deployment run \
  'ingest_gcp_mssql_ERP_invoice/ingest_gcp_mssql_erp_invoice' \
  -p TABLE_NAME=ttODH \
  -p JOB_STARTDATE_DEPEN=2024-01-01 \
  -p JOB_ENDDATE_DEPEN=2024-01-31
```

---

## Flow Parameters

| Parameter | Default | คำอธิบาย |
|---|---|---|
| `TABLE_NAME` | `""` | ว่าง = รันทุก table · ระบุชื่อ = repair mode |
| `JOB_BACKDATE_DEPEN` | `"-1"` | จำนวนวันย้อนหลัง (`-1` = ใช้ default จาก config) |
| `JOB_STARTDATE_DEPEN` | `""` | วันเริ่มต้น backfill (YYYY-MM-DD) |
| `JOB_ENDDATE_DEPEN` | `""` | วันสิ้นสุด backfill (YYYY-MM-DD) |

---

## Adding a New Domain

1. **เพิ่ม `_domain` entries ใน `registry_erp.py` หรือ `registry_ecom.py`**

2. **สร้าง flow file ใหม่**
   ```bash
   cp flows/ingest_gcp_mssql_erp_asset.py \
      flows/ingest_gcp_mssql_erp_{domain}.py
   ```
   แก้ค่า:
   - `DOMAIN` → ชื่อ domain ใหม่
   - `GCS_APPLICATION` → ถ้าต้องการ GCS path แยก ให้เปลี่ยนจาก `'gcp-ingest-mssql'`
   - ชื่อ flow function และ docstring
   - ถ้า table บางตัวใน domain ใช้ `_columns` → ให้ copy จาก `erp_party.py` แทน (มี `_PARTITION_COLS` logic)

3. **เพิ่ม import ใน `flows/__init__.py`**

4. **เพิ่ม deployment block ใน `prefect.yaml`**

5. **เพิ่มใน `deploy.py`** — import + append `FLOW_OBJECTS`

6. **เพิ่มใน `run_local.py`** — เพิ่ม entry ใน `FLOW_MAP`

7. **อัปเดต README.md** — เพิ่มแถวใน Registered Flows table

---

## Key Files

| ไฟล์ | หน้าที่ |
|---|---|
| `registries/registry_erp.py` | TABLE_REGISTRY สำหรับ ERP DB (115 tables, 27 domains) |
| `registries/registry_ecom.py` | TABLE_REGISTRY สำหรับ ECOM DB (16 tables) |
| `registries/_loader.py` | `load_sql()` — อ่าน `.sql` snippets จาก `sql/` folder |
| `tasks/main_components_gcp.py` | T-SQL builder, MSSQL connector (pyodbc), GCS writer |
| `tasks/tasks_gcp.py` | Prefect `@task` wrappers (extract / transform / load) |
| `config/__init__.py` | โหลด env, ดึง secrets จาก GCP Secret Manager |
| `config/production.py` | Production constants (bucket, DB, secret names) |
| `config/development.py` | Development constants |
| `config_flows/__init__.py` | Prefect/scheduling constants (work pool, cron) |
| `prefect.yaml` | Deployment definitions (28 deployments) |
| `deploy.py` | Prefect v3 Python API สำหรับ register deployments |
| `run_local.py` | CLI สำหรับรัน flow ใน local (28 flow keys) |