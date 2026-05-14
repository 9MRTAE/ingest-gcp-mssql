"""
Flow  : ingest_gcp_mssql_erp_party
Repo  : ingest_gcp_mssql_popcorn
DB    : GCP · MSSQL · your-db-hostname · ERP (iProp)

GCS output path:
    {bucket}/gcp-storage-parquet/gcp-ingest-mssql/{table}/
    calendar_year=YYYY/month_no=M/day_of_month=D/

Run all tables (normal / scheduled):
    python -m flows.ingest_gcp_mssql_erp_party

Repair single table:
    python -m flows.ingest_gcp_mssql_erp_party --table <TABLE_NAME>
    python -m flows.ingest_gcp_mssql_erp_party --table <TABLE_NAME> --backdate -3
"""

from __future__ import annotations

import argparse

from prefect import flow
from prefect.logging import get_run_logger

from config_flows import DB_TYPE, JOB_BACKDATE, JOB_ENDDATE, JOB_STARTDATE, PLATFORM
from registries.registry_erp import TABLE_REGISTRY, _PARTITION_COLS
from tasks.tasks_gcp import extract, load, transform

# ── Identity ──────────────────────────────────────────────────────────────────
DOMAIN          = 'party'
DB_NAME         = 'ERP'
GCS_APPLICATION = 'gcp-ingest-mssql'
FLOW_NAME       = f'ingest_{PLATFORM}_{DB_TYPE}_{DB_NAME}_{DOMAIN}'


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name=FLOW_NAME, log_prints=True)
def ingest_gcp_mssql_erp_party(
    TABLE_NAME: str = '',
    JOB_BACKDATE_DEPEN: str = JOB_BACKDATE,
    JOB_STARTDATE_DEPEN: str = JOB_STARTDATE,
    JOB_ENDDATE_DEPEN: str = JOB_ENDDATE,
) -> dict[str, str]:
    """Ingest ERP tables (domain: party) → GCS Data Lake (path: gcp-ingest-mssql/).

    Parameters
    ----------
    TABLE_NAME : str
        ว่าง = รันทุก table (normal run)
        ระบุชื่อ = repair mode สำหรับ table นั้น
    JOB_BACKDATE_DEPEN : str
        Backdate offset (negative int). '-1' = ใช้ default จาก config
    JOB_STARTDATE_DEPEN / JOB_ENDDATE_DEPEN : str
        ระบุช่วงวันที่ (YYYY-MM-DD) สำหรับ backfill
    """
    logger = get_run_logger()

    effective_backdate = JOB_BACKDATE
    if str(JOB_BACKDATE_DEPEN) != '-1':
        effective_backdate = JOB_BACKDATE_DEPEN

    # ── Resolve tables ────────────────────────────────────────────────
    domain_registry = {
        k: v for k, v in TABLE_REGISTRY.items()
        if v.get('_domain') == DOMAIN and not v.get('_skip')
    }

    if TABLE_NAME:
        if TABLE_NAME not in domain_registry:
            raise ValueError(
                f"Unknown table '{TABLE_NAME}'. "
                f"Registered (domain={DOMAIN}): {', '.join(domain_registry)}"
            )
        tables_to_run = [TABLE_NAME]
        logger.info('🔧 Repair mode — table: %s', TABLE_NAME)
    else:
        tables_to_run = list(domain_registry.keys())
        logger.info('▶ Full run — %d tables', len(tables_to_run))

    # ── ETL loop ──────────────────────────────────────────────────────
    results: dict[str, str] = {}
    for table in tables_to_run:
        logger.info('── [%s] start ──', table)
        cfg = domain_registry[table]

        source_table   = cfg.get('_source_table', table)
        columns        = cfg.get('_columns', None)
        extract_params = {k: v for k, v in cfg.items() if not k.startswith('_')}

        df_extracted   = extract(
            DB_NAME, source_table,
            p_backdate=effective_backdate,
            p_startdate=JOB_STARTDATE_DEPEN,
            p_enddate=JOB_ENDDATE_DEPEN,
            **extract_params,
        )
        df_transformed = transform(df_extracted)

        if columns:
            lower_map      = {c.lower(): c for c in df_transformed.columns}
            cols_keep      = list(_PARTITION_COLS | set(columns))
            keep           = [lower_map[c.lower()] for c in cols_keep if c.lower() in lower_map]
            df_transformed = df_transformed[keep]

        results[table] = load(
            p_tablename=table,
            p_dataframe=df_transformed,
            p_application=GCS_APPLICATION,
        )
        logger.info('── [%s] → %s ──', table, results[table])

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=f'Run flow: {FLOW_NAME}')
    parser.add_argument('--table',     default='',            help='Repair single table (empty = all)')
    parser.add_argument('--backdate',  default=JOB_BACKDATE,  help='Backdate offset e.g. -3')
    parser.add_argument('--startdate', default=JOB_STARTDATE, help='Start date YYYY-MM-DD')
    parser.add_argument('--enddate',   default=JOB_ENDDATE,   help='End date   YYYY-MM-DD')
    args = parser.parse_args()

    ingest_gcp_mssql_erp_party(
        TABLE_NAME=args.table,
        JOB_BACKDATE_DEPEN=args.backdate,
        JOB_STARTDATE_DEPEN=args.startdate,
        JOB_ENDDATE_DEPEN=args.enddate,
    )
