"""
deploy.py — Register all flows in this repo as Prefect v3 deployments.

Usage:
    PREFECT_DEPLOY_MODE=1 python deploy.py

To add a new BU / application flow:
    1. Create flows/ingest_gcp_mssql_{db_name}_{application}.py
    2. Import and append to FLOW_OBJECTS below
"""

from __future__ import annotations

# CHANGE POINT: was using Prefect v1/v2 API:
#   from prefect.deployments import Deployment
#   from prefect.infrastructure.container import DockerContainer
#   Deployment.build_from_flow(...).apply()
#
# Rewritten to Prefect v3 pattern: flow_obj.deploy(**kwargs)
# — matches the auth template (ingest_gcp_postgresql_auth).

from config_flows import PREFECT_IMAGE, PREFECT_SCHEDULER_INGEST, PREFECT_WORK_POOL, PREFECT_WORK_QUEUE

# ── Register flows here ───────────────────────────────────────────────────────
from flows.ingest_gcp_mssql_ecom import ingest_gcp_mssql_ecom
from flows.ingest_gcp_mssql_erp import ingest_gcp_mssql_erp
from flows.ingest_gcp_mssql_erp_livingmart import ingest_gcp_mssql_erp_livingmart
from flows.ingest_gcp_mssql_erp_smartaudit import ingest_gcp_mssql_erp_smartaudit
from flows.ingest_gcp_mssql_erp_stockkeycard import ingest_gcp_mssql_erp_stockkeycard
from flows.ingest_gcp_mssql_erp_timeattendance import ingest_gcp_mssql_erp_timeattendance
from flows.ingest_gcp_mssql_erp_visitor import ingest_gcp_mssql_erp_visitor

from flows.ingest_gcp_mssql_erp_accounting_doc import ingest_gcp_mssql_erp_accounting_doc
from flows.ingest_gcp_mssql_erp_asset import ingest_gcp_mssql_erp_asset
from flows.ingest_gcp_mssql_erp_bookbank import ingest_gcp_mssql_erp_bookbank
from flows.ingest_gcp_mssql_erp_budget import ingest_gcp_mssql_erp_budget
from flows.ingest_gcp_mssql_erp_contract_legacy import ingest_gcp_mssql_erp_contract_legacy
from flows.ingest_gcp_mssql_erp_favoritemenu import ingest_gcp_mssql_erp_favoritemenu
from flows.ingest_gcp_mssql_erp_gl import ingest_gcp_mssql_erp_gl
from flows.ingest_gcp_mssql_erp_invoice import ingest_gcp_mssql_erp_invoice
from flows.ingest_gcp_mssql_erp_mobile import ingest_gcp_mssql_erp_mobile
from flows.ingest_gcp_mssql_erp_notice import ingest_gcp_mssql_erp_notice
from flows.ingest_gcp_mssql_erp_party import ingest_gcp_mssql_erp_party
from flows.ingest_gcp_mssql_erp_payment import ingest_gcp_mssql_erp_payment
from flows.ingest_gcp_mssql_erp_paymentrequest import ingest_gcp_mssql_erp_paymentrequest
from flows.ingest_gcp_mssql_erp_product import ingest_gcp_mssql_erp_product
from flows.ingest_gcp_mssql_erp_project import ingest_gcp_mssql_erp_project
from flows.ingest_gcp_mssql_erp_receipt import ingest_gcp_mssql_erp_receipt
from flows.ingest_gcp_mssql_erp_report import ingest_gcp_mssql_erp_report
from flows.ingest_gcp_mssql_erp_role import ingest_gcp_mssql_erp_role
from flows.ingest_gcp_mssql_erp_standardform import ingest_gcp_mssql_erp_standardform
from flows.ingest_gcp_mssql_erp_transferslip import ingest_gcp_mssql_erp_transferslip
from flows.ingest_gcp_mssql_erp_unit import ingest_gcp_mssql_erp_unit

FLOW_OBJECTS = [
    ingest_gcp_mssql_ecom,
    ingest_gcp_mssql_erp,
    ingest_gcp_mssql_erp_livingmart,
    ingest_gcp_mssql_erp_smartaudit,
    ingest_gcp_mssql_erp_stockkeycard,
    ingest_gcp_mssql_erp_timeattendance,
    ingest_gcp_mssql_erp_visitor,
    ingest_gcp_mssql_erp_accounting_doc,
    ingest_gcp_mssql_erp_asset,
    ingest_gcp_mssql_erp_bookbank,
    ingest_gcp_mssql_erp_budget,
    ingest_gcp_mssql_erp_contract_legacy,
    ingest_gcp_mssql_erp_favoritemenu,
    ingest_gcp_mssql_erp_gl,
    ingest_gcp_mssql_erp_invoice,
    ingest_gcp_mssql_erp_mobile,
    ingest_gcp_mssql_erp_notice,
    ingest_gcp_mssql_erp_party,
    ingest_gcp_mssql_erp_payment,
    ingest_gcp_mssql_erp_paymentrequest,
    ingest_gcp_mssql_erp_product,
    ingest_gcp_mssql_erp_project,
    ingest_gcp_mssql_erp_receipt,
    ingest_gcp_mssql_erp_report,
    ingest_gcp_mssql_erp_role,
    ingest_gcp_mssql_erp_standardform,
    ingest_gcp_mssql_erp_transferslip,
    ingest_gcp_mssql_erp_unit,
]
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    for flow_obj in FLOW_OBJECTS:
        kwargs = dict(
            name=flow_obj.name,
            work_pool_name=PREFECT_WORK_POOL,
            image=PREFECT_IMAGE,
            push=False,
            job_variables={'env': {'PREFECT_WORK_QUEUE': PREFECT_WORK_QUEUE}},
        )
        if PREFECT_SCHEDULER_INGEST:
            kwargs['cron'] = PREFECT_SCHEDULER_INGEST
        flow_obj.deploy(**kwargs)
        print(f'Deployed: {flow_obj.name}')