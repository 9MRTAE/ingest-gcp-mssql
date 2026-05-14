"""
flows/
    ingest_{platform}_{db_type}_{db_name}_{application}.py  — one file per BU/application

Repo    : ingest_gcp_mssql_popcorn
Instance: your-db-hostname  (GCP · MSSQL)

NOTE: Flow granularity = application (BU), not DB — to preserve existing GCS paths
      consumed by downstream ETL.

Registered flows
----------------
- ingest_gcp_mssql_ecom            (DB: ECOM · application: livingmart)
- ingest_gcp_mssql_erp             (DB: ERP  · domain: erp)
- ingest_gcp_mssql_erp_livingmart  (DB: ERP  · domain: livingmart → GCS: livingmart/)
- ingest_gcp_mssql_erp_smartaudit  (DB: ERP  · domain: smartaudit)
- ingest_gcp_mssql_erp_stockkeycard(DB: ERP  · domain: stockkeycard)
- ingest_gcp_mssql_erp_timeattendance(DB: ERP · domain: timeattendance)
- ingest_gcp_mssql_erp_visitor     (DB: ERP  · domain: visitor)

To add a new application/BU:
    1. Create flows/ingest_gcp_mssql_{db_name}_{application}.py
    2. Add import below
    3. Add deployment block in prefect.yaml + deploy.py + run_local.py
"""

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


__all__ = [
    'ingest_gcp_mssql_ecom',
    'ingest_gcp_mssql_erp',
    'ingest_gcp_mssql_erp_livingmart',
    'ingest_gcp_mssql_erp_smartaudit',
    'ingest_gcp_mssql_erp_stockkeycard',
    'ingest_gcp_mssql_erp_timeattendance',
    'ingest_gcp_mssql_erp_visitor',
    'ingest_gcp_mssql_erp_accounting_doc',
    'ingest_gcp_mssql_erp_asset',
    'ingest_gcp_mssql_erp_bookbank',
    'ingest_gcp_mssql_erp_budget',
    'ingest_gcp_mssql_erp_contract_legacy',
    'ingest_gcp_mssql_erp_favoritemenu',
    'ingest_gcp_mssql_erp_gl',
    'ingest_gcp_mssql_erp_invoice',
    'ingest_gcp_mssql_erp_mobile',
    'ingest_gcp_mssql_erp_notice',
    'ingest_gcp_mssql_erp_party',
    'ingest_gcp_mssql_erp_payment',
    'ingest_gcp_mssql_erp_paymentrequest',
    'ingest_gcp_mssql_erp_product',
    'ingest_gcp_mssql_erp_project',
    'ingest_gcp_mssql_erp_receipt',
    'ingest_gcp_mssql_erp_report',
    'ingest_gcp_mssql_erp_role',
    'ingest_gcp_mssql_erp_standardform',
    'ingest_gcp_mssql_erp_transferslip',
    'ingest_gcp_mssql_erp_unit',
    
]