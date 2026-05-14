"""
run_local.py — รันใน local สำหรับ dev / repair

Examples
--------
Full run (all tables):
    python run_local.py --flow erp

Repair single table:
    python run_local.py --flow erp --table tmCOR
    python run_local.py --flow visitor --table ttVisitor

Backfill date range:
    python run_local.py --flow ecom --startdate 2024-01-01 --enddate 2024-01-31

Override backdate:
    python run_local.py --flow ecom --backdate -3
"""

from __future__ import annotations

import argparse

from config_flows import JOB_BACKDATE, JOB_ENDDATE, JOB_STARTDATE

FLOW_MAP = {
    'ecom':           'flows.ingest_gcp_mssql_ecom:ingest_gcp_mssql_ecom',
    'erp':            'flows.ingest_gcp_mssql_erp:ingest_gcp_mssql_erp',
    'erp_livingmart': 'flows.ingest_gcp_mssql_erp_livingmart:ingest_gcp_mssql_erp_livingmart',
    'smartaudit':     'flows.ingest_gcp_mssql_erp_smartaudit:ingest_gcp_mssql_erp_smartaudit',
    'stockkeycard':   'flows.ingest_gcp_mssql_erp_stockkeycard:ingest_gcp_mssql_erp_stockkeycard',
    'timeattendance': 'flows.ingest_gcp_mssql_erp_timeattendance:ingest_gcp_mssql_erp_timeattendance',
    'visitor':        'flows.ingest_gcp_mssql_erp_visitor:ingest_gcp_mssql_erp_visitor',
    'erp_accounting_doc': 'flows.ingest_gcp_mssql_erp_accounting_doc:ingest_gcp_mssql_erp_accounting_doc',
    'erp_asset':         'flows.ingest_gcp_mssql_erp_asset:ingest_gcp_mssql_erp_asset',
    'erp_bookbank':      'flows.ingest_gcp_mssql_erp_bookbank:ingest_gcp_mssql_erp_bookbank',
    'erp_budget':        'flows.ingest_gcp_mssql_erp_budget:ingest_gcp_mssql_erp_budget',
    'erp_contract_legacy': 'flows.ingest_gcp_mssql_erp_contract_legacy:ingest_gcp_mssql_erp_contract_legacy',
    'erp_favoritemenu':  'flows.ingest_gcp_mssql_erp_favoritemenu:ingest_gcp_mssql_erp_favoritemenu',
    'erp_gl':            'flows.ingest_gcp_mssql_erp_gl:ingest_gcp_mssql_erp_gl',
    'erp_invoice':       'flows.ingest_gcp_mssql_erp_invoice:ingest_gcp_mssql_erp_invoice',
    'erp_mobile':        'flows.ingest_gcp_mssql_erp_mobile:ingest_gcp_mssql_erp_mobile',
    'erp_notice':        'flows.ingest_gcp_mssql_erp_notice:ingest_gcp_mssql_erp_notice',
    'erp_party':         'flows.ingest_gcp_mssql_erp_party:ingest_gcp_mssql_erp_party',
    'erp_payment':       'flows.ingest_gcp_mssql_erp_payment:ingest_gcp_mssql_erp_payment',
    'erp_paymentrequest': 'flows.ingest_gcp_mssql_erp_paymentrequest:ingest_gcp_mssql_erp_paymentrequest',
    'erp_product':       'flows.ingest_gcp_mssql_erp_product:ingest_gcp_mssql_erp_product',
    'erp_project':       'flows.ingest_gcp_mssql_erp_project:ingest_gcp_mssql_erp_project',
    'erp_receipt':       'flows.ingest_gcp_mssql_erp_receipt:ingest_gcp_mssql_erp_receipt',
    'erp_report':        'flows.ingest_gcp_mssql_erp_report:ingest_gcp_mssql_erp_report',
    'erp_role':          'flows.ingest_gcp_mssql_erp_role:ingest_gcp_mssql_erp_role',
    'erp_standardform':  'flows.ingest_gcp_mssql_erp_standardform:ingest_gcp_mssql_erp_standardform',
    'erp_transferslip':  'flows.ingest_gcp_mssql_erp_transferslip:ingest_gcp_mssql_erp_transferslip',
    'erp_unit':          'flows.ingest_gcp_mssql_erp_unit:ingest_gcp_mssql_erp_unit',
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run ingest_gcp_mssql_popcorn flow locally')
    parser.add_argument(
        '--flow',
        required=True,
        choices=list(FLOW_MAP),
        help=f'Flow name: {", ".join(FLOW_MAP)}',
    )
    parser.add_argument('--table',     default='',            help='Repair single table (empty = all)')
    parser.add_argument('--backdate',  default=JOB_BACKDATE,  help='Backdate offset e.g. -3')
    parser.add_argument('--startdate', default=JOB_STARTDATE, help='Start date YYYY-MM-DD')
    parser.add_argument('--enddate',   default=JOB_ENDDATE,   help='End date   YYYY-MM-DD')
    args = parser.parse_args()

    module_path, fn_name = FLOW_MAP[args.flow].split(':')
    module = __import__(module_path, fromlist=[fn_name])
    flow_fn = getattr(module, fn_name)

    print(f'\n▶ Running flow : {fn_name}')
    print(f'  table        : {args.table or "(all)"}')
    print(f'  backdate     : {args.backdate}')
    print(f'  startdate    : {args.startdate or "(none)"}')
    print(f'  enddate      : {args.enddate or "(none)"}')
    print()

    result = flow_fn(
        TABLE_NAME=args.table,
        JOB_BACKDATE_DEPEN=args.backdate,
        JOB_STARTDATE_DEPEN=args.startdate,
        JOB_ENDDATE_DEPEN=args.enddate,
    )
    print('\nResult:', result)