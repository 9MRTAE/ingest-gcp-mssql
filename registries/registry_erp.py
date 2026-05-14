"""
TABLE_REGISTRY for ERP (iProp) — MSSQL source.

Registry key  : target table name used in load() / GCS path
Registry value: extract() kwargs + optional private keys:
  _domain       : domain / flow slice this table belongs to
  _source_table : actual DB table to extract from  (default = key)
  _columns      : column whitelist after transform  (partition cols always kept)
  _skip         : True → exclude from all run loops

Domain → GCS_APPLICATION mapping (single source of truth)
──────────────────────────────────────────────────────────
  GCS = gcp-ingest-mssql
    asset, bookbank, budget, contract_legacy, erp,
    favoritemenu, gl, invoice, mobile, notice,
    party, payment, paymentrequest, product, project,
    receipt, report, role, standardform, stockkeycard, transferslip, unit

  GCS = livingmart       → livingmart  (source DB = ERP)
  GCS = smartaudit       → smartaudit
  GCS = stockkeycard     → stockkeycard
  GCS = timeattendance   → timeattendance
  GCS = visitor          → visitor

SQL JOIN clauses are stored in sql/erp_join_<table>.sql and loaded at import
time via load_sql() so they can be edited without touching Python files.

Special-table notes
───────────────────
A) Column-subset (_source_table + _columns)
   tmCOR_mobileuser, tmRoomH_mobileuser, ttCORHist_mobileuser
   → extract full source table → post-filter to _columns + partition cols

B) WHERE-filter (_source_table + p_where_extra)
   tmCOR_livingmart : tmCOR WHERE fcisecom='Y' AND fcissupp='Y'
   tmCOR_Supplier   : tmCOR WHERE fcissupp='Y'
   _LOS tables      : filtered to LOS company subset via erp_where_*.sql

C) System DMV (p_schema='sys')
   dm_exec_query_stats → INFORMATION_SCHEMA returns empty (safe, no casting)
"""

from __future__ import annotations

from registries._loader import load_sql

# Partition columns — always preserved after _columns filtering
_PARTITION_COLS: frozenset[str] = frozenset({'calendar_year', 'month_no', 'day_of_month'})

TABLE_REGISTRY: dict[str, dict] = {

    # ══════════════════════════════════════════════════════════════════
    # GCS_APPLICATION = gcp-ingest-mssql
    # ══════════════════════════════════════════════════════════════════

    # ── asset ─────────────────────────────────────────────────────────
    'asset_source': {'_domain': 'asset', 'p_createdate': '', 'p_updatedate': ''},
    'tmAST':        {'_domain': 'asset', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},

    # ── bookbank ──────────────────────────────────────────────────────
    'tmBKB': {'_domain': 'bookbank', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmBNK': {'_domain': 'bookbank', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},

    # ── budget ────────────────────────────────────────────────────────
    'BudgetDetail': {'_domain': 'budget', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'BudgetHeader': {'_domain': 'budget', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},

    # ── contract_legacy ───────────────────────────────────────────────
    'ttContractH':       {'_domain': 'contract_legacy', 'p_createdate': 'ftCreateDate',    'p_updatedate': 'ftUpdateDate'},
    'ttContractMonitor': {'_domain': 'contract_legacy', 'p_createdate': 'CalculateDate',   'p_updatedate': 'CalculateDate'},
    'ttContractD': {
        '_domain':          'contract_legacy',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttContractD.sql'),
        'p_tablename_join': 'ttContractH',
    },

    # ── erp (misc / uncategorized) ────────────────────────────────────
    'tmBusinessUnit':         {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmCountry':              {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmEMP':                  {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~460K rows
    'tmIDT':                  {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmJuristicSettings':     {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmMeter':                {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~530K rows
    'tmMISC':                 {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmMNU':                  {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmPartner':              {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmPostCode':             {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmProjectManagementCompany': {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmRevenue':              {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'ttLbReception':          {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'ttLOS_COM':              {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'ttLOG_MapPrepaid':       {'_domain': 'erp', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftCreateDate'}, # total ~950K rows
    'dm_exec_query_stats': {
        '_domain':      'erp',
        'p_createdate': 'creation_time',
        'p_updatedate': 'last_execution_time',
        'p_schema':     'sys',
    },

    # ── favoritemenu ──────────────────────────────────────────────────
    'UserFavoriteMenu':       {'_domain': 'favoritemenu', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'UserFavoriteReportMenu': {'_domain': 'favoritemenu', 'p_createdate': 'CreateDate', 'p_updatedate': 'CreateDate'},  # no UpdateDate col

    # ── gl (general ledger) ───────────────────────────────────────────
    'tmACC': {'_domain': 'gl', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~800K rows
    'tmGLB': {'_domain': 'gl', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~23K rows
    'ttVCH': {'_domain': 'gl', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~1.8M rows
    'ttVCD': { # total ~15.2M rows
        '_domain':          'gl',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttVCD.sql'),
        'p_tablename_join': 'ttVCH', #
    }, 

    # ── accounting_doc (accounting doc) ───────────────────────────────────────────
    'tmDCB': {'_domain': 'accounting_doc', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~200K rows
    'tmDCB_LOS': {
        '_domain':       'accounting_doc',
        '_source_table': 'tmDCB',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': load_sql('erp_where_tmDCB_LOS.sql'), # total ~200K rows
    },
    'tmDCT': {'_domain': 'accounting_doc', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~166K rows
    'tmDCT_LOS': {
        '_domain':       'accounting_doc',
        '_source_table': 'tmDCT',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': load_sql('erp_where_tmDCT_LOS.sql'), # total ~166K rows
    },

    # ── invoice ───────────────────────────────────────────────────────
    'ttODH': {'_domain': 'invoice', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftCreateDate'}, # total ~25.3M rows
    'ttODH_LOS': {
        '_domain':       'invoice',
        '_source_table': 'ttODH',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftCreateDate',
        'p_where_extra': load_sql('erp_where_fcDCBID_LOS.sql'),
    },
    'ttODD': { # total ~56.5M rows
        '_domain':          'invoice',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttODD.sql'),
        'p_tablename_join': 'ttODH',
    },
    'ttODD_LOS': {
        '_domain':          'invoice',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttODD_LOS.sql'),
        'p_tablename_join': 'ttODH',
    },

    # ── mobile ────────────────────────────────────────────────────────
    'tmMobileAddressType':  {'_domain': 'mobile', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmMobileCountry':      {'_domain': 'mobile', 'p_createdate': '',              'p_updatedate': ''},
    'tmMobileGender':       {'_domain': 'mobile', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmMobileSettings':     {'_domain': 'mobile', 'p_createdate': '',              'p_updatedate': ''},
    'tmmobileuser':         {'_domain': 'mobile', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmMobileUser_Address': {'_domain': 'mobile', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},

    # ── notice ────────────────────────────────────────────────────────
    'NoticePrintSettingDetail': {'_domain': 'notice', 'p_createdate': '',           'p_updatedate': ''},
    'NoticePrintSettingHeader': {'_domain': 'notice', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},

    # ── party ─────────────────────────────────────────────────────────
    'tmCOR':          {'_domain': 'party', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~1M rows
    'tmNationality':  {'_domain': 'party', 'p_createdate': '',              'p_updatedate': ''},
    'ttCORHist':      {'_domain': 'party', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~1M rows
    'vmMobileUser_Temp': {'_domain': 'party', 'p_createdate': '',           'p_updatedate': ''}, # total ~350K rows
    'tmCOR_mobileuser': {
        '_domain':       'party',
        '_source_table': 'tmCOR',
        '_columns':      ['fcID', 'fcShipMobile', 'ftCreateDate', 'ftUpdateDate'],
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate', # total ~1M rows
    },
    'ttCORHist_mobileuser': {
        '_domain':       'party',
        '_source_table': 'ttCORHist',
        '_columns':      ['fcID', 'fcCORNewRentalID', 'fcType', 'fcRoomID',
                          'ftCreateDate', 'ftUpdateDate'],
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate', # total ~1M rows
    },
    'tmCOR_Supplier': {
        '_domain':       'party',
        '_source_table': 'tmCOR',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': "fcissupp = 'Y'", # total ~1M rows
    },

    # ── payment ───────────────────────────────────────────────────────
    'PaymentMethodSetting': {'_domain': 'payment', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},

    # ── paymentrequest ────────────────────────────────────────────────
    'PaymentRequestHeader': {'_domain': 'paymentrequest', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'PaymentRequestType':   {'_domain': 'paymentrequest', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'PaymentRequestDetail': {
        '_domain':          'paymentrequest',
        'p_createdate':     'CreateDate',
        'p_updatedate':     'UpdateDate',
        'p_query_join':     load_sql('erp_join_PaymentRequestDetail.sql'),
        'p_tablename_join': 'PaymentRequestHeader',
    },

    # ── product ───────────────────────────────────────────────────────
    'tmPCO':               {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmPMO':               {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmPRO':               {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~240K rows
    'tmPRO_ExtraDetail':   {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmPRO_LOS': {
        '_domain':       'product',
        '_source_table': 'tmPRO',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': load_sql('erp_where_tmPRO_LOS.sql'), # total ~240K rows
    },
    'tmProductCategory':    {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmProductSubCategory': {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmPUP':               {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmPUP_LOS': {
        '_domain':       'product',
        '_source_table': 'tmPUP',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': load_sql('erp_where_tmPUP_LOS.sql'),
    },
    'tmUNT':               {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'ttProMapDCB':         {'_domain': 'product', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},

    # ── project ───────────────────────────────────────────────────────
    'tmBRN': {'_domain': 'project', 'p_createdate': '', 'p_updatedate': ''},
    'tmBRN_LOS': {
        '_domain':       'project',
        '_source_table': 'tmBRN',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': load_sql('erp_where_tmCOM_LOS.sql'),  # same fcCOMID filter
    },
    'tmCOM':     {'_domain': 'project', 'p_createdate': '', 'p_updatedate': ''},
    'tmCOM_LOS': {
        '_domain':       'project',
        '_source_table': 'tmCOM',
        'p_createdate':  '',
        'p_updatedate':  '',
        'p_where_extra': load_sql('erp_where_tmCOM_LOS.sql'),
    },
    'tmCOM_Type': {'_domain': 'project', 'p_createdate': '', 'p_updatedate': ''},

    # ── receipt ───────────────────────────────────────────────────────
    'ttINH': {'_domain': 'receipt', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~11.8M rows
    'ttINH_LOS': {
        '_domain':       'receipt',
        '_source_table': 'ttINH',
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
        'p_where_extra': load_sql('erp_where_fcDCBID_LOS.sql'),
    },
    'ttIND': {
        '_domain':          'receipt',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttIND.sql'),
        'p_tablename_join': 'ttINH', # total 37M rows
    },
    'ttIND_LOS': {
        '_domain':          'receipt',
        'p_createdate':     '',
        'p_updatedate':     '',
        'p_query_join':     load_sql('erp_join_ttIND_LOS.sql'),
        'p_tablename_join': 'ttINH',
    },

    # ── report ────────────────────────────────────────────────────────
    'ReportSignatureManagementDetail': {'_domain': 'report', 'p_createdate': '',           'p_updatedate': ''},
    'ReportMenu':                      {'_domain': 'report', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'ReportMenuDetail':                {'_domain': 'report', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'ReportMenuSub':                   {'_domain': 'report', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'ReportSignature':                 {'_domain': 'report', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'ReportSignatureManagementHeader': {'_domain': 'report', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},

    # ── role ──────────────────────────────────────────────────────────
    'tmAUT':              {'_domain': 'role', 'p_createdate': '',       'p_updatedate': ''}, # total ~2M rows
    'tmUSG':              {'_domain': 'role', 'p_createdate': 'ftCreateDate',       'p_updatedate': 'ftUpdateDate'},
    'tmUSG_LOS':          {'_domain': 'role', 'p_createdate': 'ftCreateDate',       'p_updatedate': 'ftUpdateDate'},
    'tmUSR':              {'_domain': 'role', 'p_createdate': 'ftCreateDate',       'p_updatedate': 'ftUpdateDate'},
    'tmUSR_LOS':          {'_domain': 'role', 'p_createdate': 'ftCreateDate',       'p_updatedate': 'ftUpdateDate'},
    'ttLOG_Menu_Access':  {'_domain': 'role', 'p_createdate': 'ftWHEN', 'p_updatedate': 'ftWHEN'}, # total ~3.8M rows

    # ── transferslip ──────────────────────────────────────────────────
    'TransferSlip':        {'_domain': 'transferslip', 'p_createdate': 'CreateDate',   'p_updatedate': 'UpdateDate'}, # total ~280K rows
    'ttODH_Transfer_Slip': {'_domain': 'transferslip', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},# total ~600K rows

    # ── unit ──────────────────────────────────────────────────────────
    'tmRoomH': {'_domain': 'unit', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'}, # total ~750K rows
    'tmRoomH_mobileuser': {
        '_domain':       'unit',
        '_source_table': 'tmRoomH',
        '_columns':      ['fcID', 'fcOwnerID', 'fcCurrentOwnerID', 'fcOwnerID2', 'fcOwnerID3',
                          'ftCreateDate', 'ftUpdateDate'],
        'p_createdate':  'ftCreateDate',
        'p_updatedate':  'ftUpdateDate',
    },

    # ── standardform ─────────────────────────────────────────────────
    'FormInvoiceResult': {'_domain': 'standardform', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'}, # total ~400K rows
    'FormReceiptResult': {'_domain': 'standardform', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'}, # total ~300K rows

    # ══════════════════════════════════════════════════════════════════
    # GCS_APPLICATION = livingmart  (source DB = ERP)
    # flow: ingest_gcp_mssql_erp_livingmart
    # ══════════════════════════════════════════════════════════════════

    # ── livingmart ────────────────────────────────────────────────────
    'tmCOR_livingmart': { # total ~1M rows
        '_domain':       'livingmart',
        '_source_table': 'tmCOR',
        'p_createdate':  '',
        'p_updatedate':  '',
        'p_where_extra': "fcisecom = 'Y' AND fcissupp = 'Y'",
    },
    'tmPRO_livingmart': { # total ~236K rows
        '_domain':          'livingmart',
        'p_createdate':     'ftCreateDate',
        'p_updatedate':     'ftUpdateDate',
        'p_query_join':     load_sql('erp_join_tmPRO_livingmart.sql'),
        'p_tablename_join': 'tmCOR',
    },
    'tmEMP_livingmart': {
        '_domain':          'livingmart',
        'p_createdate':     'ftCreateDate',
        'p_updatedate':     'ftUpdateDate',
        'p_query_join':     load_sql('erp_join_tmEMP_livingmart.sql'),
        'p_tablename_join': 'tmEMP',
    },

    # ══════════════════════════════════════════════════════════════════
    # GCS_APPLICATION = smartaudit
    # ══════════════════════════════════════════════════════════════════

    # ── smartaudit ────────────────────────────────────────────────────
    'tmSmartAuditLocation':    {'_domain': 'smartaudit', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmSmartAuditLocationNFC': {'_domain': 'smartaudit', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmSmartAuditNFC':         {'_domain': 'smartaudit', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},  # no UpdateDate col
    'tmSmartAuditTask':        {'_domain': 'smartaudit', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},  # no UpdateDate col
    'ttSmartAuditEvent':       {'_domain': 'smartaudit', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},  # no UpdateDate col
    'ttSmartAuditEventImage':  {'_domain': 'smartaudit', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},  # no UpdateDate col

    # ══════════════════════════════════════════════════════════════════
    # GCS_APPLICATION = stockkeycard
    # ══════════════════════════════════════════════════════════════════

    # ── stockkeycard ──────────────────────────────────────────────────
    'StockKeycard':        {'_domain': 'stockkeycard', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},
    'StockKeycardHistory': {'_domain': 'stockkeycard', 'p_createdate': 'CreateDate', 'p_updatedate': 'UpdateDate'},

    # ══════════════════════════════════════════════════════════════════
    # GCS_APPLICATION = timeattendance
    # ══════════════════════════════════════════════════════════════════

    # ── timeattendance ────────────────────────────────────────────────
    'tmTimeAtPosition': {'_domain': 'timeattendance', 'p_createdate': '',              'p_updatedate': ''},
    'tmTimeAttEmp':     {'_domain': 'timeattendance', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmTimeAtten':      {'_domain': 'timeattendance', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftCreateDate'},  # no UpdateDate col

    # ══════════════════════════════════════════════════════════════════
    # GCS_APPLICATION = visitor
    # ══════════════════════════════════════════════════════════════════

    # ── visitor ───────────────────────────────────────────────────────
    'tmLbReceptionSum':    {'_domain': 'visitor', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmOnBoardingVisitor': {'_domain': 'visitor', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmVehicleType':       {'_domain': 'visitor', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'tmVisitorType':       {'_domain': 'visitor', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
    'ttVisitor':           {'_domain': 'visitor', 'p_createdate': 'ftCreateDate', 'p_updatedate': 'ftUpdateDate'},
}