"""
TABLE_REGISTRY for ECOM (Livingmart) — MSSQL source.

Registry key  : table name (case-sensitive, ตาม MSSQL object name)
Registry value: extract() kwargs + optional private keys:
  _domain       : domain / flow slice this table belongs to
                  → livingmart

Domain → GCS_APPLICATION mapping (single source of truth)
──────────────────────────────────────────────────────────
  livingmart → livingmart
"""

from __future__ import annotations

TABLE_REGISTRY: dict[str, dict] = {

    # ══════════════════════════════════════════════════════════════════
    # DOMAIN: livingmart  (GCS_APPLICATION = livingmart)
    # ══════════════════════════════════════════════════════════════════

    # ── Tables with CreateDate / UpdateDate columns ───────────────────
    'tmActionSetting':      {'_domain': 'livingmart', 'p_createdate': 'CreateDate',    'p_updatedate': 'UpdateDate'},

    # ── Tables with fnCreateDate / fnUpdateDate columns ───────────────
    'tmAttribute':          {'_domain': 'livingmart', 'p_createdate': 'fnCreateDate',  'p_updatedate': 'fnUpdateDate'},
    'tmRunPrefix':          {'_domain': 'livingmart', 'p_createdate': 'fnUpdateDate',  'p_updatedate': 'fnUpdateDate'},
    'ttOrderProductDetail': {'_domain': 'livingmart', 'p_createdate': 'fnCreateDate',  'p_updatedate': 'fnUpdateDate'},
    'ttProductAttribute':   {'_domain': 'livingmart', 'p_createdate': 'fnCreateDate',  'p_updatedate': 'fnUpdateDate'},
    'ttShoppingCart':       {'_domain': 'livingmart', 'p_createdate': 'fnCreateDate',  'p_updatedate': 'fnUpdateDate'},

    # ── Tables with fcCreate_date / fcUpdate_date columns ─────────────
    'ttOrderProduct':       {'_domain': 'livingmart', 'p_createdate': 'fcCreate_date', 'p_updatedate': 'fcUpdate_date'},

    # ── Tables with fnCreate_date / fnUpdate_date columns ─────────────
    'ttOrderShop':           {'_domain': 'livingmart', 'p_createdate': 'fnCreate_date', 'p_updatedate': 'fnUpdate_date'},
    'ttPaymentOrderProduct': {'_domain': 'livingmart', 'p_createdate': 'fnCreate_date', 'p_updatedate': 'fnUpdate_date'},
    'ttProductCategory':     {'_domain': 'livingmart', 'p_createdate': 'fnCreate_date', 'p_updatedate': 'fnUpdate_date'},
    'ttProductDetail':       {'_domain': 'livingmart', 'p_createdate': 'fnCreate_date', 'p_updatedate': 'fnUpdate_date'},
    'ttProductDiscount':     {'_domain': 'livingmart', 'p_createdate': 'fnCreate_date', 'p_updatedate': 'fnUpdate_date'},
    'ttShippingProvider':    {'_domain': 'livingmart', 'p_createdate': 'fnCreate_date', 'p_updatedate': 'fnUpdate_date'},
    'ttStoreProduct':        {'_domain': 'livingmart', 'p_createdate': 'fnCreate_date', 'p_updatedate': 'fnUpdate_date'},

    # ── Simple tables (no date columns → synthetic timestamps) ────────
    'trCORPostCode':        {'_domain': 'livingmart', 'p_createdate': '',              'p_updatedate': ''},
    'trPRODiscount':        {'_domain': 'livingmart', 'p_createdate': '',              'p_updatedate': ''},
}